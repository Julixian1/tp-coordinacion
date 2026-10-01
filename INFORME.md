# Informe de Coordinación, Escalabilidad y Decisiones de Diseño

## 1. Introducción

El presente sistema busca resolver el procesamiento distribuido de un gran flujo de eventos para calcular el **Top K** de frutas acumuladas por cliente, garantizando el desacoplamiento entre la lógica de negocio y la infraestructura de mensajería (MOM).

La arquitectura se diseña como una tubería (*pipeline*) de cuatro etapas desacopladas sobre RabbitMQ:

1. **Gateway:** Punto de contacto TCP externo. Recibe los mensajes, los parametriza con un `client_id` y los inyecta al middleware.
2. **SumFilter (`Sum`):** Primera etapa de reducción. Realiza la acumulación local en memoria por cliente y fruta.
3. **AggregationFilter (`Aggregation`):** Segunda etapa de reducción. Recibe subconjuntos particionados de datos y calcula Tops parciales.
4. **JoinFilter (`Join`):** Etapa de consolidación final. Une los Tops parciales de cada agregador y emite el ranking definitivo.

---

## 2. Coordinación y Flujo de Etapas

### 2.1. Ingesta y acumulación en `Sum`

El `Gateway` deposita los registros en la cola compartida `INPUT_QUEUE`. Las `S` instancias de
`Sum` consumen de ella como **consumidores competidores**, con `prefetch_count=1`.

El `message_handler` del gateway **cuenta los mensajes de datos de cada cliente** y el `EOF`
transporta ese total: `[client_id, N]`.

Cada `Sum` mantiene en memoria, por `client_id`, un diccionario `{fruta: FruitItem}` y un
contador de los mensajes que sumó. La acumulación usa el operador `+` de `FruitItem`, sin
asumir su implementación.

### 2.2. Sincronización del fin de ingesta (`Sum` → `Aggregation`)

Los datos de un cliente se reparten entre las `S` instancias por una cola compartida, pero el
`EOF` lo consume una sola. Esa instancia no puede enviar sus resultados en ese momento: otra
podría tener un dato de ese cliente ya entregado y todavía sin procesar, y un envío anticipado
lo perdería. Un broadcast simple del EOF tampoco alcanza, porque viaja por un canal distinto al
de los datos y no hay orden entre ambos.

**Idea.** Gracias al total `N`, cada instancia puede *comprobar* que ya se procesaron todos los
datos, en lugar de suponerlo. Las instancias se coordinan por `SUM_CONTROL_EXCHANGE` (exchange
`topic`). 
El `Sum` que consume el `EOF` actúa como **coordinador de ese cliente**; el rol rota según quién
lo consuma, por lo que no hay un nodo maestro fijo.

**Protocolo:**

1. **`PREPARE(client_id, N, coordinador)`**: lo publica por broadcast el `Sum` que consumió el `EOF`.
2. Cada `Sum`, al recibir el `PREPARE`, recuerda quién es el coordinador y le envía
   **`COUNT(client_id, c)`**, con `c` la cantidad de mensajes de ese cliente que ya sumó
   (solo si `c > 0`). El coordinador además guarda `N`.
3. Si después le llega un dato de ese cliente a un `Sum` que ya vio el `PREPARE`, lo suma y envía
   `COUNT(client_id, 1)` al coordinador.
4. El coordinador acumula los `COUNT`. Cuando la suma es igual a `N`, publica por broadcast
   **`FLUSH(client_id)`**.
5. Cada `Sum`, al recibir el `FLUSH`, envía cada `FruitItem` al aggregator que le corresponde.

- Cada dato se cuenta exactamente una vez: o queda incluido en el `c` del `PREPARE` o se informa como `+1`. Ambas decisiones se toman bajo el mismo `Lock` que protege el diccionario y los contadores.
- El contador se incrementa *después* de sumar el dato, así que todo mensaje contado ya está en un diccionario.
- Los conteos solo crecen. Si la suma llega a `N`, todos los datos ya fueron sumados y ninguno puede llegar después. Si hay un dato que falta procesar, la suma da menos que `N`; cuando ese `Sum` lo procese enviará su `+1` y el coordinador volverá a evaluar. No hay polling, sleeps ni reintentos.
- El coordinador borra su estado antes de emitir el `FLUSH`, por lo que no puede emitirlo dos veces. Un `COUNT` ajeno que llegue antes que su `PREPARE` se acumula y se revisa al llegar este, de modo que el orden entre ambos es irrelevante.
- Al terminar, ningún `Sum` conserva estado del cliente.

**Concurrencia.** Cada `Sum` tiene dos hilos: el principal (datos) y el de control. El estado por
cliente se protege con un `Lock` global. Como Pika no es thread-safe, los `data_output_exchanges`
los usa únicamente el hilo de control (quien envía a los aggregators), y `control_sender`, que
usan ambos hilos, se utiliza siempre con el lock tomado.

**Costo del protocolo de control.** Por cliente: un `PREPARE` (broadcast, `S` entregas), a lo sumo
`S` `COUNT` dirigidos a un único nodo, más a lo sumo `S-1` `COUNT` por datos que lleguen tras el
`PREPARE` (con `prefetch_count=1`, cada `Sum` tiene a lo sumo un mensaje en proceso), y un
`FLUSH` (broadcast). Es **`O(S)`** y **no depende del volumen de datos**. El precio es una ronda
extra de latencia (`PREPARE → COUNT → FLUSH`). Se descartó un esquema donde cada `Sum` difunde su
`COUNT` a todos los demás por ser `O(S²)` en réplicas.


### 2.3. Barrera de Sincronización en `AggregationFilter`
Cada nodo `Aggregation` mantiene en memoria una lista ordenada por cliente utilizando el módulo `bisect` y un contador de señales de finalización (`eof_count_by_client`).

* **Condición de Sincronización:** Un nodo `Aggregation` concluye la recepción de datos de un cliente cuando cumple la condición:
  $$\text{eof\_count\_by\_client}[\text{client\_id}] == \text{SUM\_AMOUNT}$$
* Al alcanzar la barrera, el agregador extrae el *Top K* de su partición, envía el mensaje estructurado al `Joiner` y libera la memoria del `client_id`.

### 2.4. Consolidación en `JoinFilter`
El `Joiner` espera los resultados de los $M$ agregadores (`AGGREGATION_AMOUNT`). Al recibir las $M$ respuestas asociadas a un `client_id`, fusiona los Tops parciales, reordena los elementos apoyándose en la comparación nativa de `FruitItem`, y envía el payload definitivo al `Gateway` a través de `OUTPUT_QUEUE` para su devolución al cliente TCP.

---

## 3. Particionamiento de Datos por Hash de Fruta

Para evitar el envío redundante por *broadcast* desde `Sum` hacia todos los agregadores, se aplica un ruteo determinístico por clave sobre el canal de salida:

```python
fruit_bytes = final_fruit_item.fruit.encode("utf-8")
aggregator_idx = int(hashlib.md5(fruit_bytes).hexdigest(), 16) % AGGREGATION_AMOUNT
```

### 3.1. Selección del Criterio de Hash para el Particionamiento

Para distribuir el trabajo entre la etapa de acumulación (`Sum`) y la de agregación (`Aggregation`), fue necesario definir la clave para la función de hash. Se analizaron dos alternativas principales:

* **Opción A: Hash por `Client_ID + Fruta` (Seleccionada)**
  * **Ventaja:** Reparte la carga de forma pareja entre todos los agregadores. Evita que un agregador se sature si una fruta es muy vendida.
  * **Desventaja:** Requiere combinar dos campos para armar la clave.

* **Opción B: Hash solo por `Fruta`**
  * **Ventaja:** Agrupa toda la información de cada fruta en un único nodo.
  * **Desventaja:** Riesgo alto de sobrecargar un solo nodo (*data skew*) si una fruta domina el volumen de ventas.

#### Justificación

Se eligió la **Opción A (`Client_ID + Fruta`)** por las siguientes razones:
1. **Evita cuellos de botella:** Previene que una fruta "popular" llene la memoria o sobrecargue la red de un solo agregador.
2. **Uso parejo de recursos:** Se busca que todos los contenedores de agregación procesan una cantidad similar de trabajo.
3. **Sin costo extra:** El nodo `Joiner` igual tenía que esperar la respuesta de los $M$ agregadores por cada cliente, por lo que este reparto no agrega complejidad ni demoras al flujo de datos

## 4. Escalabilidad y Flexibilidad

### 4.1. Escalado por Clientes Concurrentes
Los mensajes de datos y de control transportan el metadato `client_id` (key). Como todos los nodos (`Sum`, `Aggregation`, `Joiner`) estructuran sus diccionarios de estado con este identificador en el nivel superior, el sistema procesa múltiples solicitudes de manera simultánea.

### 4.2. Escalado por Volumen de Datos
El sistema maneja un alto volumen de datos mediante la reducción por etapas:
* **Etapa de Ingesta (`Sum`):** Permite agregar más nodos en paralelo (*escalabilidad horizontal*). Además, cada nodo suma en su memoria local todas las repeticiones de una misma fruta antes de enviarla. De esta forma se envía un único mensaje consolidado por fruta.
* **Etapa de Agregación (`Aggregation`):** La función de hash reparte las frutas entre las $M$ instancias de `Aggregation`, dividiendo el trabajo de ordenamiento y el cálculo del Top.

### 4.3. Parametrización de la Topología
El código no contiene constantes rígidas sobre la cantidad de contenedores. Todas las barreras de sincronización y divisiones modulares dependen directamente de variables de entorno:
* `SUM_AMOUNT`
* `AGGREGATION_AMOUNT`
* `TOP_SIZE`

Al alterar la multiplicidad de nodos en los archivos de `docker-compose`, el pipeline adapta automáticamente sus barreras de control sin requerir modificaciones en el código fuente.

### 4.4. Resiliencia y Apagado Limpio (`SIGTERM`)
Todos los componentes registran manejadores de señales para `SIGTERM` y `SIGINT`. Ante una orden de detención enviada por el entorno, cada servicio ejecuta las siguientes acciones:

1. **Detención del consumo principal:** Se invoca `stop_consuming()` sobre las colas de entrada para evitar aceptar nuevos mensajes del broker.
2.**Cierre seguro multihilo en el Middleware (`stop_consuming_threadsafe`):** Debido a que la librería `pika` no es *thread-safe*, invocar métodos de conexión directamente desde el hilo que captura las señales hacia el hilo secundario (`control_thread`) genera errores de concurrencia. Para solucionar esto, se implementó en el middleware el método `stop_consuming_threadsafe()`, el cual utiliza:
   ```python
   self.connection.add_callback_threadsafe(self.stop_consuming)
   ``` 
Esto permite programar la orden de detención como un callback dentro del propio event loop del hilo secundario, garantizando un apagado limpio y libre de condiciones de carrera.
3. **Liberación de conexiones:** Se realiza el cierre explícito de sockets, colas y exchanges dentro de bloques `finally` e invocaciones a `close()`, garantizando que no queden conexiones TCP o recursos bloqueados en el middleware.

#### Diagnóstico de Fallos en Pruebas: Desconexión del Cliente (`Errno 107`)

Durante la ejecución de las pruebas automáticas, se observó que esporádicamente algunos contenedores de clientes de prueba (`ciaahtbord_X`) fallaban registrando la siguiente traza en los logs:

```text
ERROR:root:The connection with the server was lost
...
File "//main.py", line 35, in disconnect
  self.server_socket.shutdown(socket.SHUT_RDWR)
OSError: [Errno 107] Socket not connected
```
Y al gateway no le llegaba ningún mensaje. Dado que el código del cliente de pruebas no es modificable, la hipótesis de este comportamiento radica en una condición de carrera durante la etapa de arranque (startup). Si un cliente intenta ejecutar la llamada a connect() antes de que el proceso del Gateway haya terminado de inicializarse y abrir su socket, la conexión falla al inicio. Posteriormente, cuando el cliente ejecuta la limpieza en el bloque final llamando a client.disconnect(), se invoca shutdown() sobre un socket que nunca llegó a establecer una conexión TCP. Esto provoca que el sistema operativo lance la excepción OSError: [Errno 107] Socket not connected

Se comprobó que, implementando políticas de reintento de conexión a nivel de red, la condición de carrera desaparece por completo y las pruebas se ejecutan bien.
