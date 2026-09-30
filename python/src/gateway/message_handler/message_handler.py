from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
        self.sent_messages = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.sent_messages += 1
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message=None):
        return message_protocol.internal.serialize([self.client_id, self.sent_messages])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        ms_client_id = fields[0]
        top_results = fields[1:]
        if self.client_id == ms_client_id:
            return top_results
        return None
