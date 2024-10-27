pip install kafka-python streamlit

import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient

class KafkaProducerClass:
    def __init__(self, kafka_server='localhost:9092'):
        self.kafka_server = kafka_server
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_server)

    def topic_exists(self, topic):
        """Check if a Kafka topic exists."""
        topics = self.admin_client.list_topics()
        return topic in topics

    def send_query(self, query):
        """Send a query to the Kafka retriever."""
        if self.topic_exists('query'):
            self.producer.send('query', value=query)
            self.producer.flush()
            return f"Sent query: {query}"
        else:
            return "Topic 'query' does not exist."

class KafkaConsumerClass:
    def __init__(self, kafka_server='localhost:9092'):
        self.kafka_server = kafka_server
        self.consumer = KafkaConsumer(
            'response',
            bootstrap_servers=self.kafka_server,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='response-group'
        )

    def consume_response(self):
        """Consume messages from the response topic."""
        for message in self.consumer:
            return message.value  # Return the first response received

import streamlit as st

# Streamlit app
def main():
    st.title("Kafka Query Producer")

    kafka_producer = KafkaProducerClass()
    kafka_consumer = KafkaConsumerClass()

    query = st.text_input("Enter your query:")
    
    if st.button("Send Query"):
        response_message = kafka_producer.send_query(query)
        st.write(response_message)

        # Wait for the response from the Kafka retriever
        st.write("Waiting for response...")
        response = kafka_consumer.consume_response()
        st.write("Response received:", response)

if __name__ == "__main__":
    main()
