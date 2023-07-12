#!/usr/bin/env python
# coding: utf-8

# 
# 1. Setting up a Kafka Producer:
#    a) Write a Python program to create a Kafka producer.
#    b) Configure the producer to connect to a Kafka cluster.
#    c) Implement logic to send messages to a Kafka topic.
# 
# 2. Setting up a Kafka Consumer:
#    a) Write a Python program to create a Kafka consumer.
#    b) Configure the consumer to connect to a Kafka cluster.
#    c) Implement logic to consume messages from a Kafka topic.
# 
# 3. Creating and Managing Kafka Topics:
#    a) Write a Python program to create a new Kafka topic.
#    b) Implement functionality to list existing topics.
#    c) Develop logic to delete an existing Kafka topic.
# 
# 4. Producing and Consuming Messages:
#    a) Write a Python program to produce messages to a Kafka topic.
#    b) Implement logic to consume messages from the same Kafka topic.
#    c) Test the end-to-end flow of message production and consumption.
# 
# 5. Working with Kafka Consumer Groups:
#    a) Write a Python program to create a Kafka consumer within a consumer group.
#    b) Implement logic to handle messages consumed by different consumers within the same group.
#    c) Observe the behavior of consumer group rebalancing when adding or removing consumers.
# 
# 

# In[5]:


pip install confluent_kafka


# In[6]:


#1Ans
pip install confluent_kafka
from confluent_kafka import Producer

def create_kafka_producer(bootstrap_servers):
    # Create the Kafka producer configuration
    config = {
        'bootstrap.servers': bootstrap_servers
    }

    # Create the Kafka producer instance
    producer = Producer(config)

    return producer

# Set the Kafka cluster bootstrap servers
bootstrap_servers = 'localhost:9092'

# Create the Kafka producer
kafka_producer = create_kafka_producer(bootstrap_servers)
def send_message(producer, topic, message):
    # Produce the message to the Kafka topic
    producer.produce(topic, message.encode('utf-8'))
    
    # Flush the producer to ensure the message is sent
    producer.flush()

# Set the Kafka topic
topic = 'my_topic'

# Set the message to be sent
message = 'Hello, Kafka!'

# Send the message to the Kafka topic
send_message(kafka_producer, topic, message)


# In[ ]:


#2ANS
from confluent_kafka import Consumer

def create_kafka_consumer(bootstrap_servers, group_id):
    # Create the Kafka consumer configuration
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id
    }

    # Create the Kafka consumer instance
    consumer = Consumer(config)

    return consumer

# Set the Kafka cluster bootstrap servers
bootstrap_servers = 'localhost:9092'

# Set the Kafka consumer group ID
group_id = 'my_consumer_group'

# Create the Kafka consumer
kafka_consumer = create_kafka_consumer(bootstrap_servers, group_id)
def consume_messages(consumer, topic):
    # Subscribe to the Kafka topic
    consumer.subscribe([topic])

    # Consume messages from the Kafka topic
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        print(f"Received message: {message.value().decode('utf-8')}")

# Set the Kafka topic to consume from
topic = 'my_topic'

# Consume messages from the Kafka topic
consume_messages(kafka_consumer, topic)


# In[ ]:



pip install confluent-kafka[admin]


# In[ ]:


#3ANS
from confluent_kafka.admin import AdminClient, NewTopic

def create_kafka_topic(bootstrap_servers, topic, partitions=1, replication_factor=1):
    # Create the AdminClient instance
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Create the NewTopic instance
    new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)

    # Create the topic using the AdminClient
    admin_client.create_topics([new_topic])

    # Close the AdminClient
    admin_client.close()

# Set the Kafka cluster bootstrap servers
bootstrap_servers = 'localhost:9092'

# Set the topic details
topic = 'my_topic'
partitions = 3
replication_factor = 2

# Create the Kafka topic
create_kafka_topic(bootstrap_servers, topic, partitions, replication_factor)
def list_kafka_topics(bootstrap_servers):
    # Create the AdminClient instance
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Get the list of topics
    topics_metadata = admin_client.list_topics().topics

    # Print the list of topics
    for topic, metadata in topics_metadata.items():
        print(f"Topic: {topic}, Partitions: {len(metadata.partitions)}, Replication Factor: {metadata.replication_factor}")

    # Close the AdminClient
    admin_client.close()

# List existing Kafka topics
list_kafka_topics(bootstrap_servers)


# In[ ]:


#4ANS
from confluent_kafka import Producer

def produce_messages(bootstrap_servers, topic, messages):
    # Create the Kafka producer configuration
    config = {'bootstrap.servers': bootstrap_servers}

    # Create the Kafka producer instance
    producer = Producer(config)

    # Produce messages to the Kafka topic
    for message in messages:
        producer.produce(topic, message.encode('utf-8'))
        producer.flush()

    # Close the Kafka producer
    producer.close()

# Set the Kafka cluster bootstrap servers
bootstrap_servers = 'localhost:9092'

# Set the Kafka topic to produce messages to
topic = 'my_topic'

# Set the messages to be produced
messages = ['Message 1', 'Message 2', 'Message 3']

# Produce messages to the Kafka topic
produce_messages(bootstrap_servers, topic, messages)
from confluent_kafka import Consumer

def consume_messages(bootstrap_servers, topic, group_id):
    # Create the Kafka consumer configuration
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    # Create the Kafka consumer instance
    consumer = Consumer(config)

    # Subscribe to the Kafka topic
    consumer.subscribe([topic])

    # Consume messages from the Kafka topic
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        print(f"Received message: {message.value().decode('utf-8')}")

    # Close the Kafka consumer
    consumer.close()

# Set the Kafka consumer group ID
group_id = 'my_consumer_group'

# Consume messages from the Kafka topic
consume_messages(bootstrap_servers, topic, group_id)


# In[ ]:


#5ANS
from confluent_kafka import Consumer, KafkaException

def create_kafka_consumer(bootstrap_servers, group_id):
    # Create the Kafka consumer configuration
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    # Create the Kafka consumer instance
    consumer = Consumer(config)

    return consumer

# Set the Kafka cluster bootstrap servers
bootstrap_servers = 'localhost:9092'

# Set the Kafka consumer group ID
group_id = 'my_consumer_group'

# Create the Kafka consumer
kafka_consumer = create_kafka_consumer(bootstrap_servers, group_id)
def consume_messages(consumer, topic):
    # Subscribe to the Kafka topic
    consumer.subscribe([topic])

    # Consume messages from the Kafka topic
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            if message.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {message.error()}")
                break

        print(f"Received message: {message.value().decode('utf-8')}")

        # Commit the offset to mark the message as processed
        consumer.commit(message)

# Set the Kafka topic to consume from
topic = 'my_topic'

# Consume messages from the Kafka topic
consume_messages(kafka_consumer, topic)


# In[ ]:




