from kafka import KafkaConsumer
 
 
def main():
    consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"])
    consumer.subscribe(['test'])
    for message in consumer:
        # This will wait and print messages as they become available
        print(message)
 
 
if __name__ == "__main__":
    main()
