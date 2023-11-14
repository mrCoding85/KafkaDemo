import json

from kafka import KafkaProducer


def main():
    # Khởi tạo client Kafka
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
    )

    # Tạo dữ liệu JSON
    data = {
        "name": "John Doe",
        "age": 30,
        "address": "123 Main Street, Anytown, CA 12345"
    }

    # Gửi dữ liệu đến topic Kafka
    producer.send(
        "user-activity",
        json.dumps(data).encode("utf-8"),
    )

    print("send")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
