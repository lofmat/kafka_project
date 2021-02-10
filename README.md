# kafka_project

HOWTO:
1. Start Aiven Kafka ver. 2.7 and Aiven PostgreSQL ver. 12 
2. Add topic with default configuration and pass it to config/config.yaml
3. Download ca.pem, service.cert, service.key and copy it to kafka_project/certs directory
4. Fill the connection details to kafka_project/config/config.yaml
5. Install python 3.9 and pip
6. Install all required modules with 
    ```python
    pip install -r requirements.txt
    ```

7. Сreate a table where statistics will be saved

    ```python
    cd kafka_project
    python3.9 src/db_writer.py
    ```

8. Start Kafka Producer:

    ```python
    cd kafka_project
    python3.9 src/kafka_producer.py
    ```

9. Start Kafka Consumer:

    ```python
    cd kafka_project
    python3.9 src/kafka_consumer.py
    ```
