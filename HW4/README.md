### Состав проекта

- Кластер kafka из 3-х узлов
- PostgreSQL сервер
- Kafka Connect
- Debezium
- Prometheus, Grafana

### Запуск

docker-compose up -d

Вывод 

```[+] up 11/11
 ✔ Network custom_network                     Created                                          0.0s
 ✔ Container infra_template-kafka-2-1         Created                                          0.2s
 ✔ Container infra_template-x-kafka-common-1  Created                                          0.1s
 ✔ Container infra_template-kafka-1-1         Created                                          0.2s
 ✔ Container infra_template-grafana-1         Created                                          0.2s
 ✔ Container infra_template-kafka-0-1         Created                                          0.1s
 ✔ Container infra_template-ui-1              Created                                          0.2s
 ✔ Container postgres                         Created                                          0.2s
 ✔ Container infra_template-schema-registry-1 Created                                          0.2s
 ✔ Container infra_template-kafka-connect-1   Created                                          0.6s
 ✔ Container infra_template-prometheus-1      Created
```

### Порядок выполнения

- Подключаемся к PostgreSQL и создаем таблицы

 1 `docker exec -it postgres psql -h 127.0.0.1 -U postgres-user -d customers`
 2 CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
3 CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
Таблицы должны успешно создаться

### Настройки Kafka connector

  1 Проверим, что плагин установлен
  `curl localhost:8083/connector-plugins | jq | grep postgre`
  Вывод:
  "class": "io.debezium.connector.postgresql.PostgresConnector",

  2 Создаем коннектор
  Добавляем коннектор 
  `curl -X PUT -H 'Content-Type: application/json' --data @connector-config.json http://localhost:8083/connectors/pg-connector/config`
  Ответ:
  {
  "name": "pg-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres-user",
    "database.password": "postgres-pw",
    "database.dbname": "customers",
    "database.server.name": "customers",
    "table.whitelist": "public.customers",
    "table.include.list": "public.users,public.orders",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "topic.prefix": "customers",
    "topic.creation.enable": "true",
    "topic.creation.default.replication.factor": "2",
    "topic.creation.default.partitions": "9",
    "skipped.operations": "none",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "name": "pg-connector"
  },
  "tasks": [
    {
      "connector": "pg-connector",
      "task": 0
    }
  ],
  "type": "source"
} 
Проверим статус коннектора:
`curl -s -X GET "http://localhost:8083/connectors/pg-connector/status"`
Вывод:
{
  "name": "pg-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "localhost:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "localhost:8083"
    }
  ],
  "type": "source"
}

### Добавим данные в базу данных

-- Добавление пользователей
INSERT INTO users (name, email) VALUES ('John Doe', '[john@example.com](mailto:john@example.com)');
INSERT INTO users (name, email) VALUES ('Jane Smith', '[jane@example.com](mailto:jane@example.com)');
INSERT INTO users (name, email) VALUES ('Alice Johnson', '[alice@example.com](mailto:alice@example.com)');
INSERT INTO users (name, email) VALUES ('Bob Brown', '[bob@example.com](mailto:bob@example.com)');

-- Добавление заказов
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product A', 2);
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product B', 1);
INSERT INTO orders (user_id, product_name, quantity) VALUES (2, 'Product C', 5);
INSERT INTO orders (user_id, product_name, quantity) VALUES (3, 'Product D', 3);
INSERT INTO orders (user_id, product_name, quantity) VALUES (4, 'Product E', 4); 
customers=# 

### Просмотр данных в Grafana

[http://localhost:3000/d/kafka-connect-overview-0/kafka-connect-overview-0?orgId=1&from=now-30m&to=now](http://localhost:3000/d/kafka-connect-overview-0/kafka-connect-overview-0?orgId=1&from=now-30m&to=now)

### Запуск consumer

cd consumer 
go run main.go
После этого необходимо сделать вставку в PostgreSQL и можно увидеть сообщения в консоли

### Таблица результатов эксперимента из урока


| Эксперимент | batch.size | [linger.ms](http://linger.ms) | compression.type | buffer.memory | Source Record Write Rate (кops/sec) |
| ----------- | ---------- | ----------------------------- | ---------------- | ------------- | ----------------------------------- |
| 1           | 100        | 0                             | none             | 33554432      | 61.6                                |
| 2           | 12600      | 1000                          | none             | 33554432      | ~160                                |
| 3           | 12600      | 2000                          | none             | 33554432      | ~160                                |
| 4           | 12600      | 2000                          | lz4              | 134217728     | ~160                                |
| 5           | 12600      | 0                             | lz4              | 33554432      | ~170                                |
| 5           | 100        | 0                             | lz4              | 33554432      | ~170                                |


- в 5 поднял "batch.max.rows": 1000,

curl -X PUT  
-H "Content-Type: application/json"  
--data '{
"connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector",
"tasks.max":"1",
"connection.url":"jdbc:postgresql://postgres:5432/customers?user=postgres-user&password=postgres-pw&useSSL=false",
"connection.attempts":"5",
"connection.backoff.ms":"50000",
"mode":"timestamp",
"timestamp.column.name":"updated_at",
"topic.prefix":"postgresql-jdbc-bulk-",
"table.whitelist": "users",
"poll.interval.ms": "0",
"batch.max.rows": 1000,
"producer.override.linger.ms": 1000,
"producer.override.batch.size": 50000,
"producer.override.compression.type": "lz4",
"transforms":"MaskField",
"transforms.MaskField.type":"org.apache.kafka.connect.transforms.MaskField$Value",
"transforms.MaskField.fields":"private_info",
"transforms.MaskField.replacement":"CENSORED"
}'  
[http://localhost:8083/connectors/postgres-source/config](http://localhost:8083/connectors/postgres-source/config)