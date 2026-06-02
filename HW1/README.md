1 Запустить кластер
docker-compose up -d

2 Создать топик с 3 партициями и 2 репликами через консоль.
docker exec -it <CONTAINER_ID> kafka-topics.sh --create \
--topic yandex-hw-1 \
--partitions 3 \
--replication-factor 2 \
--bootstrap-server localhost:9094
Created topic my-topic.


2 Запустить консьюмер
cd ./cmd/consumer/batch && go run main.go (Batch)
или
cd ./cmd/consumer/ && go run main.go (Single)

3 Запустить продюсер
cd ./cmd/producer && go run main.go

4 Вывод об обработке сообщений логируется в терминал