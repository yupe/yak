1 Шаг 1. Создайте топик с 3 партициями и 2 репликами через консоль.
docker exec -it <CONTAINER_ID> kafka-topics.sh --create \
--topic yandex-hw-1 \
--partitions 3 \
--replication-factor 2 \
--bootstrap-server localhost:9094
Created topic my-topic.


