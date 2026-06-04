1 Запустить kafka: 
docker compose up -d

2 Создать топики
docker exec -it <kafka-1>  kafka-topics.sh --create --bootstrap-server localhost:9094 --topic messages 