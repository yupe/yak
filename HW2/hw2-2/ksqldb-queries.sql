-- 1. Создание потока сообщений

CREATE STREAM messages_stream (
	user_id BIGINT,
    recipient_id BIGINT,
    message STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='messages',   -- Имя топика
    VALUE_FORMAT='JSON',      
    PARTITIONS=1             
); 


-- 2. Подсчет количества уникальных получателей
SELECT  COUNT_DISTINCT(recipient_id) as count
FROM messages_stream
EMIT CHANGES; 

-- 3. Подсчет количества сообщений
SELECT  COUNT(*) as count
FROM messages_stream
EMIT CHANGES; 

-- Таблица user_statistics для агрегирования данных по каждому пользователю:
-- сообщения, отправленные каждым пользователем;
-- количество уникальных получателей для каждого пользователя;
CREATE TABLE user_statistics AS
SELECT  user_id, COUNT_DISTINCT(*) as message_count, COUNT_DISTINCT(recipient_id) as recipient_count
FROM messages_stream
GROUP BY user_id
EMIT CHANGES; 
