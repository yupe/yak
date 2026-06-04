Задача реализована в виде 3-х сервисов: users, messages, words
Каждый сервис это консольная команда с флагами запуска
Список доступных флагов и действий можно увидеть запустив сервис без параметров, например:
go run ./cmd/users/

Порядок запуска и проверки

1 Запустить kafka: 
docker compose up -d

2 Запускаем севрисы в трех разных терминалах:
go run ./cmd/words/ -action consume - сервис запрещенных слов
go run ./cmd/users/ -action consume - сервис блокировки/разблокировки
go run ./cmd/messages/ -action consume - сервис сообщений
Все сервисы пишут вывод  в консоль.

3 Проверяем отправку (4-ый терминал):
go run ./cmd/messages/ -action send -from 22 -to 11 -text "hello user 11" - 
В логах сервиса сообщений должны увидеть:
[DELIVERED] "22" -> "11": hello user 11

4 Блокируем пользователя:
go run ./cmd/users/ -action block -user 11 -blocked_user 22
Должны увидеть вывод в консоли
✓ User "11" block "22"
Пробуем отправить сообщение:
go run ./cmd/messages/ -action send -from 22 -to 11 -text "hello user 11" 
В логе сервиса сообщений должны увидеть:
[BLOCKED] "11" blocked "22" — message not delivered

5 Проверяем работу стоп-слов:
Добавляем слово:
go run ./cmd/words/ -action add -word hello
✓ Word "hello" (add)
Проверяем отправку сообщения
Разблокируем пользователя:
go run ./cmd/users/ -action unblock -user 11 -blocked_user 22
Отправляем сообщение:
go run ./cmd/messages/ -action send -from 22 -to 11 -text "hello user 11"
В логе сервиса сообщений должны увидеть маскирование:
[DELIVERED] "22" -> "11": * user 11