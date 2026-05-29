# Схема событий

`movie_event.avsc` - Avro-схема `movie-events` (namespace `cinema.analytics`,
запись `MovieEvent`). Без изменений переиспользована из ДЗ-5. Регистрируется
сервисом `producer` при старте.

- Subject: `movie-events-value`
- Compatibility: BACKWARD (default)
- Партиций: 3, `replication.factor=2`, `min.insync.replicas=1`
- Ключ партиционирования: `user_id` - сохраняет порядок событий внутри сессии.
