# tarantool-kv-grpc

gRPC-сервис на Java для работы с key-value хранилищем на базе Tarantool 3.2.

## API

| Метод | Описание |
|-------|----------|
| `Put(key, value)` | Сохраняет значение. Перезаписывает если ключ существует |
| `Get(key)` | Возвращает значение по ключу |
| `Delete(key)` | Удаляет значение по ключу |
| `Range(key_since, key_to)` | Возвращает gRPC stream пар ключ-значение из диапазона |
| `Count()` | Возвращает количество записей |

- Поле `value` имеет тип `bytes` и поддерживает `null`
- Данные хранятся в спейсе `KV` с TREE-индексом для поддержки range-запросов

## Технологии

- Java 17
- gRPC + Protocol Buffers
- Tarantool 3.2 (in-memory database)
- tarantool-java-sdk 1.5.0
- Docker
- Gradle

## Требования

- JDK 17+
- Docker и Docker Compose
- Gradle (или используйте `./gradlew`)

## Запуск

### 1. Запустить Tarantool
```bash
docker-compose up -d
```

### 2. Собрать проект
```bash
./gradlew build
```

### 3. Запустить сервис
```bash
./gradlew run
```

gRPC-сервер запустится на порту `8080`.

## Порты

| Сервис | Порт |
|--------|------|
| gRPC-сервер | 8080 |
| Tarantool | 3301 |

## Тестирование

Для тестирования можно использовать Postman (gRPC) или grpcurl.
Сервер поддерживает gRPC Server Reflection.

### Пример: Put
```json
{
    "key": "mykey",
    "value": "aGVsbG8="
}
```

### Пример: Get
```json
{
    "key": "mykey"
}
```

### Пример: Range
```json
{
    "key_since": "a",
    "key_to": "z"
}
```

## Структура проекта
```
├── docker-compose.yml          — запуск Tarantool
├── tarantool/init.lua          — инициализация спейса KV
├── src/main/proto/kv.proto     — описание gRPC API
└── src/main/java/
    ├── KvServer.java           — точка входа, запуск gRPC-сервера
    ├── KvServiceImpl.java      — реализация gRPC-методов
    └── TarantoolClientWrapper.java — взаимодействие с Tarantool
```