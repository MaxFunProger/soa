# ER-диаграмма (3NF)

Схемы разделены по сервисам

## Flight Service (PostgreSQL)

```mermaid
erDiagram
    airport ||--o{ flight : "origin_iata"
    airport ||--o{ flight : "destination_iata"
    airline ||--o{ flight : "operates"
    flight ||--o{ seat_reservation : "has"

    airport {
        char_3 iata PK "IATA код"
        varchar name
    }
    airline {
        uuid id PK
        varchar code UK "IATA airline"
        varchar name
    }
    flight {
        uuid id PK
        varchar flight_number
        date departure_date
        uuid airline_id FK
        char_3 origin_iata FK
        char_3 destination_iata FK
        timestamptz scheduled_departure
        timestamptz scheduled_arrival
        int total_seats "CHECK > 0"
        int available_seats "CHECK >= 0 AND <= total_seats"
        numeric price "CHECK > 0"
        varchar status "SCHEDULED, DEPARTED, CANCELLED, COMPLETED"
        text unique_flight UK "flight_number + departure_date"
    }
    seat_reservation {
        uuid id PK
        uuid flight_id FK
        uuid booking_id UK "внешний ID брони"
        int seat_count "CHECK > 0"
        varchar status "ACTIVE|RELEASED|EXPIRED"
    }
```

Ограничения целостности: `available_seats` неотрицательно; `total_seats`, `seat_count`, `price` > 0; уникальность пары `(flight_number, departure_date)`; одна активная резервация на `booking_id`

## Booking Service (PostgreSQL)

```mermaid
erDiagram
    booking {
        uuid id PK
        varchar user_id
        uuid flight_id "ссылка на Flight Service"
        varchar passenger_name
        varchar passenger_email
        int seat_count "CHECK > 0"
        numeric total_price "CHECK > 0 snapshot"
        varchar status "CONFIRMED|CANCELLED"
        timestamptz created_at
    }
```
