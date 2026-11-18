# Go + Kafka Example

A small Go service that exposes a products API over HTTP and also creates products from Kafka messages.

## What's inside

- **HTTP API** with chi: `GET /products` and `POST /products` on port 8080
- **Kafka consumer** on the `products` topic that runs the same use case as the API
- **Clean architecture:** entity, use cases and infra (web, repository, Kafka)
- **MySQL** repository
- **Docker Compose** with MySQL, Zookeeper and Kafka

## Stack

Go · Kafka (confluent-kafka-go) · chi · MySQL · Docker

## Getting started

```bash
docker compose up -d
go run ./cmd/app
```
