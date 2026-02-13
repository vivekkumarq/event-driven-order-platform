# Event-Driven Order Management Platform

A production-style backend platform demonstrating **event-driven microservices architecture** using **Spring Boot, Kafka, Docker, and H2/PostgreSQL-ready persistence**.

This project simulates how real-world order processing systems work using **asynchronous messaging** and **service decoupling**.

---

## 🧩 Architecture Overview

Order Service ---> Kafka Topic (order-created-topic) ---> Inventory Service
| |
REST API Kafka Consumer
| |
H2 DB H2 DB


- **Order Service** publishes `OrderCreated` events to Kafka  
- **Inventory Service** consumes events and processes stock reservation (currently logs the event)  
- Kafka runs via Docker for local development  
- Services are fully decoupled and communicate asynchronously  

---

## 🛠 Tech Stack

- Java 17  
- Spring Boot 3.x  
- Spring Data JPA (H2 for local dev, PostgreSQL-ready)  
- Apache Kafka  
- Docker & Docker Compose  
- REST APIs  
- Maven  

---

## 🚀 How to Run Locally

### 1️⃣ Start Kafka (Docker)
```bash
docker compose up -d
docker ps
2️⃣ Run Order Service
cd order-service
mvn spring-boot:run
Service runs on:

http://localhost:8080
3️⃣ Run Inventory Service
cd inventory-service
mvn spring-boot:run
Service runs on:

http://localhost:8082
📌 API Documentation
➤ Create Order
POST /orders?amount=1500
Sample Response

{
  "id": "UUID",
  "amount": 1500.0,
  "status": "CREATED",
  "createdAt": "2026-02-14T00:00:00Z"
}
➤ Get Order By ID
GET /orders/{id}
Sample Response

{
  "id": "UUID",
  "amount": 1500.0,
  "status": "CREATED",
  "createdAt": "2026-02-14T00:00:00Z"
}
📬 Kafka Topics
Topic Name	Producer	Consumer	Purpose
order-created-topic	Order Service	Inventory Service	Trigger inventory update
🧪 Verify Kafka Events
Open Kafka consumer:

docker exec -it order-service-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-created-topic \
  --from-beginning
Then create an order:

POST http://localhost:8080/orders?amount=1500





🧠 Key Concepts Demonstrated

Event-driven microservices architecture
Kafka producers and consumers
Asynchronous communication between services
Loose coupling and service decoupling
Clean layered architecture (Controller → Service → Repository)
Docker-based local infrastructure
Test isolation from infrastructure dependencies

🔮 Future Enhancements

Implement stock reservation logic in Inventory Service
Publish StockReserved / StockRejected events
Implement Saga pattern for distributed transactions
Add OAuth2 / Keycloak-based authentication
Replace H2 with PostgreSQL + Flyway migrations
Add GraphQL BFF for API aggregation
Add Kubernetes deployment manifests
