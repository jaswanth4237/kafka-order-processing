# Event-Driven Order Processing System

A robust, resilient, and scalable order processing backend system demonstrating an **Event-Driven Architecture (EDA)** built with **Node.js**, **Apache Kafka**, and **MySQL**.

## 🏗️ Architectural Overview

The backend is decomposed into three decoupled microservices communicating asynchronously via Kafka events. 

### Microservices
1. **Order Service** (`port 8080`)
    - Exposes a REST API (`POST /api/orders`, `GET /api/orders/:orderId`) to accept and track orders.
    - Uses the **Transactional Outbox Pattern** to ensure consistency. Orders are saved to MySQL alongside an outbox event in a single transaction. A background poller publishes these to the `OrderCreated` Kafka topic.
2. **Inventory Service** (Consumer)
    - Consumes the `OrderCreated` topic.
    - Deducts ordered items' stock from the inventory database.
    - Updates order status in the `order-service` database directly (or via event) to `PROCESSING` or `FAILED`.
    - Publishes **`InventoryUpdated`** (on success) or **`OrderFailed`** (on insufficient stock) events.
3. **Notification Service** (Consumer)
    - Subscribes to `OrderCreated`, `InventoryUpdated`, and `OrderFailed` topics to log simulating notifications to the user for every stage of the order lifecycle.

### Core EDA Patterns Implemented
- **Transactional Outbox Pattern**: Guarantees no events are lost or published without state being committed to the database.
- **Idempotent Consumers**: Prevents duplicate executions (e.g., deducting stock multiple times) using a `processed_events` checkpoint table, matching against specific `eventId`s.
- **Dead-Letter Queues (DLQ)**: Invalid or unprocessable messages after max retries are published to `InventoryDLQ` and `NotificationDLQ` topics.
- **Exponential Backoff**: When errors occur (e.g., database connection timeout inside a consumer), the system automatically retries with an exponentially increasing delay before pushing to the DLQ.

---

## 🛠️ Prerequisites

- **Docker** and **Docker Compose** installed (e.g., Docker Desktop). Wait for the docker daemon to run.
- Optional: Postman or `curl` to send HTTP requests.
- Optional: Node.js (v18+) if you wish to run unit tests manually on your local host outside the container.

---

## 🚀 Setup & Running instructions

1. **Clone the repository** and navigate to its root folder.
2. **Setup Environment**:  
   If you wish to change defaults, copy `.env.example` to `.env`. Otherwise, Docker will automatically pick up `.env.example` or defaults mapped in `docker-compose.yml`.
3. **Start the System**:
   ```bash
   docker-compose up -d --build
   ```
4. **Health Checking**:
   The containers run with strict `depends_on` wait conditions. `order-service`, `inventory-service`, and `notification-service` will actively hold and wait until the **Kafka Brokers** and **MySQL databases** specify they are healthy. 
   - Wait 10-20 seconds to see all services running: `docker-compose ps`

*(Data Seeding: The system automatically provisions schemas and seeds 5 starting products via `sql/init.sql` upon first boot).*

---

## 🧪 Testing the API & Flow

Once the services are up, use your terminal to issue `curl` requests.

### Scenario 1: Successful Order Processing
**1. Place an order for an in-stock item (PROD-001 has 10 units):**
```bash
curl -X POST http://localhost:8080/api/orders \
-H "Content-Type: application/json" \
-d '{
  "user_id": "user-123",
  "items": [
    {"sku": "PROD-001", "quantity": 1}
  ]
}'
```
*Expected Response:*
`{"order_id":"<uuid>","status":"PENDING"}`

**2. Check the Order Status**  
Wait 2-3 seconds for Event Outbox Polling + Inventory Consumption to happen, then check the status:
```bash
curl http://localhost:8080/api/orders/<insert-order_id-here>
```
*Expected Response:*
`{"order_id":"<uuid>","status":"PROCESSING","items":[...], ...}`

**3. Check Notification Logs**
```bash
docker-compose logs notification-service
```
*You will see the simulated Notifications sent for the `OrderCreated` and `InventoryUpdated` events.*


### Scenario 2: Failed Order Processing (Insufficient Stock)
**1. Attempt to buy more than what's available:**
```bash
curl -X POST http://localhost:8080/api/orders \
-H "Content-Type: application/json" \
-d '{
  "user_id": "user-123",
  "items": [
    {"sku": "PROD-001", "quantity": 9999}
  ]
}'
```

**2. Check the status:**
```bash
curl http://localhost:8080/api/orders/<insert-order_id-here>
```
*Expected Response:*
`{"order_id":"<uuid>","status":"FAILED","items":[...], ...}` *(Inventory service failed to deduct stock and marked the order as FAILED).*


---

## 💻 Executing Automated Tests

All services include comprehensive Jest Testing suites for their core internal validations (Order validation logic, Inventory constraints logic, and Integration Flow definitions).

### Run inside containers
*(Warning: these test suites must have `supertest` and `jest` available in the image build, or you can run locally).*
```bash
docker-compose exec order-service npm test
docker-compose exec inventory-service npm test
docker-compose exec notification-service npm test
```

### Run locally (Node.js Required)
Navigate to the root and run the global wrapper or individual folders:
```bash
# Order Service Tests
cd order-service && npm install && npm test

# Inventory Service Tests
cd ../inventory-service && npm install && npm test

# Notification Service Tests
cd ../notification-service && npm install && npm test

# Integration tests
cd .. && npm install && npm test
```

---

## 🗄️ Database Design

The MySQL server manages three separated database schemas (logical segregation):
- `orderdb`: `orders` table, `outbox_events` table.
- `inventorydb`: `inventory` table, `processed_events` table (idempotency safety).
- `notificationdb`: `processed_events` table (idempotency safety).

---

## 📚 API Reference
A full detailed HTTP API spec can be found in [API_DOCS.md](./API_DOCS.md).
