# Event-Driven Order Processing System

A resilient and scalable order processing system built using **Node.js**, **Apache Kafka**, and **MySQL**.

## Architecture

The system consists of three main microservices:
1.  **Order Service**: Accepting orders, saving them, and publishing `OrderCreated` events via the Transactional Outbox pattern.
2.  **Inventory Service**: Consumes `OrderCreated`, checks stock, deducts inventory, and updates order status. Publishes `InventoryUpdated` or `OrderFailed`.
3.  **Notification Service**: Consumes all events and logs notifications.

### Key Features
-   **Transactional Outbox**: Ensures order creation and event publishing are atomic.
-   **Event Idempotency**: Consumers check for previously processed event IDs to avoid duplicate actions.
-   **Dead-Letter Queues (DLQ)**: Failed messages are moved to DLQ topics (`InventoryDLQ`, `NotificationDLQ`).
-   **Docker Orchestration**: Full stack managed by Docker Compose.

## How to Run

1.  **Prerequisites**: Docker and Docker Compose installed.
2.  **Start Services**:
    ```bash
    docker-compose up --build
    ```
3.  **Access**:
    -   Order Service: `http://localhost:8080/api/orders`
    -   Inventory Service: `http://localhost:8081` (Health check)
    -   Notification Service: `http://localhost:8082` (Health check)

## Testing the System

### 1. Create a successful order
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
Response: `{"order_id": "...", "status": "PENDING"}`

### 2. Check order status
Wait a few seconds for the consumers to process, then:
```bash
curl http://localhost:8080/api/orders/<order_id>
```
Response should show status `PROCESSING`.

### 3. Create a failing order (insufficient stock)
```bash
curl -X POST http://localhost:8080/api/orders \
-H "Content-Type: application/json" \
-d '{
  "user_id": "user-123",
  "items": [
    {"sku": "PROD-001", "quantity": 99999}
  ]
}'
```
Check status: should be `FAILED`.

## Running Tests
Unit tests can be run inside containers:
```bash
docker-compose exec order-service npm test
```
(Note: tests must be implemented in the `tests/` directory).
