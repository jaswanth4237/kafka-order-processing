# Order Service API Documentation

## Endpoints

### 1. Create Order
- **URL**: `/api/orders`
- **Method**: `POST`
- **Request Body**:
  ```json
  {
    "user_id": "uuid",
    "items": [
      { "sku": "PROD-001", "quantity": 2 },
      { "sku": "PROD-002", "quantity": 1 }
    ]
  }
  ```
- **Response**: `202 Accepted`
  ```json
  {
    "order_id": "uuid",
    "status": "PENDING"
  }
  ```
- **Errors**:
  - `400 Bad Request`: Validation failure.
  - `500 Internal Server Error`: Server failure.

### 2. Get Order Status
- **URL**: `/api/orders/:orderId`
- **Method**: `GET`
- **Response**: `200 OK`
  ```json
  {
    "order_id": "uuid",
    "status": "PROCESSING",
    "items": [...],
    "created_at": "datetime",
    "updated_at": "datetime"
  }
  ```
- **Errors**:
  - `404 Not Found`: Order does not exist.
  - `500 Internal Server Error`: Server failure.

### 3. Health Check
- **URL**: `/health`
- **Method**: `GET`
- **Response**: `200 OK`
