CREATE DATABASE IF NOT EXISTS orderdb;
CREATE DATABASE IF NOT EXISTS inventorydb;
CREATE DATABASE IF NOT EXISTS notificationdb;

-- Order Service Database
USE orderdb;

CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    items JSON NOT NULL,
    status ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED') DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    event_id VARCHAR(36) NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id VARCHAR(36) PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(36) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Inventory Service Database
USE inventorydb;

CREATE TABLE IF NOT EXISTS inventory (
    sku VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    stock INT NOT NULL,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_id VARCHAR(50),
    event_id VARCHAR(36),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_id, event_id)
);

-- Seed Inventory Data
INSERT INTO inventory (sku, name, stock) VALUES
('PROD-001', 'High-Performance Laptop', 10),
('PROD-002', 'Wireless Noise-Cancelling Headphones', 25),
('PROD-003', 'Ergonomic Mechanical Keyboard', 15),
('PROD-004', '4K Ultra HD Monitor', 8),
('PROD-005', 'Speedy External SSD 1TB', 30);

-- Notification Service Database (sharing processed_events for idempotency)
USE notificationdb;

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_id VARCHAR(50),
    event_id VARCHAR(36),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_id, event_id)
);
