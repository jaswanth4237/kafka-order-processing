const express = require('express');
const { Kafka } = require('kafkajs');
const mysql = require('mysql2/promise');
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

const app = express();
app.use(express.json());

const port = process.env.PORT || 8080;

const dbConfig = {
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
};

const kafka = new Kafka({
    clientId: 'order-service',
    brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS],
});

const producer = kafka.producer();

async function init() {
    await producer.connect();
    console.log('Producer connected');
}

init().catch(console.error);

// Transactional Outbox Poller
async function pollOutbox() {
    let connection;
    try {
        connection = await mysql.createConnection(dbConfig);
        const [rows] = await connection.execute(
            'SELECT * FROM outbox_events WHERE processed = FALSE LIMIT 10'
        );

        for (const event of rows) {
            await producer.send({
                topic: 'OrderCreated',
                messages: [{ 
                    key: event.aggregate_id, 
                    value: JSON.stringify(event.payload),
                    headers: { eventId: event.id }
                }],
            });

            await connection.execute(
                'UPDATE outbox_events SET processed = TRUE WHERE id = ?',
                [event.id]
            );
            console.log(`Processed event ${event.id}`);
        }
    } catch (err) {
        console.error('Outbox poll error:', err);
    } finally {
        if (connection) await connection.end();
        setTimeout(pollOutbox, 2000);
    }
}

pollOutbox();

app.post('/api/orders', async (req, res) => {
    const { user_id, items } = req.body;
    if (!user_id || !items || !Array.isArray(items)) {
        return res.status(400).json({ error: 'Invalid request payload' });
    }

    const order_id = uuidv4();
    const event_id = uuidv4();
    const connection = await mysql.createConnection(dbConfig);

    try {
        await connection.beginTransaction();

        // 1. Save Order
        await connection.execute(
            'INSERT INTO orders (id, user_id, items, status, event_id) VALUES (?, ?, ?, ?, ?)',
            [order_id, user_id, JSON.stringify(items), 'PENDING', event_id]
        );

        // 2. Save Event to Outbox
        const eventPayload = { order_id, user_id, items, event_id };
        await connection.execute(
            'INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload) VALUES (?, ?, ?, ?, ?)',
            [event_id, 'ORDER', order_id, 'OrderCreated', JSON.stringify(eventPayload)]
        );

        await connection.commit();
        res.status(202).json({ order_id, status: 'PENDING' });
    } catch (err) {
        await connection.rollback();
        console.error('Order creation error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        await connection.end();
    }
});

app.get('/api/orders/:orderId', async (req, res) => {
    const { orderId } = req.params;
    let connection;
    try {
        connection = await mysql.createConnection(dbConfig);
        const [rows] = await connection.execute(
            'SELECT * FROM orders WHERE id = ?',
            [orderId]
        );

        if (rows.length === 0) {
            return res.status(404).json({ error: 'Order not found' });
        }

        const order = rows[0];
        res.json({
            order_id: order.id,
            status: order.status,
            items: order.items,
            created_at: order.created_at,
            updated_at: order.updated_at
        });
    } catch (err) {
        console.error('Fetch order error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        if (connection) await connection.end();
    }
});

app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

app.listen(port, () => {
    console.log(`Order Service listening on port ${port}`);
});
