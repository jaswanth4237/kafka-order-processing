const { Kafka } = require('kafkajs');
const mysql = require('mysql2/promise');
const express = require('express');
require('dotenv').config();

const app = express();
const port = 8082;

const dbConfigNotification = {
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
};

const kafka = new Kafka({
    clientId: 'notification-service',
    brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS],
});

const consumer = kafka.consumer({ groupId: 'notification-group' });
const producer = kafka.producer();

async function init() {
    await consumer.connect();
    await producer.connect();
    await consumer.subscribe({ topic: 'OrderCreated', fromBeginning: true });
    await consumer.subscribe({ topic: 'InventoryUpdated', fromBeginning: true });
    await consumer.subscribe({ topic: 'OrderFailed', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const eventId = message.headers.eventId?.toString() || message.key?.toString();
            const payload = JSON.parse(message.value.toString());
            const { order_id } = payload;

            const MAX_RETRIES = 3;
            let attempt = 0;
            let success = false;

            while (attempt < MAX_RETRIES && !success) {
                const connection = await mysql.createConnection(dbConfigNotification);

                try {
                    // Idempotency: Check if the event has been processed
                    const [processed] = await connection.execute(
                        'SELECT 1 FROM processed_events WHERE consumer_id = ? AND event_id = ?',
                        ['notification-service', eventId]
                    );

                    if (processed.length > 0) {
                        console.log(`Skipping duplicate notification for event ${eventId}`);
                        success = true;
                        break;
                    }

                    await connection.beginTransaction();

                    // Idempotency: Mark as processed
                    await connection.execute(
                        'INSERT INTO processed_events (consumer_id, event_id) VALUES (?, ?)',
                        ['notification-service', eventId]
                    );

                    await connection.commit();

                    // Notification logging
                    console.log(`[Notification Service] Sending notification for order ${order_id} on topic ${topic}: ${JSON.stringify(payload)}`);
                    success = true;
                } catch (err) {
                    await connection.rollback();
                    attempt++;
                    console.error(`Attempt ${attempt} - Error processing event ${eventId} in notification-service:`, err);

                    if (attempt >= MAX_RETRIES) {
                        // Move to DLQ
                        console.log(`Max retries reached. Moving event ${eventId} to DLQ.`);
                        await producer.send({
                            topic: 'NotificationDLQ',
                            messages: [{ value: JSON.stringify({ originalEvent: payload, error: err.message, eventId }) }],
                        });
                    } else {
                        // Exponential backoff
                        const backoff = Math.pow(2, attempt) * 1000;
                        console.log(`Waiting ${backoff}ms before retrying...`);
                        await new Promise(resolve => setTimeout(resolve, backoff));
                    }
                } finally {
                    await connection.end();
                }
            }
        },
    });
}

init().catch(console.error);

app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

app.listen(port, () => {
    console.log(`Notification Service listening on port ${port}`);
});
