const { Kafka } = require('kafkajs');
const mysql = require('mysql2/promise');
const express = require('express');
require('dotenv').config();

const app = express();
const port = 8081;

const dbConfigInventory = {
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
};

const dbConfigOrder = {
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_ORDER_DATABASE,
};

const kafka = new Kafka({
    clientId: 'inventory-service',
    brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS],
});

const consumer = kafka.consumer({ groupId: 'inventory-group' });
const producer = kafka.producer();

async function init() {
    await consumer.connect();
    await producer.connect();
    await consumer.subscribe({ topic: 'OrderCreated', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const eventId = message.headers.eventId?.toString();
            const payload = JSON.parse(message.value.toString());
            const { order_id, items } = payload;

            const MAX_RETRIES = 3;
            let attempt = 0;
            let success = false;

            while (attempt < MAX_RETRIES && !success) {
                const connectionInventory = await mysql.createConnection(dbConfigInventory);
                const connectionOrder = await mysql.createConnection(dbConfigOrder);

                try {
                    // IDEMPOTENCY CHECK
                    const [processed] = await connectionInventory.execute(
                        'SELECT 1 FROM processed_events WHERE consumer_id = ? AND event_id = ?',
                        ['inventory-service', eventId]
                    );

                    if (processed.length > 0) {
                        console.log(`Skipping duplicate event ${eventId}`);
                        success = true;
                        break;
                    }

                    await connectionInventory.beginTransaction();
                    await connectionOrder.beginTransaction();

                    let stockAvailable = true;
                    for (const item of items) {
                        const [stockRows] = await connectionInventory.execute(
                            'SELECT stock FROM inventory WHERE sku = ? FOR UPDATE',
                            [item.sku]
                        );

                        if (stockRows.length === 0 || stockRows[0].stock < item.quantity) {
                            stockAvailable = false;
                            break;
                        }
                    }

                    if (stockAvailable) {
                        // Deduct Stock
                        for (const item of items) {
                            await connectionInventory.execute(
                                'UPDATE inventory SET stock = stock - ? WHERE sku = ?',
                                [item.quantity, item.sku]
                            );
                        }

                        // Update Order Status in Order DB
                        await connectionOrder.execute(
                            'UPDATE orders SET status = ? WHERE id = ?',
                            ['PROCESSING', order_id]
                        );

                        // Idempotency: Mark as processed
                        await connectionInventory.execute(
                            'INSERT INTO processed_events (consumer_id, event_id) VALUES (?, ?)',
                            ['inventory-service', eventId]
                        );

                        await connectionInventory.commit();
                        await connectionOrder.commit();

                        // Publish InventoryUpdated event
                        await producer.send({
                            topic: 'InventoryUpdated',
                            messages: [{
                                key: order_id,
                                value: JSON.stringify({ order_id, status: 'PROCESSING', eventId })
                            }],
                        });
                        console.log(`Inventory updated for order ${order_id}`);
                    } else {
                        // Logic for failure (insufficient stock)
                        await connectionOrder.execute(
                            'UPDATE orders SET status = ? WHERE id = ?',
                            ['FAILED', order_id]
                        );

                        await connectionInventory.execute(
                            'INSERT INTO processed_events (consumer_id, event_id) VALUES (?, ?)',
                            ['inventory-service', eventId]
                        );

                        await connectionInventory.commit();
                        await connectionOrder.commit();

                        // Publish OrderFailed event
                        await producer.send({
                            topic: 'OrderFailed',
                            messages: [{
                                key: order_id,
                                value: JSON.stringify({ order_id, status: 'FAILED', reason: 'Insufficient Stock', eventId })
                            }],
                        });
                        console.log(`Order ${order_id} failed due to stock`);
                    }

                    success = true; // Mark as successful to exit loop
                } catch (err) {
                    await connectionInventory.rollback();
                    await connectionOrder.rollback();
                    attempt++;
                    console.error(`Attempt ${attempt} - Error processing OrderCreated event ${eventId}:`, err);

                    if (attempt >= MAX_RETRIES) {
                        // Move to DLQ
                        console.log(`Max retries reached. Moving event ${eventId} to DLQ.`);
                        await producer.send({
                            topic: 'InventoryDLQ',
                            messages: [{ value: JSON.stringify({ originalEvent: payload, error: err.message, eventId }) }],
                        });
                    } else {
                        // Exponential backoff
                        const backoff = Math.pow(2, attempt) * 1000;
                        console.log(`Waiting ${backoff}ms before retrying...`);
                        await new Promise(resolve => setTimeout(resolve, backoff));
                    }
                } finally {
                    await connectionInventory.end();
                    await connectionOrder.end();
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
    console.log(`Inventory Service listening on port ${port}`);
});
