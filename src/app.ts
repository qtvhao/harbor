import express, { Request, Response } from 'express';
import bodyParser from 'body-parser';
import { sendMessageToQueue } from './utils/kafkaHelper.js';

const app = express();
const port: number = Number(process.env.PORT) || 3000;

// Middleware to parse JSON payloads
app.use(bodyParser.json());

// Health check endpoint
app.get('/health', (_req: Request, res: Response) => {
    res.status(200).send('OK');
});

// POST endpoint to send data to Kafka
app.post('/publish/:topic', async (req: Request, res: Response) => {
    const topic: string = req.params.topic;
    const message: Record<string, unknown> = req.body;

    if (!message || Object.keys(message).length === 0) {
        res.status(400).json({ error: 'Request body is empty or invalid JSON.' });

        return;
    }

    try {
        await sendMessageToQueue(topic, message);
        res.status(200).json({ message: 'Message sent to Kafka successfully.' });
    } catch (error) {
        console.error('Error publishing message:', error);
        res.status(500).json({ error: 'Failed to send message to Kafka.' });
    }
});

// Start the Express server
app.listen(port, () => {
    console.log(`Express server is running on port ${port}`);
});
