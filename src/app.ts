import express, { Request, Response } from 'express';
import bodyParser from 'body-parser';
import { sendMessageToQueue } from './utils/kafkaHelper.js';
import { startKafkaConsumer } from './kafka/kafkaConsumer.js';
import { EachMessagePayload } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

const app = express();
const port: number = Number(process.env.PORT) || 3000;

// In-memory task queue and pending task tracker
interface Task {
    id: string;
    payload: any;
}

const taskQueue: Task[] = [];
const pendingTasks: Map<string, Task> = new Map();

// Kafka Consumer subscribes to RESPONSE_TOPIC and adds tasks to the queue
startKafkaConsumer({
    topic: process.env.RESPONSE_TOPIC || 'enkyuu-prompts',
    groupId: 'harbor-group',
    eachMessageHandler: async (a: EachMessagePayload) => {
        const messageValue = a.message.value?.toString();
        if (!messageValue) return;

        try {
            const payload = JSON.parse(messageValue);
            const task: Task = {
                id: uuidv4(),
                payload
            };

            taskQueue.push(task);
            console.log(`Task added to queue: ${task.id}`);
        } catch (error) {
            console.error('Failed to parse message:', error);
        }
    }
});

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

// GET endpoint to fetch a task from the queue
app.get('/task/fetch', (_req: Request, res: Response) => {
    if (taskQueue.length === 0) {
        res.status(404).json({ error: 'No tasks available.' });
        return;
    }

    const task = taskQueue.shift()!;
    pendingTasks.set(task.id, task);

    res.status(200).json({ task });
});

// POST endpoint to acknowledge task success
app.post('/task/ack/:taskId', (req: Request, res: Response) => {
    const taskId = req.params.taskId;

    if (!pendingTasks.has(taskId)) {
        res.status(404).json({ error: 'Task not found or already acknowledged.' });
        return;
    }

    pendingTasks.delete(taskId);
    res.status(200).json({ message: `Task ${taskId} acknowledged successfully.` });
});

// POST endpoint to negatively acknowledge task failure and retry
app.post('/task/nack/:taskId', (req: Request, res: Response) => {
    const taskId = req.params.taskId;

    const task = pendingTasks.get(taskId);
    if (!task) {
        res.status(404).json({ error: 'Task not found or already acknowledged.' });
        return;
    }

    pendingTasks.delete(taskId);
    taskQueue.push(task);

    res.status(200).json({ message: `Task ${taskId} re-queued for retry.` });
});

// Start the Express server
app.listen(port, () => {
    console.log(`Express server is running on port ${port}`);
});
