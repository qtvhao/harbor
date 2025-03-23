import express, { Application, Request, Response } from 'express';
import bodyParser from 'body-parser';
import { sendMessageToQueue } from './utils/kafkaHelper.js';
import { startKafkaConsumer } from './kafka/kafkaConsumer.js';
import { EachMessagePayload } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

interface Task {
    id: string;
    payload: any;
    accountId: string;
}

class KafkaExpressApp {
    private app: Application;
    private port: number;
    private taskQueue: Task[];
    private pendingTasks: Map<string, Task>;

    constructor(port: number) {
        this.app = express();
        this.port = port;
        this.taskQueue = [];
        this.pendingTasks = new Map();

        this.configureMiddleware();
        this.defineRoutes();
        this.initializeKafkaConsumer();
    }

    private configureMiddleware(): void {
        this.app.use(bodyParser.json());
    }

    private defineRoutes(): void {
        this.app.get('/health', this.healthCheck.bind(this));
        this.app.post('/publish/:topic', this.publishMessage.bind(this));
        this.app.get('/task/fetch', this.fetchTask.bind(this));
        this.app.post('/task/ack/:taskId', this.acknowledgeTask.bind(this));
        this.app.post('/task/nack/:taskId', this.negativelyAcknowledgeTask.bind(this));
    }

    private initializeKafkaConsumer(): void {
        startKafkaConsumer({
            topic: process.env.RESPONSE_TOPIC || 'enkyuu-prompts',
            groupId: 'harbor-group',
            eachMessageHandler: this.handleKafkaMessage.bind(this),
        });
    }

    private async handleKafkaMessage({ message }: EachMessagePayload): Promise<void> {
        const messageValue = message.value?.toString();
        if (!messageValue) return;

        try {
            const payload = JSON.parse(messageValue);

            const accountId = payload.accountId;
            if (!accountId) {
                console.warn('Received message without accountId. Skipping.');
                return;
            }

            const task: Task = {
                id: uuidv4(),
                payload,
                accountId,
            };

            this.taskQueue.push(task);
            console.log(`Task added to queue: ${task.id} for account: ${accountId}`);
        } catch (error) {
            console.error('Failed to parse message:', error);
        }
    }

    private healthCheck(_req: Request, res: Response): void {
        res.status(200).send('OK');
    }

    private async publishMessage(req: Request, res: Response): Promise<void> {
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
    }

    private fetchTask(req: Request, res: Response): void {
        const accountId = req.query.accountId as string;

        if (!accountId) {
            res.status(400).json({ error: 'accountId query parameter is required.' });
            return;
        }

        const taskIndex = this.taskQueue.findIndex(task => task.accountId === accountId);

        if (taskIndex === -1) {
            res.status(404).json({ error: `No tasks available for accountId: ${accountId}` });
            return;
        }

        const [task] = this.taskQueue.splice(taskIndex, 1);
        this.pendingTasks.set(task.id, task);

        res.status(200).json({ task });
    }

    private acknowledgeTask(req: Request, res: Response): void {
        const taskId = req.params.taskId;

        if (!this.pendingTasks.has(taskId)) {
            res.status(404).json({ error: 'Task not found or already acknowledged.' });
            return;
        }

        this.pendingTasks.delete(taskId);
        res.status(200).json({ message: `Task ${taskId} acknowledged successfully.` });
    }

    private negativelyAcknowledgeTask(req: Request, res: Response): void {
        const taskId = req.params.taskId;

        const task = this.pendingTasks.get(taskId);
        if (!task) {
            res.status(404).json({ error: 'Task not found or already acknowledged.' });
            return;
        }

        this.pendingTasks.delete(taskId);
        this.taskQueue.push(task);

        res.status(200).json({ message: `Task ${taskId} re-queued for retry.` });
    }

    public start(): void {
        this.app.listen(this.port, () => {
            console.log(`Express server is running on port ${this.port}`);
        });
    }
}

// Initialize and start the app
const port = Number(process.env.PORT) || 3000;
const kafkaExpressApp = new KafkaExpressApp(port);
kafkaExpressApp.start();
