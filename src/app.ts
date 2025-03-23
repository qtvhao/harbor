import express, { Application, Request, Response } from 'express';
import bodyParser from 'body-parser';
import { sendMessageToQueue } from './utils/kafkaHelper.js';
import { startKafkaConsumer } from './kafka/kafkaConsumer.js';
import { EachMessagePayload } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

interface Task {
    id: string;
    payload: any;
    accountId: number;
}

class KafkaExpressApp {
    private app: Application;
    private port: number;
    private taskQueue: Task[];
    private pendingTasks: Map<string, Task>;

    constructor(port: number) {
        console.debug('Initializing KafkaExpressApp');
        this.app = express();
        this.port = port;
        this.taskQueue = [];
        this.pendingTasks = new Map();

        this.configureMiddleware();
        this.defineRoutes();
        this.initializeKafkaConsumer();
    }

    private configureMiddleware(): void {
        console.debug('Configuring middleware');
        this.app.use(bodyParser.json());
    }

    private defineRoutes(): void {
        console.debug('Defining routes');
        this.app.get('/health', this.healthCheck.bind(this));
        this.app.post('/publish/:topic', this.publishMessage.bind(this));
        this.app.post('/task/dispatch', this.dispatchJob.bind(this));
        this.app.post('/task/ack/:taskId', this.acknowledgeTask.bind(this));
        this.app.post('/task/nack/:taskId', this.negativelyAcknowledgeTask.bind(this));
    }

    private initializeKafkaConsumer(): void {
        console.debug('Initializing Kafka consumer');
        startKafkaConsumer({
            topic: process.env.RESPONSE_TOPIC || 'enkyuu-prompts',
            groupId: 'harbor-group',
            eachMessageHandler: this.handleKafkaMessage.bind(this),
        });
    }

    private async handleKafkaMessage({ message }: EachMessagePayload): Promise<void> {
        const messageValue = message.value?.toString();
        console.debug('Received Kafka message:', messageValue);
        if (!messageValue) {
            console.warn('Empty message value received, skipping');
            return;
        }

        try {
            const payload = JSON.parse(messageValue);
            console.debug('Parsed payload:', payload);

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
        console.debug('Health check requested');
        res.status(200).send('OK');
    }

    private async publishMessage(req: Request, res: Response): Promise<void> {
        const topic: string = req.params.topic;
        const message: Record<string, unknown> = req.body;

        console.debug(`Publish message request received for topic: ${topic}`, message);

        if (!message || Object.keys(message).length === 0) {
            console.warn('Empty or invalid message received');
            res.status(400).json({ error: 'Request body is empty or invalid JSON.' });
            return;
        }

        try {
            await sendMessageToQueue(topic, message);
            console.log(`Message sent to Kafka topic: ${topic}`);
            res.status(200).json({ message: 'Message sent to Kafka successfully.' });
        } catch (error) {
            console.error('Error publishing message:', error);
            res.status(500).json({ error: 'Failed to send message to Kafka.' });
        }
    }

    private async dispatchJob(req: Request, res: Response): Promise<void> {
        const accountId = Number(req.query.accountId);
        const targetTopic = process.env.JOB_DISPATCH_TOPIC || 'processed-tasks';

        console.debug('Dispatch job request received for accountId:', accountId);

        if (isNaN(accountId)) {
            console.warn('Invalid accountId provided, must be a number');
            res.status(400).json({ error: 'accountId query parameter must be a number.' });
            return;
        }

        if (!accountId) {
            console.warn('No accountId provided in dispatch job request');
            res.status(400).json({ error: 'accountId query parameter is required.' });
            return;
        }

        const taskIndex = this.taskQueue.findIndex(task => task.accountId === accountId);

        if (taskIndex === -1) {
            console.warn(`No tasks found for accountId: ${accountId}`);
            res.status(404).json({ error: `No tasks available for accountId: ${accountId}` });
            return;
        }

        const [task] = this.taskQueue.splice(taskIndex, 1);
        this.pendingTasks.set(task.id, task);

        try {
            await sendMessageToQueue(targetTopic, {
                taskId: task.id,
                payload: task.payload,
                accountId: task.accountId,
            });

            console.log(`Task ${task.id} dispatched to topic ${targetTopic}`);
            res.status(200).json({
                message: `Task ${task.id} dispatched successfully.`,
                taskId: task.id
            });
        } catch (error) {
            console.error('Failed to dispatch job:', error);
            res.status(500).json({ error: 'Failed to dispatch job to Kafka.' });
        }
    }

    private acknowledgeTask(req: Request, res: Response): void {
        const taskId = req.params.taskId;

        console.debug(`Acknowledge task request received for taskId: ${taskId}`);

        if (!this.pendingTasks.has(taskId)) {
            console.warn(`Task ${taskId} not found or already acknowledged`);
            res.status(404).json({ error: 'Task not found or already acknowledged.' });
            return;
        }

        this.pendingTasks.delete(taskId);
        console.log(`Task ${taskId} acknowledged successfully`);
        res.status(200).json({ message: `Task ${taskId} acknowledged successfully.` });
    }

    private negativelyAcknowledgeTask(req: Request, res: Response): void {
        const taskId = req.params.taskId;

        console.debug(`Negative acknowledge request received for taskId: ${taskId}`);

        const task = this.pendingTasks.get(taskId);
        if (!task) {
            console.warn(`Task ${taskId} not found or already acknowledged`);
            res.status(404).json({ error: 'Task not found or already acknowledged.' });
            return;
        }

        this.pendingTasks.delete(taskId);
        this.taskQueue.push(task);

        console.log(`Task ${taskId} re-queued for retry`);
        res.status(200).json({ message: `Task ${taskId} re-queued for retry.` });
    }

    public start(): void {
        console.debug(`Starting Express server on port ${this.port}`);
        this.app.listen(this.port, () => {
            console.log(`Express server is running on port ${this.port}`);
        });
    }
}

// Initialize and start the app
const port = Number(process.env.PORT) || 3000;
console.debug(`Starting KafkaExpressApp on port ${port}`);
const kafkaExpressApp = new KafkaExpressApp(port);
kafkaExpressApp.start();
