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
    private completedTasks: Map<string, Task>;

    constructor(port: number) {
        console.debug('Initializing KafkaExpressApp');
        this.app = express();
        this.port = port;
        this.taskQueue = [];
        this.pendingTasks = new Map();
        this.completedTasks = new Map();

        this.configureMiddleware();
        this.defineRoutes();

        this.initializeNewTaskConsumer();
        this.initializeTaskResponseConsumer();
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
        this.app.get('/tasks/completed/:taskId', this.getCompletedTask.bind(this));
        this.app.get('/tasks/completed/:taskId/downloads/:download', this.downloadCompletedTask.bind(this));
    }

    private initializeNewTaskConsumer(): void {
        console.debug('Initializing Kafka New Task Consumer');
        startKafkaConsumer({
            topic: process.env.DELAYED_RESPONSE_TOPIC || 'enkyuu-prompts',
            groupId: 'harbor-group',
            eachMessageHandler: this.handleNewTaskMessage.bind(this),
        });
    }

    private initializeTaskResponseConsumer(): void {
        console.debug('Initializing Kafka Task Response Consumer');
        startKafkaConsumer({
            topic: process.env.DISPATCHED_RESPONSE_TOPIC || 'dispatched-task-response',
            groupId: 'harbor-group',
            eachMessageHandler: this.handleTaskResponseMessage.bind(this),
        });
    }

    private parseKafkaMessage(message: EachMessagePayload['message']): any | null {
        const messageValue = message.value?.toString();
        console.debug('Received Kafka message:', messageValue);

        if (!messageValue) {
            console.debug('Condition: messageValue is empty');
            console.warn('Empty message value received, skipping');
            return null;
        }

        try {
            const payload = JSON.parse(messageValue);
            console.debug('Condition: messageValue parsed successfully');
            console.debug('Parsed payload:', payload);
            return payload;
        } catch (error) {
            console.debug('Condition: messageValue failed to parse');
            console.error('Failed to parse Kafka message:', error);
            return null;
        }
    }

    private removePendingTask(taskId: string): Task | null {
        console.debug(`Attempting to remove task with ID: ${taskId} from pendingTasks`);
        const task = this.pendingTasks.get(taskId);
        
        if (!task) {
            console.debug(`Condition: task with ID ${taskId} not found`);
            console.warn(`Task ${taskId} not found in pendingTasks`);
            return null;
        }

        this.pendingTasks.delete(taskId);
        console.debug(`Condition: task with ID ${taskId} successfully removed from pendingTasks`);
        console.log(`Task ${taskId} removed from pending tasks`);
        return task;
    }

    private markTaskAsCompleted(task: Task): void {
        console.debug(`Marking task ${task.id} as completed`);
        this.completedTasks.set(task.id, task);
        console.debug(`Condition: task ${task.id} added to completedTasks`);
        console.log(`Task ${task.id} marked as completed`);
    }

    private async handleNewTaskMessage({ message }: EachMessagePayload): Promise<void> {
        console.debug('Handling new task message');
        const payload = this.parseKafkaMessage(message);
        if (!payload) {
            console.debug('Condition: payload is null, exiting handleNewTaskMessage');
            return;
        }

        const accountId = payload.accountId;
        if (!accountId) {
            console.debug('Condition: payload missing accountId');
            console.warn('Received message without accountId. Skipping.');
            return;
        }

        const task: Task = {
            id: uuidv4(),
            payload,
            accountId,
        };

        this.taskQueue.push(task);
        console.debug(`Condition: task ${task.id} pushed to taskQueue`);
        console.log(`Task added to queue: ${task.id} for account: ${accountId}`);
    }

    private async handleTaskResponseMessage({ message }: EachMessagePayload): Promise<void> {
        console.debug('Handling task response message');
        const payload = this.parseKafkaMessage(message);
        if (!payload) {
            console.debug('Condition: payload is null, exiting handleTaskResponseMessage');
            return;
        }

        const taskId = payload.taskId;
        if (!taskId) {
            console.debug('Condition: taskId missing from payload');
            console.warn('Received message without taskId in handleTaskResponseMessage, skipping.');
            return;
        }

        const task = this.removePendingTask(taskId);

        if (task) {
            console.debug(`Condition: task ${taskId} found and removed from pendingTasks`);
            this.markTaskAsCompleted(task);
        } else {
            console.debug(`Condition: task ${taskId} not found in pendingTasks`);
            console.warn(`Could not mark task ${taskId} as completed because it was not found in pendingTasks`);
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
            console.debug('Condition: message is empty or invalid');
            console.warn('Empty or invalid message received');
            res.status(400).json({ error: 'Request body is empty or invalid JSON.' });
            return;
        }

        try {
            await sendMessageToQueue(topic, message);
            console.debug(`Condition: message successfully sent to Kafka topic ${topic}`);
            console.log(`Message sent to Kafka topic: ${topic}`);
            res.status(200).json({ message: 'Message sent to Kafka successfully.' });
        } catch (error) {
            console.debug('Condition: failed to send message to Kafka');
            console.error('Error publishing message:', error);
            res.status(500).json({ error: 'Failed to send message to Kafka.' });
        }
    }

    private async dispatchJob(req: Request, res: Response): Promise<void> {
        const accountId = Number(req.query.accountId);
        const targetTopic = process.env.JOB_DISPATCH_TOPIC || 'processed-tasks';

        console.debug('Dispatch job request received for accountId:', accountId);

        if (isNaN(accountId)) {
            console.debug('Condition: accountId is not a number');
            console.warn('Invalid accountId provided, must be a number');
            res.status(400).json({ error: 'accountId query parameter must be a number.' });
            return;
        }

        if (!accountId) {
            console.debug('Condition: accountId is missing');
            console.warn('No accountId provided in dispatch job request');
            res.status(400).json({ error: 'accountId query parameter is required.' });
            return;
        }

        const taskIndex = this.taskQueue.findIndex(task => task.accountId === accountId);

        if (taskIndex === -1) {
            console.debug(`Condition: no tasks found for accountId ${accountId}`);
            console.warn(`No tasks found for accountId: ${accountId}`);
            res.status(404).json({ error: `No tasks available for accountId: ${accountId}` });
            return;
        }

        const [task] = this.taskQueue.splice(taskIndex, 1);
        console.debug(`Condition: task ${task.id} removed from taskQueue`);
        this.pendingTasks.set(task.id, task);
        console.debug(`Condition: task ${task.id} added to pendingTasks`);

        try {
            await sendMessageToQueue(targetTopic, {
                taskId: task.id,
                payload: task.payload,
                accountId: task.accountId,
            });

            console.debug(`Condition: task ${task.id} dispatched to topic ${targetTopic}`);
            console.log(`Task ${task.id} dispatched to topic ${targetTopic}`);
            res.status(200).json({
                message: `Task ${task.id} dispatched successfully.`,
                taskId: task.id
            });
        } catch (error) {
            console.debug(`Condition: failed to dispatch task ${task.id} to topic ${targetTopic}`);
            console.error('Failed to dispatch job:', error);
            res.status(500).json({ error: 'Failed to dispatch job to Kafka.' });
        }
    }

    private getCompletedTask(req: Request, res: Response): void {
        const taskId = req.params.taskId;
        console.debug(`Fetching completed task with ID: ${taskId}`);

        const task = this.completedTasks.get(taskId);

        if (!task) {
            console.debug(`Condition: completed task with ID ${taskId} not found`);
            res.status(404).json({ error: `Completed task with ID ${taskId} not found.` });
            return;
        }

        console.debug(`Condition: completed task with ID ${taskId} found`);
        res.status(200).json({ task });
    }

    private downloadCompletedTask(req: Request, res: Response): void {
        const taskId = req.params.taskId;
        const downloadId = req.params.download;
        console.debug(`Download request received for task ID: ${taskId} and download ID: ${downloadId}`);

        const task = this.completedTasks.get(taskId);

        if (!task) {
            console.debug(`Condition: task ${taskId} not found in completedTasks`);
            res.status(404).json({ error: `Completed task with ID ${taskId} not found.` });
            return;
        }

        const downloadData = task.payload.downloads?.[downloadId];

        if (!downloadData) {
            console.debug(`Condition: download ${downloadId} not found for task ${taskId}`);
            res.status(404).json({ error: `Download ${downloadId} not found for task ${taskId}.` });
            return;
        }

        console.debug(`Condition: download ${downloadId} found for task ${taskId}`);
        res.status(200).json({ download: downloadData });
    }

    public start(): void {
        console.debug(`Starting Express server on port ${this.port}`);
        this.app.listen(this.port, () => {
            console.log(`Express server is running on port ${this.port}`);
        });
    }
}

const port = Number(process.env.PORT) || 3000;
console.debug(`Starting KafkaExpressApp on port ${port}`);
const kafkaExpressApp = new KafkaExpressApp(port);
kafkaExpressApp.start();
