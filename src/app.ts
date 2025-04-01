import express, { Application, Request, Response } from 'express';
import bodyParser from 'body-parser';
import { sendMessageToQueue } from './utils/kafkaHelper.js';
import { startKafkaConsumer } from './kafka/kafkaConsumer.js';
import { EachMessagePayload } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

interface TaskResponsePayload {
    taskId: string;
    accountId: number;
    downloads: string[];
}

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

    /**
     * Initializes a Kafka consumer to listen for new tasks.
     * 
     * This consumer listens to the topic defined by NEW_TASK_TOPIC (default: 'new-task-topic').
     * Whenever a new message is published to this topic, the handler `handleNewTaskMessage`
     * will process it and push the task into the `taskQueue` for later dispatch.
     */
    private initializeNewTaskConsumer(): void {
        console.debug('Initializing Kafka New Task Consumer');
        startKafkaConsumer({
            topic: process.env.NEW_TASK_TOPIC || 'new-task-topic',
            groupId: 'harbor-new-task-group',
            eachMessageHandler: this.handleNewTaskMessage.bind(this),
        });
    }

    private initializeTaskResponseConsumer(): void {
        console.debug('Initializing Kafka Task Response Consumer');
        startKafkaConsumer({
            fromBeginning: true,
            topic: process.env.TASK_RESPONSE_TOPIC || 'task-response-topic',
            groupId: 'harbor-task-response-group',
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

    /**
     * Handles responses for dispatched tasks from Kafka.
     * 
     * This method listens to the TASK_RESPONSE_TOPIC (default: 'dispatched-task-response').
     * Upon receiving a response message, it parses the message and removes the task from `pendingTasks`.
     * If the task exists, it is marked as completed by adding it to the `completedTasks` map.
     * 
     * This allows clients to later retrieve the completed task using the provided API endpoints.
     */
    private async handleTaskResponseMessage({ message }: EachMessagePayload): Promise<void> {
        console.debug('Handling task response message');
        const rawPayload = this.parseKafkaMessage(message);
        if (!rawPayload) {
            console.debug('Condition: payload is null, exiting handleTaskResponseMessage');
            return;
        }

        const payload: TaskResponsePayload = rawPayload;
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

    /**
     * Dispatches a queued task for a specific account to the Kafka topic.
     * 
     * This endpoint is triggered via POST /task/dispatch?accountId={accountId}.
     * It checks the `taskQueue` for tasks matching the given accountId.
     * If a task is found, it is moved to the `pendingTasks` list and published
     * to the Kafka topic defined by TASK_DISPATCH_TOPIC (default: 'processed-tasks').
     * 
     * If no task is available for the account, it returns a 404 response.
     */
    private async dispatchJob(req: Request, res: Response): Promise<void> {
        const accountId = Number(req.query.accountId);
        const targetTopic = process.env.TASK_DISPATCH_TOPIC || 'task-dispatch-topic';

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
        const downloadIdStr = req.params.download;
        const downloadId = Number(downloadIdStr);
        if (isNaN(downloadId)) {
            console.debug(`Condition: downloadId ${downloadIdStr} is not a number`);
            res.status(400).json({ error: `Download ID must be a number.` });
            return;
        }

        const taskId = req.params.taskId;
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
