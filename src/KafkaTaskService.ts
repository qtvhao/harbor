import { sendMessageToQueue } from './utils/kafkaHelper.js';
import { startKafkaConsumer } from './kafka/kafkaConsumer.js';
import { EachMessagePayload } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import { Storage } from './utils/storage.js';

interface TaskResponsePayload {
    taskId: string;
    accountId: number;
    downloads: string[];
}

interface Task {
    id: string;
    payload: any;
    downloads?: string[];
    accountId: number;
}

export class KafkaTaskService {
    private taskQueue: Task[] = [];
    private pendingTasks: Map<string, Task> = new Map();
    private completedTasks: Map<string, Task> = new Map();
    private storage: Storage = Storage.getInstance();

    constructor() {
        this.initializeNewTaskConsumer();
        this.initializeTaskResponseConsumer();
    }

    private initializeNewTaskConsumer(): void {
        startKafkaConsumer({
            topic: process.env.NEW_TASK_TOPIC || 'new-task-topic',
            groupId: 'harbor-new-task-group',
            eachMessageHandler: this.handleNewTaskMessage.bind(this),
        });
    }

    private initializeTaskResponseConsumer(): void {
        startKafkaConsumer({
            fromBeginning: true,
            topic: process.env.TASK_RESPONSE_TOPIC || 'task-response-topic',
            groupId: 'harbor-task-response-group',
            eachMessageHandler: this.handleTaskResponseMessage.bind(this),
        });
    }

    private parseKafkaMessage(message: EachMessagePayload['message']): TaskResponsePayload | null {
        const messageValue = message.value?.toString();
        if (!messageValue) return null;

        try {
            return JSON.parse(messageValue);
        } catch {
            return null;
        }
    }

    private removePendingTask(taskId: string): Task | null {
        const task = this.pendingTasks.get(taskId);
        if (!task) return null;

        this.pendingTasks.delete(taskId);
        return task;
    }

    private markTaskAsCompleted(task: Task): void {
        this.completedTasks.set(task.id, task);
    }

    private async handleNewTaskMessage({ message }: EachMessagePayload): Promise<void> {
        const payload = this.parseKafkaMessage(message);
        if (!payload || !payload.accountId) return;

        const task: Task = {
            id: uuidv4(),
            payload,
            accountId: payload.accountId,
        };

        this.taskQueue.push(task);
    }

    private async handleTaskResponseMessage({ message }: EachMessagePayload): Promise<void> {
        const rawPayload = this.parseKafkaMessage(message);
        if (!rawPayload || !rawPayload.taskId) return;

        const task = this.removePendingTask(rawPayload.taskId);
        if (task) {
            this.markTaskAsCompleted({
                ...task,
                downloads: rawPayload.downloads,
            });
        }
    }

    public async publishMessage(topic: string, message: Record<string, unknown>): Promise<void> {
        await sendMessageToQueue(topic, message);
    }

    public getTaskForAccount(accountId: number): Task | null {
        const taskIndex = this.taskQueue.findIndex(task => task.accountId === accountId);
        if (taskIndex === -1) {
            return null;
        }

        const [task] = this.taskQueue.splice(taskIndex, 1);
        this.pendingTasks.set(task.id, task);
        return task;
    }

    public getCompletedTask(taskId: string): Task | null {
        return this.completedTasks.get(taskId) || null;
    }

    public getDownloadFileName(taskId: string, downloadIndex: number): string | null {
        const task = this.completedTasks.get(taskId);
        return task?.downloads?.[downloadIndex] || null;
    }

    public async getDownloadStream(fileName: string): Promise<NodeJS.ReadableStream> {
        return this.storage.getReadableStream(fileName);
    }

    public async dispatchTaskToKafka(task: Task): Promise<void> {
        const targetTopic = process.env.TASK_DISPATCH_TOPIC || 'task-dispatch-topic';
        await sendMessageToQueue(targetTopic, {
            taskId: task.id,
            payload: task.payload,
            accountId: task.accountId,
        });
    }
}
