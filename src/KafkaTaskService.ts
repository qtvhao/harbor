import { sendMessageToQueue } from './utils/kafkaHelper.js';
import { startKafkaConsumer } from './kafka/kafkaConsumer.js';
import { EachMessagePayload } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import { TaskManagerService } from './TaskManagerService.js';
import { Task } from './definitions/Task.js';

interface TaskResponsePayload {
    taskId: string;
    accountId: number;
    parentTaskId: string;
    downloads: string[];
}

export class KafkaTaskService {
    private taskManager = new TaskManagerService();
    
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

    private isValidTaskPayload(payload: TaskResponsePayload): boolean {
        return (
            typeof payload.accountId === 'number' &&
            typeof payload.parentTaskId === 'string' &&
            Array.isArray(payload.downloads)
        );
    }

    private async handleNewTaskMessage({ message }: EachMessagePayload): Promise<void> {
        const payload = this.parseKafkaMessage(message);
        if (!payload || !this.isValidTaskPayload(payload)) return;

        const task: Task = {
            id: uuidv4(),
            parentTaskId: payload.parentTaskId,
            payload,
            accountId: payload.accountId,
        };

        this.taskManager.taskQueue.push(task);
    }

    private async handleTaskResponseMessage({ message }: EachMessagePayload): Promise<void> {
        const rawPayload = this.parseKafkaMessage(message);
        if (!rawPayload || !rawPayload.taskId) return;

        const task = this.taskManager.removePendingTask(rawPayload.taskId);
        if (task) {
            this.taskManager.markTaskAsCompleted({
                ...task,
                downloads: rawPayload.downloads,
            });
        }
    }

    public async publishMessage(topic: string, message: Record<string, unknown>): Promise<void> {
        await sendMessageToQueue(topic, message);
    }

    public getTaskForAccount(accountId: number): Task | null {
        return this.taskManager.getTaskForAccount(accountId);
    }

    public getCompletedTask(taskId: string): Task | null {
        return this.taskManager.getCompletedTask(taskId);
    }

    public getDownloadFileName(taskId: string, downloadIndex: number): string | null {
        return this.taskManager.getDownloadFileName(taskId, downloadIndex);
    }

    public async getDownloadStream(fileName: string): Promise<NodeJS.ReadableStream> {
        return this.taskManager.getDownloadStream(fileName);
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
