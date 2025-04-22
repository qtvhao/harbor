import { sendMessageToQueue } from './utils/kafkaHelper.js';
import { startKafkaConsumer } from './kafka/kafkaConsumer.js';
import { EachMessagePayload } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import { TaskManagerService } from './TaskManagerService.js';
import { Task } from './definitions/Task.js';
import { config } from './config.js'
import { hostname } from 'os';
import { Lexer } from 'marked';
import { createClient } from '@supabase/supabase-js'
const supabaseUrl = process.env.SUPABASE_URL
const supabaseKey = process.env.SUPABASE_KEY
if (typeof supabaseKey === 'undefined' || !supabaseKey) {
    throw new Error('Missing Supabase key (SUPABASE_KEY). Check your environment variables.')
}
if (typeof supabaseUrl === 'undefined' || !supabaseUrl) {
    throw new Error('Missing Supabase URL (SUPABASE_URL). Check your environment variables.')
}
const supabase = createClient(supabaseUrl, supabaseKey)

interface TaskResponsePayload {
    taskId: string;
    accountId: number;
    content: any;
    translated: any;
    parentTaskId: string;
    downloads: string[];
}

export class KafkaTaskService {
    taskManager = new TaskManagerService();
    private lexer: Lexer;

    constructor() {
        this.initializeNewTaskConsumer();
        this.initializeTaskResponseConsumer();
        this.initializeTaskProgressConsumer();
        this.lexer = new Lexer();
    }

    private initializeNewTaskConsumer(): void {
        startKafkaConsumer('initializeNewTaskConsumer-' + hostname(), {
            topic: process.env.NEW_TASK_TOPIC || 'new-task-topic',
            groupId: 'harbor-new-task-group',
            eachMessageHandler: this.handleNewTaskMessage.bind(this),
        });
    }

    private initializeTaskResponseConsumer(): void {
        startKafkaConsumer('initializeTaskResponseConsumer-' + hostname(), {
            fromBeginning: true,
            topic: process.env.TASK_RESPONSE_TOPIC || 'task-response-topic',
            groupId: 'harbor-task-response-group',
            eachMessageHandler: this.handleTaskResponseMessage.bind(this),
        });
    }

    private initializeTaskProgressConsumer(): void {
        startKafkaConsumer('initializeTaskProgressConsumer-' + hostname(), {
            fromBeginning: true,
            topic: config.kafka.topics.harborProgress,
            groupId: 'harbor-task-progress-group',
            eachMessageHandler: this.handleTaskProgressMessage.bind(this),
        });
        startKafkaConsumer('initializeSubtaskProgressConsumer-' + hostname(), {
            fromBeginning: true,
            topic: config.kafka.topics.subtaskProgress,
            groupId: 'harbor-subtask-progress-group',
            eachMessageHandler: this.handleSubtaskProgressMessage.bind(this),
        });
    }

    private parseKafkaMessage(message: EachMessagePayload['message']): any | null {
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
            typeof payload.accountId === 'number'
        );
    }

    private async handleNewTaskMessage({ message }: EachMessagePayload): Promise<void> {
        const payload = this.parseKafkaMessage(message);
        if (!payload) {
            await new Promise(resolve => setTimeout(resolve, 60000));
            throw new Error('Invalid Kafka message: message body is empty or cannot be parsed');
        }

        if (!this.isValidTaskPayload(payload)) {
            await new Promise(resolve => setTimeout(resolve, 60000));
            throw new Error(`Invalid task payload received: ${JSON.stringify(payload)}`);
        }

        const task: Task = {
            id: uuidv4(),
            payload,
            accountId: payload.accountId,
        };

        this.taskManager.taskQueue.push(task);
    }

    private async handleTaskResponseMessage({ message }: EachMessagePayload): Promise<void> {
        const rawPayload = this.parseKafkaMessage(message);
        const validatedPayload = this.parseAndValidateTaskResponse(rawPayload);
        if (!validatedPayload) return;

        const task = this.taskManager.removePendingTask(validatedPayload.taskId);
        if (task) {
            this.completeTask(task, validatedPayload);
        } else {
            await new Promise(resolve => setTimeout(resolve, 60000));
            throw new Error(`Task with ID ${validatedPayload.taskId} not found in pending tasks`);
        }
    }

    private parseAndValidateTaskResponse(payload: any): TaskResponsePayload | null {
        if (!payload || !payload.taskId) return null;
        payload.translated = payload.content
        payload.content = payload.content.map((clip: any) => clip.original).join("\n\n")
        return payload as TaskResponsePayload;
    }

    private async completeTask(task: Task, rawPayload: TaskResponsePayload): Promise<void> {
        const payload = {
            id: task.id,
            payload: '', // task.payload,
            accountId: task.accountId,
            content: rawPayload.content,
            translated: rawPayload.translated,
            tokens: this.lexer.lex(rawPayload.content),
            downloads: rawPayload.downloads,
        };

        const { data, error } = await supabase
            .from('completed_tasks')
            .insert([
                {
                    accountId: task.accountId,
                    taskId: task.id,
                    payload: JSON.stringify(payload),
                },
            ])
            .select()
        console.log({ data, error })

        this.taskManager.markTaskAsCompleted(payload);
    }

    private async handleTaskProgressMessage({ message }: EachMessagePayload): Promise<void> {
        const progressPayload = this.parseKafkaMessage(message);
        if (typeof progressPayload.parentTaskId !== 'string' || typeof progressPayload.currentStep !== 'string') {
            console.error(`Invalid progress payload: ${JSON.stringify(progressPayload)}`);
            return;
        }
        const task = this.taskManager.getTaskById(progressPayload.parentTaskId);

        const elapsedMs = progressPayload.elapsedMs;
        const currentStep = progressPayload.currentStep;

        if (task) {
            this.taskManager.setCurrentStep(task.id, `[${elapsedMs}ms] ${currentStep}`);
        } else {
            console.error('Progress update received for unknown task. Context:', {
                parentTaskId: progressPayload.parentTaskId,
                currentStep: progressPayload.currentStep,
            });
        }
    }

    private async handleSubtaskProgressMessage({ message }: EachMessagePayload): Promise<void> {
        const progressPayload = this.parseKafkaMessage(message);
        if (!progressPayload) {
            console.log('Invalid Kafka message: progress message body is empty or cannot be parsed')
            await new Promise(resolve => setTimeout(resolve, 60000));
            throw new Error('Invalid Kafka message: progress message body is empty or cannot be parsed');
        }

        if ("completed" === progressPayload.status) {
            progressPayload.progress = 100;
        }

        if (typeof progressPayload.parentTaskId !== 'string' || typeof progressPayload.correlationId !== 'string' || typeof progressPayload.progress !== 'number') {
            console.log(`Invalid progress payload: ${JSON.stringify(progressPayload)}`);
            await new Promise(resolve => setTimeout(resolve, 60000));
            throw new Error(`Invalid progress payload: ${JSON.stringify(progressPayload)}`);
        }

        const task = this.taskManager.getTaskById(progressPayload.parentTaskId);

        if (task) {
            const subtaskId = progressPayload.correlationId;
            const average = this.taskManager.updateSubtaskProgress(task.id, subtaskId, progressPayload.progress);
            console.log(`Updated progress for task ${task.id}: ${progressPayload.progress}%, avg: ${average.toFixed(2)}%`, progressPayload);
        } else {
            console.error('Progress update received for unknown task. Context:', {
                parentTaskId: progressPayload.parentTaskId,
                correlationId: progressPayload.correlationId,
                progress: progressPayload.progress,
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

    public getCurrentAverageProgress(taskId: string): number | null {
        return this.taskManager.getAverageSubtaskProgress(taskId);
    }

    public getCurrentStep(taskId: string): string | null {
        return this.taskManager.getCurrentStep(taskId)
    }
}
