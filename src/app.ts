import express, { Application, Request, Response } from 'express';
import { KafkaTaskService } from './KafkaTaskService.js';

export class KafkaExpressApp {
    private app: Application;
    private port: number;
    private kafkaTaskService: KafkaTaskService;

    constructor(port: number, kafkaTaskService: KafkaTaskService) {
        this.app = express();
        this.port = port;
        this.kafkaTaskService = kafkaTaskService;

        this.configureMiddleware();
        this.defineRoutes();
    }

    private configureMiddleware(): void {
        this.app.use(express.json());
    }

    private defineRoutes(): void {
        this.app.get('/health', (_req, res) => { res.status(200).send('OK'); });

        this.app.post('/publish/:topic', async (req, res) => {
            const topic = req.params.topic;
            const message = req.body;

            if (!message || Object.keys(message).length === 0) {
                res.status(400).json({ error: 'Request body is empty or invalid JSON.' });
                return;
            }

            try {
                await this.kafkaTaskService.publishMessage(topic, message);
                res.status(200).json({ message: 'Message sent to Kafka successfully.' });
            } catch {
                res.status(500).json({ error: 'Failed to send message to Kafka.' });
            }
        });

        this.app.post('/task/dispatch', (req, res) => this.dispatchJob(req, res));

        this.app.get('/tasks/completed/:taskId', (req, res) => this.getCompletedTask(req, res));

        this.app.get('/tasks/completed/:taskId/downloads/:download', (req, res) => this.downloadCompletedTask(req, res));

        this.app.get('/tasks/progress/:taskId', (req, res) => this.getTaskProgress(req, res));

        this.app.post('/tasks/completed/:taskId/archive', (req, res) => this.archiveCompletedTask(req, res));

        this.app.get('/tasks/completed/account/:accountId', (req, res) => this.getCompletedTasksForAccount(req, res));
    }

    private async dispatchJob(req: Request, res: Response): Promise<void> {
        const { accountId } = req.body;

        if (typeof accountId !== 'number') {
            console.warn('dispatchJob: Received invalid accountId:', req.body);
            res.status(400).json({ error: 'Missing or invalid accountId.' });
            return;
        }

        const task = this.kafkaTaskService.getTaskForAccount(accountId);
        if (!task) {
            res.status(404).json({ error: 'No task available for this account.' });
            return;
        }

        try {
            await this.kafkaTaskService.dispatchTaskToKafka(task);
            res.status(200).json({ taskId: task.id });
        } catch (err) {
            res.status(500).json({ error: 'Failed to dispatch task.' });
        }
    }

    private getCompletedTask(req: Request, res: Response): void {
        const { taskId } = req.params;
        const task = this.kafkaTaskService.getCompletedTask(taskId);

        if (!task) {
            res.status(404).json({ error: 'Task not found or not completed yet.' });
            return;
        }

        res.status(200).json(task);
    }

    private async downloadCompletedTask(req: Request, res: Response): Promise<void> {
        const { taskId, download } = req.params;
        const index = parseInt(download, 10);

        if (isNaN(index)) {
            res.status(400).json({ error: 'Invalid download index.' });
            return;
        }

        const fileName = this.kafkaTaskService.getDownloadFileName(taskId, index);

        if (!fileName) {
            res.status(404).json({ error: 'Download file not found.' });
            return;
        }

        try {
            const stream = await this.kafkaTaskService.getDownloadStream(fileName);
            res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
            stream.pipe(res);
        } catch (err) {
            res.status(500).json({ error: 'Failed to stream download file.' });
        }
    }

    private getTaskProgress(req: Request, res: Response): void {
        const { taskId } = req.params;
        const progress = this.kafkaTaskService.getCurrentAverageProgress(taskId);

        if (progress === null || progress === undefined) {
            res.status(404).json({ error: 'Task not found or progress unavailable.' });
            return;
        }

        const currentStep: string | null = this.kafkaTaskService.getCurrentStep(taskId);

        const totalBars = 20;
        const filledBars = Math.round((progress / 100) * totalBars);
        const emptyBars = totalBars - filledBars;
        const progressBar = '🟩'.repeat(filledBars) + '⬜'.repeat(emptyBars);
        res.status(200).json({ taskId, progress, progressBar, currentStep });
    }

    private archiveCompletedTask(req: Request, res: Response): void {
        const { taskId } = req.params;

        if (!taskId) {
            res.status(400).json({ error: 'Missing taskId parameter.' });
            return;
        }

        try {
            const success = this.kafkaTaskService.taskManager.archiveCompletedTaskById(taskId);

            if (!success) {
                res.status(404).json({ error: 'Task not found or could not be archived.' });
                return;
            }

            res.status(200).json({ message: 'Task archived successfully.' });
        } catch (err) {
            console.error('Error archiving task:', err);
            res.status(500).json({ error: 'Failed to archive task.' });
        }
    }

    private getCompletedTasksForAccount(req: Request, res: Response): void {
        const { accountId } = req.params;

        if (!accountId || isNaN(Number(accountId))) {
            res.status(400).json({ error: 'Missing or invalid accountId parameter.' });
            return;
        }

        try {
            const tasks = this.kafkaTaskService.taskManager.getCompletedTasksForAccount(Number(accountId));

            if (!tasks || tasks.length === 0) {
                res.status(404).json({ error: 'No completed tasks found for this account.' });
                return;
            }

            res.status(200).json({ accountId: Number(accountId), completedTasks: tasks });
        } catch (err) {
            console.error('Error retrieving completed tasks for account:', err);
            res.status(500).json({ error: 'Failed to retrieve completed tasks.' });
        }
    }

    public start(): void {
        this.app.listen(this.port, () => {
            console.log(`Express server is running on port ${this.port}`);
        });
    }
}
