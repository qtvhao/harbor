import { Task } from "./definitions/Task.js";
import { Storage } from "./utils/storage.js";

export class TaskManagerService {
    taskQueue: Task[] = [];
    private pendingTasks: Map<string, Task> = new Map();
    private completedTasks: Map<string, Task> = new Map();
    private taskProgress: Map<string, Map<string, number>> = new Map();
    private currentStepMap: Map<string, string> = new Map();
    private storage: Storage = Storage.getInstance();

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

    public removePendingTask(taskId: string): Task | null {
        const task = this.pendingTasks.get(taskId);
        if (!task) return null;

        this.pendingTasks.delete(taskId);
        return task;
    }

    public markTaskAsCompleted(task: Task): void {
        this.completedTasks.set(task.id, task);
    }

    public getTaskById(taskId: string): Task | null {
        return this.pendingTasks.get(taskId) || this.completedTasks.get(taskId) || null;
    }

    public updateSubtaskProgress(taskId: string, subtaskId: string, progress: number): number {
        if (!this.taskProgress.has(taskId)) {
            this.taskProgress.set(taskId, new Map());
        }

        const subtaskProgress = this.taskProgress.get(taskId)!;
        subtaskProgress.set(subtaskId, progress);

        return this.getAverageSubtaskProgress(taskId)
    }

    public getAverageSubtaskProgress(taskId: string): number {
        const subtasks = this.taskProgress.get(taskId);
        if (!subtasks || subtasks.size === 0) {
            return 0;
        }

        const totalProgress = Array.from(subtasks.values()).reduce((sum, val) => sum + val, 0);
        return totalProgress / subtasks.size;
    }

    public setCurrentStep(taskId: string, step: string): void {
        this.currentStepMap.set(taskId, step);
    }

    public getCurrentStep(taskId: string): string | null {
        return this.currentStepMap.has(taskId) ? this.currentStepMap.get(taskId)! : null;
    }

    public clearCurrentStep(taskId: string): void {
        this.currentStepMap.delete(taskId);
    }
}
