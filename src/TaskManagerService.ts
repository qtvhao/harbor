import { Task } from "./definitions/Task.js";
import { Storage } from "./utils/storage.js";

export class TaskManagerService {
    taskQueue: Task[] = [];
    private pendingTasks: Map<string, Task> = new Map();
    private completedTasks: Map<string, Task> = new Map();
    private taskProgress: Map<string, Map<string, number>> = new Map();
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

    public getTaskProgress(parentTaskId: string, taskId: string): number | null {
        const subTaskMap = this.taskProgress.get(parentTaskId);
        return subTaskMap?.get(taskId) ?? null;
    }

    public updateTaskProgress(task: Task, progress: number): number {
        const parentId = task.parentTaskId || task.id;
        if (!this.taskProgress.has(parentId)) {
            this.taskProgress.set(parentId, new Map());
        }
        const subTaskMap = this.taskProgress.get(parentId)!;
        subTaskMap.set(task.id, progress);

        const totalProgress = Array.from(subTaskMap.values()).reduce((sum, p) => sum + p, 0);
        const averageProgress = totalProgress / subTaskMap.size;

        return averageProgress;
    }

    public getTaskById(taskId: string): Task | null {
        return this.pendingTasks.get(taskId) || this.completedTasks.get(taskId) || null;
    }
}
