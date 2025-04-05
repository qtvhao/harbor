import { Task } from "./definitions/Task";
import { Storage } from "./utils/storage";

export class TaskManagerService {
    taskQueue: Task[] = [];
    private pendingTasks: Map<string, Task> = new Map();
    private completedTasks: Map<string, Task> = new Map();
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
}
