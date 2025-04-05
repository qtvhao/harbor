export interface Task {
    id: string;
    parentTaskId: string;
    payload: any;
    downloads?: string[];
    accountId: number;
}
