export interface Task {
    id: string;
    subtaskIds?: string[];
    payload: any;
    downloads?: string[];
    accountId: number;
}
