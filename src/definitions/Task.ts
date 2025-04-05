export interface Task {
    id: string;
    payload: any;
    downloads?: string[];
    accountId: number;
}
