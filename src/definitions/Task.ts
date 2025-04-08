export interface Task {
    id: string;
    content?: string;
    translated?: any[];
    tokens?: any;
    subtaskIds?: string[];
    payload: any;
    downloads?: string[];
    accountId: number;
}
