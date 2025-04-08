export interface Task {
    id: string;
    markdown_text?: string;
    tokens?: any;
    subtaskIds?: string[];
    payload: any;
    downloads?: string[];
    accountId: number;
}
