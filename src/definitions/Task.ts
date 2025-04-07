export interface Task {
    id: string;
    markdown_text?: string;
    subtaskIds?: string[];
    payload: any;
    downloads?: string[];
    accountId: number;
}
