export type OrderPart = [field: string, direction: 'asc' | 'desc'];
export type Ordering = OrderPart[];

export type SimpleOperator = EqualityOps | OrderOps | InOps | LikeOps;
export type EqualityOps = '=' | '!=';
export type OrderOps = '<' | '>' | '<=' | '>=';
export type InOps = 'IN' | 'NOT IN';
export type LikeOps = 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';
