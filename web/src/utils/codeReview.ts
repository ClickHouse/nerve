export const reviewTime = (value: string | null) => value ? new Date(value).toLocaleString() : '—';
