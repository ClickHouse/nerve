/** The schema and the data of a record of a generic hosted source. */
export interface StoredRecordData {
  schema: string;
  data: Record<string, unknown>;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * The schema and the data of a generic hosted record, or `null`.
 *
 * Nerve stores such a record's metadata as exactly `{schema, data}`: the
 * record schema that the gateway named (such as `linear.issue_event/1`) and
 * the JSON data, with every member. The metadata of every other source has
 * other keys.
 */
export function recordData(metadata?: Record<string, unknown> | null): StoredRecordData | null {
  if (!isObject(metadata)) return null;
  const keys = Object.keys(metadata);
  if (keys.length !== 2 || !('schema' in metadata) || !('data' in metadata)) return null;
  const { schema, data } = metadata;
  return typeof schema === 'string' && isObject(data) ? { schema, data } : null;
}
