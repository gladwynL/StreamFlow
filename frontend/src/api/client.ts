/** An HTTP response outside the 2xx range from the StreamFlow API. */
export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

/** GETs a JSON resource from the same-origin API. */
export async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { headers: { Accept: 'application/json' }, signal });
  if (!response.ok) {
    throw new ApiError(response.status, `Request to ${path} failed with HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}
