/** Header carrying the operator key on state-changing requests (never in a URL). */
export const OPERATOR_HEADER = 'X-StreamFlow-Operator-Key';

/** An HTTP response outside the 2xx range from the StreamFlow API. */
export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

/** The problem detail's `detail`, if the body is an RFC 9457 problem; otherwise a generic message. */
async function errorFor(response: Response, path: string): Promise<ApiError> {
  let message = `Request to ${path} failed with HTTP ${response.status}`;
  try {
    const body = (await response.json()) as { detail?: unknown };
    if (typeof body.detail === 'string' && body.detail.length > 0) {
      message = body.detail;
    }
  } catch {
    // Not JSON: keep the generic message.
  }
  return new ApiError(response.status, message);
}

/** GETs a JSON resource from the same-origin API. */
export async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { headers: { Accept: 'application/json' }, signal });
  if (!response.ok) {
    throw await errorFor(response, path);
  }
  return (await response.json()) as T;
}

/** GETs a JSON resource, or null for 204 No Content. */
export async function getOptionalJson<T>(path: string, signal?: AbortSignal): Promise<T | null> {
  const response = await fetch(path, { headers: { Accept: 'application/json' }, signal });
  if (response.status === 204) {
    return null;
  }
  if (!response.ok) {
    throw await errorFor(response, path);
  }
  return (await response.json()) as T;
}

/** POSTs JSON as an operator action. The key travels only in the request header. */
export async function postOperator<T>(
  path: string,
  operatorKey: string,
  body?: unknown,
): Promise<T | null> {
  const response = await fetch(path, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      [OPERATOR_HEADER]: operatorKey,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
    cache: 'no-store',
    credentials: 'omit',
  });
  if (!response.ok) {
    throw await errorFor(response, path);
  }
  const text = await response.text();
  return text.length === 0 ? null : (JSON.parse(text) as T);
}
