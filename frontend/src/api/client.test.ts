import { jsonResponse } from '../test/fixtures';
import { ApiError, getJson } from './client';

describe('getJson', () => {
  it('returns the parsed body of a successful response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ ok: true })));

    await expect(getJson<{ ok: boolean }>('/api/v1/example')).resolves.toEqual({ ok: true });
  });

  it('throws ApiError with the HTTP status for a non-2xx response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({}, 502)));

    const error = await getJson('/api/v1/example').catch((e: unknown) => e);

    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 502 });
  });
});
