import { formatAge, shortId } from './format';

describe('formatAge', () => {
  const now = new Date('2026-11-10T19:00:00Z');

  it.each([
    ['2026-11-10T18:59:55Z', '5s ago'],
    ['2026-11-10T18:57:00Z', '3m ago'],
    ['2026-11-10T16:00:00Z', '3h ago'],
    ['2026-11-10T19:00:00Z', 'just now'],
    ['2026-11-10T19:00:10Z', 'just now'],
    ['not a date', 'just now'],
  ])('%s is %s', (iso, expected) => {
    expect(formatAge(iso, now)).toBe(expected);
  });
});

describe('shortId', () => {
  it('keeps the first UUID block', () => {
    expect(shortId('5f0c2a9e-1111-4c2e-9a1b-0f6a7b8c9d0e')).toBe('5f0c2a9e');
  });
});
