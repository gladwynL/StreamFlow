import { EVENT_TYPES, type StartRequest } from '../../api/types';
import { formatCount } from '../../lib/format';

/** Server-enforced bounds (SimulationController); mirrored here only to guide the form. */
export const LIMITS = { events: 500_000, rate: 5_000, seconds: 600, users: 100_000 };

export interface FormState {
  mode: 'count' | 'duration';
  events: string;
  durationSeconds: string;
  rate: string;
  users: string;
  seed: string;
  types: string[];
}

export const DEFAULT_FORM: FormState = {
  mode: 'count',
  events: '20000',
  durationSeconds: '30',
  rate: '1000',
  users: '10000',
  seed: '',
  types: [...EVENT_TYPES],
};

/** Validates the form like the server does; returns the request or the first problem. */
export function toRequest(form: FormState): { request?: StartRequest; error?: string } {
  const rate = Number(form.rate);
  if (!Number.isInteger(rate) || rate < 1 || rate > LIMITS.rate) {
    return { error: `Rate must be a whole number from 1 to ${formatCount(LIMITS.rate)} events/s.` };
  }
  const request: StartRequest = { ratePerSecond: rate };
  let events: number;
  if (form.mode === 'count') {
    events = Number(form.events);
    if (!Number.isInteger(events) || events < 1 || events > LIMITS.events) {
      return { error: `Events must be a whole number from 1 to ${formatCount(LIMITS.events)}.` };
    }
    request.events = events;
  } else {
    const seconds = Number(form.durationSeconds);
    if (!Number.isInteger(seconds) || seconds < 1 || seconds > LIMITS.seconds) {
      return { error: `Duration must be a whole number of seconds from 1 to ${LIMITS.seconds}.` };
    }
    events = seconds * rate;
    if (events > LIMITS.events) {
      return {
        error: `That is ${formatCount(events)} events; the limit is ${formatCount(LIMITS.events)}.`,
      };
    }
    request.durationSeconds = seconds;
  }
  if (Math.ceil(events / rate) > LIMITS.seconds) {
    return {
      error: `At ${formatCount(rate)}/s this run would take longer than ${LIMITS.seconds} s.`,
    };
  }
  const users = Number(form.users);
  if (!Number.isInteger(users) || users < 1 || users > LIMITS.users) {
    return { error: `Users must be a whole number from 1 to ${formatCount(LIMITS.users)}.` };
  }
  request.users = users;
  if (form.seed.trim() !== '') {
    const seed = Number(form.seed);
    if (!Number.isSafeInteger(seed) || seed < 0) {
      return { error: 'Seed must be a non-negative whole number (or empty for a random seed).' };
    }
    request.seed = seed;
  }
  if (form.types.length === 0) {
    return { error: 'Select at least one event type.' };
  }
  if (form.types.length < EVENT_TYPES.length) {
    request.eventTypes = form.types;
  }
  return { request };
}
