import { useCallback } from 'react';
import { ApiError } from '../api/client';
import { useOperator } from './operatorContext';

/** Thrown when the user dismisses the unlock dialog. */
export class OperatorCancelled extends Error {
  constructor() {
    super('Operator key not entered; nothing was changed.');
    this.name = 'OperatorCancelled';
  }
}

/**
 * Runs an operator request with the in-memory key, asking for it first if locked. A 401 forgets the key so the
 * next attempt asks again; server messages (rate limit, disabled, conflicts) are passed through.
 */
export function useOperatorAction() {
  const { requireKey, rejectKey } = useOperator();
  return useCallback(
    async <T>(action: (key: string) => Promise<T>): Promise<T> => {
      const key = await requireKey();
      if (!key) {
        throw new OperatorCancelled();
      }
      try {
        return await action(key);
      } catch (error) {
        if (error instanceof ApiError && error.status === 401) {
          rejectKey();
          throw new ApiError(
            401,
            'The operator key was rejected. Unlock again with the correct key.',
          );
        }
        throw error;
      }
    },
    [requireKey, rejectKey],
  );
}
