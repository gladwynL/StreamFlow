import { createContext, useContext } from 'react';

export interface OperatorContextValue {
  unlocked: boolean;
  /** Resolves with the key, asking for it first if locked; null if the user cancels. */
  requireKey: () => Promise<string | null>;
  /** Call when the server rejected the key (401) so the user is asked again. */
  rejectKey: () => void;
  lock: () => void;
}

export const OperatorContext = createContext<OperatorContextValue | null>(null);

/** The in-memory operator key (see OperatorKeyProvider). */
export function useOperator(): OperatorContextValue {
  const value = useContext(OperatorContext);
  if (!value) {
    throw new Error('useOperator must be used inside OperatorKeyProvider');
  }
  return value;
}
