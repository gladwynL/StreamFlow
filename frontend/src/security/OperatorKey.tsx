import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
  type ReactNode,
} from 'react';
import { Modal } from '../components/Modal';

/**
 * The operator key lives only in this component's memory: never in a URL, storage, cookie or log. It is dropped
 * after AUTO_LOCK_MS without operator activity, when the tab is closed or reloaded, or when the user locks.
 */
export const AUTO_LOCK_MS = 10 * 60_000;

interface OperatorContextValue {
  unlocked: boolean;
  /** Resolves with the key, asking for it first if locked; null if the user cancels. */
  requireKey: () => Promise<string | null>;
  /** Call when the server rejected the key (401) so the user is asked again. */
  rejectKey: () => void;
  lock: () => void;
}

const OperatorContext = createContext<OperatorContextValue | null>(null);

export function OperatorKeyProvider({
  children,
  autoLockMs = AUTO_LOCK_MS,
}: {
  children: ReactNode;
  autoLockMs?: number;
}) {
  const keyRef = useRef<string | null>(null);
  const [unlocked, setUnlocked] = useState(false);
  const [asking, setAsking] = useState(false);
  const pending = useRef<((key: string | null) => void) | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const lock = useCallback(() => {
    keyRef.current = null;
    clearTimeout(timer.current);
    setUnlocked(false);
  }, []);

  const touch = useCallback(() => {
    clearTimeout(timer.current);
    timer.current = setTimeout(lock, autoLockMs);
  }, [autoLockMs, lock]);

  useEffect(() => () => clearTimeout(timer.current), []);

  const requireKey = useCallback(() => {
    if (keyRef.current) {
      touch();
      return Promise.resolve(keyRef.current);
    }
    return new Promise<string | null>((resolve) => {
      pending.current?.(null);
      pending.current = resolve;
      setAsking(true);
    });
  }, [touch]);

  const finish = useCallback(
    (key: string | null) => {
      setAsking(false);
      if (key) {
        keyRef.current = key;
        setUnlocked(true);
        touch();
      }
      pending.current?.(key);
      pending.current = null;
    },
    [touch],
  );

  const value = useMemo(
    () => ({ unlocked, requireKey, rejectKey: lock, lock }),
    [unlocked, requireKey, lock],
  );

  return (
    <OperatorContext.Provider value={value}>
      {children}
      <UnlockDialog open={asking} onDone={finish} />
    </OperatorContext.Provider>
  );
}

export function useOperator(): OperatorContextValue {
  const value = useContext(OperatorContext);
  if (!value) {
    throw new Error('useOperator must be used inside OperatorKeyProvider');
  }
  return value;
}

function UnlockDialog({ open, onDone }: { open: boolean; onDone: (key: string | null) => void }) {
  const [value, setValue] = useState('');

  const submit = (event: FormEvent) => {
    event.preventDefault();
    const key = value.trim();
    setValue('');
    onDone(key.length > 0 ? key : null);
  };
  const cancel = () => {
    setValue('');
    onDone(null);
  };

  return (
    <Modal open={open} onCancel={cancel} labelledBy="unlock-title">
      <form onSubmit={submit} className="space-y-4">
        <div>
          <h2 id="unlock-title" className="text-base font-semibold text-ink">
            Unlock operator actions
          </h2>
          <p className="mt-1 text-sm text-ink-muted">
            Enter the operator key configured for this api (STREAMFLOW_OPERATOR_KEY). It is kept in
            this tab&apos;s memory only and is forgotten after {Math.round(AUTO_LOCK_MS / 60_000)}{' '}
            minutes without operator activity, or when you lock or reload.
          </p>
        </div>
        <label className="block text-sm">
          <span className="text-ink-muted">Operator key</span>
          <input
            type="password"
            autoComplete="off"
            spellCheck={false}
            required
            value={value}
            onChange={(e) => setValue(e.target.value)}
            className="mt-1 w-full rounded-md border border-line-strong bg-canvas px-3 py-2 font-mono text-sm text-ink"
          />
        </label>
        <div className="flex justify-end gap-2">
          <button type="button" onClick={cancel} className="btn-secondary">
            Cancel
          </button>
          <button type="submit" className="btn-primary">
            Unlock
          </button>
        </div>
      </form>
    </Modal>
  );
}
