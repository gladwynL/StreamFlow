import { useEffect, useRef, useState, type ReactNode } from 'react';

/**
 * A native modal <dialog>: focus is trapped and restored by the browser, Escape cancels, and the page behind is
 * inert while it is open.
 */
export function Modal({
  open,
  onCancel,
  labelledBy,
  children,
  wide = false,
}: {
  open: boolean;
  onCancel: () => void;
  labelledBy: string;
  children: ReactNode;
  wide?: boolean;
}) {
  const ref = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const dialog = ref.current;
    if (!dialog) {
      return;
    }
    if (open && !dialog.open) {
      dialog.showModal();
    } else if (!open && dialog.open) {
      dialog.close();
    }
  }, [open]);

  return (
    <dialog
      ref={ref}
      aria-labelledby={labelledBy}
      onCancel={(event) => {
        event.preventDefault();
        onCancel();
      }}
      className={`m-auto w-[calc(100%-2rem)] ${wide ? 'max-w-3xl' : 'max-w-md'} rounded-xl border border-line-strong bg-panel p-0 text-ink shadow-2xl shadow-black/60`}
    >
      {open && <div className="max-h-[85vh] overflow-y-auto p-6">{children}</div>}
    </dialog>
  );
}

/**
 * Confirmation for an irreversible or state-changing action. The confirm button stays disabled while the action
 * runs; an error is shown in the dialog.
 */
export function ConfirmDialog({
  open,
  title,
  children,
  confirmLabel,
  tone = 'primary',
  onConfirm,
  onCancel,
  confirmDisabled = false,
}: {
  open: boolean;
  title: string;
  children: ReactNode;
  confirmLabel: string;
  tone?: 'primary' | 'danger';
  onConfirm: () => Promise<void>;
  onCancel: () => void;
  confirmDisabled?: boolean;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const confirm = async () => {
    setBusy(true);
    setError(null);
    try {
      await onConfirm();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const cancel = () => {
    setError(null);
    onCancel();
  };

  return (
    <Modal open={open} onCancel={cancel} labelledBy="confirm-title">
      <div className="space-y-4">
        <h2 id="confirm-title" className="text-base font-semibold text-ink">
          {title}
        </h2>
        <div className="space-y-2 text-sm text-ink-muted">{children}</div>
        {error && (
          <p
            role="alert"
            className="rounded-md border border-bad/40 bg-bad/10 px-3 py-2 text-sm text-bad"
          >
            {error}
          </p>
        )}
        <div className="flex justify-end gap-2">
          <button type="button" className="btn-secondary" onClick={cancel} disabled={busy}>
            Cancel
          </button>
          <button
            type="button"
            className={tone === 'danger' ? 'btn-danger' : 'btn-primary'}
            onClick={() => void confirm()}
            disabled={busy || confirmDisabled}
          >
            {busy ? 'Working…' : confirmLabel}
          </button>
        </div>
      </div>
    </Modal>
  );
}
