'use client';

import {
  createContext,
  useContext,
  useEffect,
  useId,
  useMemo,
  useState,
  type KeyboardEvent,
  type ReactNode,
} from 'react';

export type WorkshopPlatform = 'macos' | 'windows';

const STORAGE_KEY = 'clickhouse-workshop-platform';

type PlatformContextValue = {
  platform: WorkshopPlatform;
  setPlatform: (platform: WorkshopPlatform) => void;
};

const PlatformContext = createContext<PlatformContextValue | null>(null);

function detectPlatform(): WorkshopPlatform {
  const userAgent = navigator.userAgent.toLowerCase();
  const navigatorPlatform = navigator.platform?.toLowerCase() ?? '';
  return userAgent.includes('windows') || navigatorPlatform.startsWith('win')
    ? 'windows'
    : 'macos';
}

export function PlatformProvider({ children }: { children: ReactNode }) {
  const [platform, setPlatformState] = useState<WorkshopPlatform>('macos');

  useEffect(() => {
    const saved = window.localStorage.getItem(STORAGE_KEY);
    const initial = saved === 'macos' || saved === 'windows' ? saved : detectPlatform();
    setPlatformState(initial);
  }, []);

  useEffect(() => {
    document.documentElement.dataset.workshopPlatform = platform;
  }, [platform]);

  const value = useMemo<PlatformContextValue>(
    () => ({
      platform,
      setPlatform(nextPlatform) {
        window.localStorage.setItem(STORAGE_KEY, nextPlatform);
        setPlatformState(nextPlatform);
      },
    }),
    [platform],
  );

  return <PlatformContext.Provider value={value}>{children}</PlatformContext.Provider>;
}

function usePlatform() {
  const context = useContext(PlatformContext);
  if (!context) throw new Error('Platform components require PlatformProvider');
  return context;
}

export function PlatformSelector({ compact = false }: { compact?: boolean }) {
  const { platform, setPlatform } = usePlatform();
  const labelId = useId();

  function moveWithKeyboard(event: KeyboardEvent<HTMLButtonElement>) {
    let next: WorkshopPlatform | null = null;
    if (['ArrowLeft', 'ArrowUp', 'Home'].includes(event.key)) next = 'macos';
    if (['ArrowRight', 'ArrowDown', 'End'].includes(event.key)) next = 'windows';
    if (!next) return;

    event.preventDefault();
    setPlatform(next);
    const option = event.currentTarget.parentElement?.querySelector<HTMLButtonElement>(
      `[data-platform="${next}"]`,
    );
    option?.focus();
  }

  return (
    <div className={`workshop-platform-selector${compact ? ' is-compact' : ''}`}>
      <span className="workshop-platform-label" id={labelId}>
        Your computer
      </span>
      <div
        aria-labelledby={labelId}
        className="workshop-platform-options"
        role="radiogroup"
      >
        <button
          aria-checked={platform === 'macos'}
          className="workshop-platform-option"
          data-platform="macos"
          onClick={() => setPlatform('macos')}
          onKeyDown={moveWithKeyboard}
          role="radio"
          tabIndex={platform === 'macos' ? 0 : -1}
          type="button"
        >
          macOS
        </button>
        <button
          aria-checked={platform === 'windows'}
          className="workshop-platform-option"
          data-platform="windows"
          onClick={() => setPlatform('windows')}
          onKeyDown={moveWithKeyboard}
          role="radio"
          tabIndex={platform === 'windows' ? 0 : -1}
          type="button"
        >
          Windows
        </button>
      </div>
    </div>
  );
}

export function PlatformShellNote() {
  const { platform } = usePlatform();

  return (
    <div className="workshop-shell-note" role="status">
      <strong>{platform === 'windows' ? 'Windows terminal:' : 'macOS terminal:'}</strong>{' '}
      {platform === 'windows'
        ? 'Run workshop commands in Ubuntu (WSL 2), not PowerShell. Use PowerShell only when a block explicitly says so.'
        : 'Run workshop commands in Terminal using zsh or bash.'}
    </div>
  );
}

export function PlatformOnly({
  platform,
  children,
}: {
  platform: WorkshopPlatform;
  children: ReactNode;
}) {
  const selected = usePlatform().platform;
  if (selected !== platform) return null;
  return <>{children}</>;
}
