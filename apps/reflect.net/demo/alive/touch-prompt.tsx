import {useIsomorphicLayoutEffect} from '@/hooks/use-isomorphic-layout-effect';
import classNames from 'classnames';
import {useRef, useState} from 'react';
import type {Rect, Size} from './util';

type Mode = 'off' | 'prompt-rotate' | 'prompt-tap' | 'prompt-desktop';

function getMode(gameMode: boolean, isPortrait: boolean): Mode {
  const isTouch = 'ontouchstart' in window;

  if (gameMode) {
    return 'off';
  }

  if (!isTouch) {
    return 'off';
  }

  if (/Macintosh/.test(navigator.userAgent)) {
    return 'off';
  }

  if (isPortrait) {
    if (canGameMode()) {
      return 'prompt-rotate';
    }
    return 'prompt-desktop';
  }
  if (canGameMode()) {
    return 'prompt-tap';
  }
  return 'prompt-desktop';
}

function canGameMode() {
  // Supposed to be iOS/Safari and embedded WebView only. But not Chrome on iOS.
  return (
    /iP(ad|hone|od)/i.test(navigator.userAgent) &&
    /Safari/.test(navigator.userAgent) &&
    !/CriOS/.test(navigator.userAgent)
  );
}

function getMessage(mode: Mode) {
  switch (mode) {
    case 'prompt-rotate':
      return 'Rotate to Play';
    case 'prompt-tap':
      return 'Tap to Play';
    case 'prompt-desktop':
      return 'Visit on Desktop to Play';
  }
  return undefined;
}

function getIcon(mode: Mode) {
  switch (mode) {
    case 'prompt-rotate':
      return '/icon-rotate-phone.svg';
  }
  return undefined;
}

export function TouchPrompt({
  winSize,
  stage,
  gameMode,
  setGameMode,
}: {
  winSize: Size;
  stage: Rect;
  gameMode: boolean;
  setGameMode: (mode: boolean) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [showTempPrompt, setShowTempPrompt] = useState(false);

  const isPortrait = winSize.width < winSize.height;
  const mode = getMode(gameMode, isPortrait);
  const isTemp = isPortrait;
  const enableScreen = isPortrait;
  const message = getMessage(mode);
  const icon = getIcon(mode);

  useIsomorphicLayoutEffect(() => {
    if (!isPortrait && !gameMode && canGameMode() && showTempPrompt) {
      setGameMode(true);
      setShowTempPrompt(false);
    } else if (isPortrait && gameMode) {
      setGameMode(false);
      setShowTempPrompt(false);
    }
  }, [isPortrait, gameMode, showTempPrompt]);

  useIsomorphicLayoutEffect(() => {
    const handleTouch = (e: Event) => {
      if (mode === 'prompt-tap') {
        if (ref.current?.contains(e.target as Node)) {
          setGameMode(true);
        } else {
          setGameMode(false);
        }
        return;
      }

      if (isTemp) {
        if (!showTempPrompt && ref.current?.contains(e.target as Node)) {
          setShowTempPrompt(true);
        } else {
          setShowTempPrompt(false);
        }
      }
    };

    const event = isTemp ? 'touchstart' : 'click';

    window.addEventListener(event, handleTouch);
    return () => {
      window.removeEventListener(event, handleTouch);
    };
  }, [mode, isTemp, showTempPrompt]);

  if (mode === 'off') {
    return null;
  }

  const active = !isTemp || showTempPrompt;

  return (
    <div
      ref={ref}
      className={classNames('prompt', {
        active,
        enableScreen,
      })}
      style={{
        top: stage.top(),
        left: 0,
        width: '100%',
        height: stage.height,
      }}
    >
      <div className="message">
        {icon && (
          <div className="icon">
            <img src={icon} />
          </div>
        )}
        <div className="wordz">{message}</div>
      </div>
    </div>
  );
}
