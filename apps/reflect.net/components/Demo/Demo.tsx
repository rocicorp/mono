import {hasClient, listClients} from '@/demo/alive/client-model';
import {colorToString, idToColor} from '@/demo/alive/colors';
import {CursorField} from '@/demo/alive/CursorField';
import {
  getClientRoomAssignment,
  ORCHESTRATOR_ROOM,
} from '@/demo/alive/orchestrator-model';
import {listPieces} from '@/demo/alive/piece-model';
import {Puzzle} from '@/demo/alive/Puzzle';
import {TouchPrompt} from '@/demo/alive/touch-prompt';
import {generateRandomPieces, getStage, Rect, Size} from '@/demo/alive/util';
import {loggingOptions} from '@/demo/frontend/logging-options';
import {mutators, type M} from '@/demo/shared/mutators';
import {useElementSize} from '@/hooks/use-element-size';
import {useIsomorphicLayoutEffect} from '@/hooks/use-isomorphic-layout-effect';
import styles from '@/styles/Home.module.css';
import {getLocationString, Location} from '@/util/get-location-string';
import {closeReflect} from '@/util/reflect';
import {getWorkerHost} from '@/util/worker-host';
import {Reflect} from '@rocicorp/reflect/client';
import classNames from 'classnames';
import {event} from 'nextjs-google-analytics';
import {useCallback, useEffect, useState} from 'react';
import ConfettiExplosion from 'react-confetti-explosion';
import {useInView} from 'react-intersection-observer';
import {useSubscribe} from 'replicache-react';

const ORCHESTRATOR_ALIVE_INTERVAL_MS = 10_000;

function usePuzzleRoomID() {
  const [puzzleRoomID, setPuzzleRoomID] = useState<string | null>(null);
  useEffect(() => {
    const orchestratorClient = new Reflect<M>({
      socketOrigin: getWorkerHost(),
      userID: 'anon',
      roomID: ORCHESTRATOR_ROOM,
      mutators,
      ...loggingOptions,
    });

    orchestratorClient.onUpdateNeeded = reason => {
      if (reason.type !== 'NewClientGroup') {
        location.reload();
      }
    };

    orchestratorClient.subscribe(
      tx => getClientRoomAssignment(tx, tx.clientID),
      {
        onData: result => {
          setPuzzleRoomID(prev => {
            const newVal = result?.roomID ?? null;
            if (prev !== newVal) {
              console.info('NEW ROOM ID', newVal);
            }
            return newVal;
          });
        },
      },
    );
    const aliveIfVisible = () => {
      if (document.visibilityState === 'visible') {
        void orchestratorClient.mutate.alive();
      }
    };
    aliveIfVisible();
    const aliveInterval = setInterval(
      aliveIfVisible,
      ORCHESTRATOR_ALIVE_INTERVAL_MS,
    );
    const visibilityChangeListener = () => {
      aliveIfVisible();
    };
    document.addEventListener('visibilitychange', visibilityChangeListener);
    const pageHideListener = () => {
      void orchestratorClient.mutate.unload();
    };
    window.addEventListener('pagehide', pageHideListener);

    return () => {
      clearInterval(aliveInterval);
      document.removeEventListener(
        'visibilitychange',
        visibilityChangeListener,
      );
      window.removeEventListener('pagehide', pageHideListener);
      closeReflect(orchestratorClient);
    };
    // Run once.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return puzzleRoomID;
}

function useReflect(
  puzzleRoomID: string | null,
  stage: Rect | null,
  home: Rect | null,
) {
  const [r, setR] = useState<Reflect<M> | null>(null);
  const [online, setOnline] = useState<boolean>(true);

  // Runs once, when dimensions are available.
  // We only want to initialize reflect once, even if the dimensions change. The pieces are placed relatively,
  // we only need the current dimensions so that we pick locations in which the pieces are spread out on this
  // screen.
  useEffect(() => {
    if (!home || !puzzleRoomID || !stage) {
      return;
    }

    const reflect = new Reflect<M>({
      socketOrigin: getWorkerHost(),
      userID: 'anon',
      roomID: puzzleRoomID,
      mutators,
      ...loggingOptions,
    });

    reflect.onUpdateNeeded = reason => {
      if (reason.type !== 'NewClientGroup') {
        location.reload();
      }
    };

    const url = new URL(location.href);
    if (url.searchParams.has('reset')) {
      console.info('Resetting replicache');
      void reflect.mutate.resetRoom();
    }
    if (url.searchParams.has('solve')) {
      console.info('Solving puzzle');
      void reflect.mutate.solve();
    }

    void reflect.mutate.initializePuzzle({
      pieces: generateRandomPieces(home, stage),
      force: false,
    });

    reflect.onOnlineChange = online => {
      setOnline(online);
    };

    const onBlur = async () => {
      void reflect.mutate.updateClient({
        id: await reflect.clientID,
        focused: false,
      });
    };
    const onFocus = async () => {
      void reflect.mutate.updateClient({
        id: await reflect.clientID,
        focused: true,
      });
    };
    window.addEventListener('blur', onBlur);
    window.addEventListener('focus', onFocus);

    setR(reflect);
    return () => {
      setR(null);
      window.removeEventListener('blur', onBlur);
      window.removeEventListener('focus', onFocus);
      closeReflect(reflect);
    };
    // we only want to do this once per page-load.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [home !== null, stage !== null, puzzleRoomID]);

  return {r, online};
}

function useEnsureMyClient(
  r: Reflect<M> | null,
  tabIsVisible: boolean,
): string | undefined {
  const cid = useSubscribe(
    r,
    async tx => {
      const cid = tx.clientID;
      if (await hasClient(tx, cid)) {
        return cid;
      }
      return undefined;
    },
    undefined,
  );

  if (cid !== undefined) {
    return cid;
  }

  // Runs on every render :(
  // Ideally we could do this only when we come back online, but the online
  // event in Reflect is currently broken and doesn't fire. When we fix the
  // online event, there is an interesting subtlety: we have decided that
  // we will wait for two errors in a row to fire the online change, but the
  // disconnect on server happens sooner. We have to be sure that it is not
  // possible for the client to observe a situation where the disconnect
  // handler has run server-side and had some effect that the client sees,
  // before the online change event happens. Otherwise you cannot use this
  // nice pattern of creating client-specific state in the online change
  // handler and deleting in the server-side disconnect handler.
  if (r === null) {
    return undefined;
  }

  // Do not re-create the client if tab not visible.
  if (!tabIsVisible) {
    return undefined;
  }

  const ensure = async () => {
    const cid = await r.clientID;
    await r.mutate.ensureClient({
      id: cid,
      selectedPieceID: '',
      // off the page, so not visible till user moves cursor
      // avoids cursors stacking up at 0,0
      x: Number.MIN_SAFE_INTEGER,
      y: 0,
      color: colorToString(idToColor(cid)),
      location: null,
      focused: document.hasFocus(),
      botControllerID: '',
      manuallyTriggeredBot: false,
    });
  };

  void ensure();

  return undefined;
}

function useEnsureLocation(
  r: Reflect<M> | null,
  myClientID: string | undefined,
) {
  const [location, setLocation] = useState<Location | null>(null);
  const ignore = false;

  useEffect(() => {
    void fetch('/api/get-location')
      .then(resp => resp.json())
      .then(data => {
        if (ignore) {
          return;
        }
        setLocation(data);
      });
  }, [ignore]);

  useEffect(() => {
    if (r === null || location === null || myClientID === undefined) {
      return;
    }
    void r.mutate.updateClient({
      id: myClientID,
      location: getLocationString(location),
    });
  }, [location, r, myClientID]);
}

function useClientIDs(r: Reflect<M> | null) {
  const clientIDs = useSubscribe(
    r,
    async tx => {
      const clients = await listClients(tx);
      const ids = [];
      for (const client of clients) {
        if ((client.id === tx.clientID, client.focused)) {
          ids.push(client.id);
        }
      }
      return ids;
    },
    [],
  );
  return clientIDs;
}

function useTabIsVisible() {
  const [tabIsVisible, setTabIsVisible] = useState(false);
  useIsomorphicLayoutEffect(() => {
    const onVisibilityChange = () => {
      setTabIsVisible(document.visibilityState === 'visible');
    };
    onVisibilityChange();
    document.addEventListener('visibilitychange', onVisibilityChange);
    return () => {
      document.removeEventListener('visibilitychange', onVisibilityChange);
    };
  }, []);
  return tabIsVisible;
}

function useBodyClasses() {
  const [classes, setClasses] = useState(new Map<string, boolean>());
  useIsomorphicLayoutEffect(() => {
    for (const [cls, enabled] of classes.entries()) {
      document.body.classList.toggle(cls, enabled);
    }
    return () => {
      for (const [cls] of classes.entries()) {
        document.body.classList.remove(cls);
      }
    };
  }, [classes]);

  const setClass = useCallback(
    (cls: string, enabled: boolean) => {
      setClasses(old => {
        const next = new Map(old);
        next.set(cls, enabled);
        return next;
      });
    },
    [setClasses],
  );

  return [classes, setClass] as const;
}

export function Demo({
  winSize,
  docSize,
  gameMode,
  onSetGameMode,
}: {
  winSize: Size | null;
  docSize: Size | null;
  gameMode: boolean;
  onSetGameMode: (gameMode: boolean) => void;
}) {
  const tabIsVisible = useTabIsVisible();
  const [homeRef, home] = useElementSize<SVGSVGElement>([
    winSize,
    docSize,
    gameMode,
  ]);
  const stage = getStage(home);
  const puzzleRoomID = usePuzzleRoomID();
  const {r, online} = useReflect(puzzleRoomID, stage, home);
  const myClientID = useEnsureMyClient(r, tabIsVisible);
  useEnsureLocation(r, myClientID);
  const clientIDs = useClientIDs(r);
  const [demoInView, setDemoInView] = useState(false);
  const {ref} = useInView({
    onChange: inView => setDemoInView(inView),
  });
  const [bodyClasses, setBodyClass] = useBodyClasses();

  const isPuzzleComplete = useSubscribe<boolean>(
    r,
    async tx => {
      const pieces = await listPieces(tx);
      return pieces.length > 0 && pieces.findIndex(p => !p.placed) === -1;
    },
    false,
  );

  const resetGame = useCallback(() => {
    event('alive_solve_puzzle', {
      category: 'Alive Demo',
      action: 'Place final piece',
      label: 'Demo',
    });
    if (!r || !home || !stage) {
      return;
    }
    void r.mutate.initializePuzzle({
      pieces: generateRandomPieces(home, stage),
      force: true,
    });
  }, [r, home, stage]);

  return (
    <section id="intro" className={classNames(styles.section, {gameMode})}>
      <div id="title-container">
        <h1 className="title">The next web is</h1>
      </div>
      <div id="demo">
        <div
          ref={ref}
          id="confetti-container"
          className={classNames({active: isPuzzleComplete})}
        >
          {isPuzzleComplete && demoInView && (
            <ConfettiExplosion
              force={0.7}
              particleCount={100}
              duration={3500}
              colors={['#fc49ab', '#5fe8ff', '#ff9900', '#d505e8', '#1d9de5']}
            />
          )}
          {isPuzzleComplete && demoInView && (
            <ConfettiExplosion
              force={0.9}
              particleCount={100}
              duration={3000}
              colors={['#fc49ab', '#5fe8ff', '#ff9900', '#d505e8', '#1d9de5']}
              onComplete={resetGame}
            />
          )}
        </div>
        <svg
          ref={homeRef}
          id="wells"
          viewBox="0 0 568 198"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
        >
          <mask
            id="path-1-outside-1_425_487"
            maskUnits="userSpaceOnUse"
            x="0"
            y="0"
            width="568"
            height="198"
            fill="black"
          >
            <rect fill="white" width="568" height="198" />
            <path
              fillRule="evenodd"
              clipRule="evenodd"
              d="M220.12 59.88V193H263.64V59.88H220.12ZM223.192 7.912C218.755 12.52 216.536 18.2373 216.536 25.064C216.536 31.5493 218.755 37.1813 223.192 41.96C227.8 46.7387 234.029 49.128 241.88 49.128C249.731 49.128 255.875 46.7387 260.312 41.96C264.92 37.1813 267.224 31.5493 267.224 25.064C267.224 18.2373 264.92 12.52 260.312 7.912C255.875 3.304 249.731 1 241.88 1C234.029 1 227.8 3.304 223.192 7.912ZM502.102 196.072C488.619 196.072 476.673 193.341 466.262 187.88C455.851 182.248 447.745 174.227 441.942 163.816C436.139 153.405 433.238 140.947 433.238 126.44C433.238 114.152 435.969 102.717 441.43 92.136C447.062 81.5546 454.913 73.0213 464.982 66.536C475.222 60.0506 487.254 56.808 501.078 56.808C515.243 56.808 527.19 59.88 536.918 66.024C546.646 71.9973 553.985 80.4453 558.934 91.368C564.054 102.291 566.614 115.091 566.614 129.768V136.936H477.431C477.941 140.528 478.741 143.685 479.83 146.408C482.049 151.357 485.035 155.027 488.79 157.416C492.545 159.805 496.982 161 502.102 161C507.051 161 511.147 159.891 514.39 157.672C517.803 155.283 520.619 152.381 522.838 148.968L561.75 163.048C557.313 172.947 550.401 180.968 541.014 187.112C531.627 193.085 518.657 196.072 502.102 196.072ZM479.318 106.728C478.629 108.795 478.079 111.099 477.668 113.64H522.159C521.783 110.714 521.156 107.984 520.278 105.448C518.913 101.011 516.609 97.5973 513.366 95.208C510.294 92.648 506.198 91.368 501.078 91.368C495.787 91.368 491.265 92.648 487.51 95.208C483.926 97.5973 481.195 101.437 479.318 106.728ZM327.954 193L278.034 59.88H327.442L350.994 139.496H353.554L378.642 59.88H426.514L376.594 193H327.954ZM147.87 193V8.67999H191.39V193H147.87ZM12.776 185.832C20.6267 192.659 30.5253 196.072 42.472 196.072C50.4933 196.072 57.576 194.451 63.72 191.208C69.864 187.795 74.728 182.675 78.312 175.848H80.36L81.384 193H121.832L120.296 162.28V109.8C120.296 93.2453 115.261 80.2746 105.192 70.888C95.2933 61.5013 81.0427 56.808 62.44 56.808C50.4933 56.808 40.168 58.9413 31.464 63.208C22.9307 67.304 16.2747 73.0213 11.496 80.36C6.71733 87.6986 4.15733 95.976 3.816 105.192H45.544C45.8853 100.755 47.4213 97.256 50.152 94.696C53.0533 91.9653 56.9787 90.6 61.928 90.6C66.3653 90.6 70.0347 92.0507 72.936 94.952C75.8373 97.6826 77.288 101.523 77.288 106.472V113.217L46.312 115.944C32.3173 117.139 21.224 121.235 13.032 128.232C5.01067 135.059 1 144.36 1 156.136C1 168.936 4.92533 178.835 12.776 185.832ZM59.112 138.472L77.288 136.304V139.752C77.288 145.043 76.52 149.565 74.984 153.32C73.448 156.904 71.0587 159.72 67.816 161.768C64.744 163.645 60.904 164.584 56.296 164.584C51.8587 164.584 48.4453 163.389 46.056 161C43.8373 158.44 42.728 155.368 42.728 151.784C42.728 148.2 44.008 145.299 46.568 143.08C49.128 140.691 53.3093 139.155 59.112 138.472Z"
            />
          </mask>
          <path
            fillRule="evenodd"
            clipRule="evenodd"
            d="M220.12 59.88V193H263.64V59.88H220.12ZM223.192 7.912C218.755 12.52 216.536 18.2373 216.536 25.064C216.536 31.5493 218.755 37.1813 223.192 41.96C227.8 46.7387 234.029 49.128 241.88 49.128C249.731 49.128 255.875 46.7387 260.312 41.96C264.92 37.1813 267.224 31.5493 267.224 25.064C267.224 18.2373 264.92 12.52 260.312 7.912C255.875 3.304 249.731 1 241.88 1C234.029 1 227.8 3.304 223.192 7.912ZM502.102 196.072C488.619 196.072 476.673 193.341 466.262 187.88C455.851 182.248 447.745 174.227 441.942 163.816C436.139 153.405 433.238 140.947 433.238 126.44C433.238 114.152 435.969 102.717 441.43 92.136C447.062 81.5546 454.913 73.0213 464.982 66.536C475.222 60.0506 487.254 56.808 501.078 56.808C515.243 56.808 527.19 59.88 536.918 66.024C546.646 71.9973 553.985 80.4453 558.934 91.368C564.054 102.291 566.614 115.091 566.614 129.768V136.936H477.431C477.941 140.528 478.741 143.685 479.83 146.408C482.049 151.357 485.035 155.027 488.79 157.416C492.545 159.805 496.982 161 502.102 161C507.051 161 511.147 159.891 514.39 157.672C517.803 155.283 520.619 152.381 522.838 148.968L561.75 163.048C557.313 172.947 550.401 180.968 541.014 187.112C531.627 193.085 518.657 196.072 502.102 196.072ZM479.318 106.728C478.629 108.795 478.079 111.099 477.668 113.64H522.159C521.783 110.714 521.156 107.984 520.278 105.448C518.913 101.011 516.609 97.5973 513.366 95.208C510.294 92.648 506.198 91.368 501.078 91.368C495.787 91.368 491.265 92.648 487.51 95.208C483.926 97.5973 481.195 101.437 479.318 106.728ZM327.954 193L278.034 59.88H327.442L350.994 139.496H353.554L378.642 59.88H426.514L376.594 193H327.954ZM147.87 193V8.67999H191.39V193H147.87ZM12.776 185.832C20.6267 192.659 30.5253 196.072 42.472 196.072C50.4933 196.072 57.576 194.451 63.72 191.208C69.864 187.795 74.728 182.675 78.312 175.848H80.36L81.384 193H121.832L120.296 162.28V109.8C120.296 93.2453 115.261 80.2746 105.192 70.888C95.2933 61.5013 81.0427 56.808 62.44 56.808C50.4933 56.808 40.168 58.9413 31.464 63.208C22.9307 67.304 16.2747 73.0213 11.496 80.36C6.71733 87.6986 4.15733 95.976 3.816 105.192H45.544C45.8853 100.755 47.4213 97.256 50.152 94.696C53.0533 91.9653 56.9787 90.6 61.928 90.6C66.3653 90.6 70.0347 92.0507 72.936 94.952C75.8373 97.6826 77.288 101.523 77.288 106.472V113.217L46.312 115.944C32.3173 117.139 21.224 121.235 13.032 128.232C5.01067 135.059 1 144.36 1 156.136C1 168.936 4.92533 178.835 12.776 185.832ZM59.112 138.472L77.288 136.304V139.752C77.288 145.043 76.52 149.565 74.984 153.32C73.448 156.904 71.0587 159.72 67.816 161.768C64.744 163.645 60.904 164.584 56.296 164.584C51.8587 164.584 48.4453 163.389 46.056 161C43.8373 158.44 42.728 155.368 42.728 151.784C42.728 148.2 44.008 145.299 46.568 143.08C49.128 140.691 53.3093 139.155 59.112 138.472Z"
            fill="black"
            fillOpacity="0.04"
          />
          <path
            d="M220.12 193H219.12V194H220.12V193ZM220.12 59.88V58.88H219.12V59.88H220.12ZM263.64 193V194H264.64V193H263.64ZM263.64 59.88H264.64V58.88H263.64V59.88ZM223.192 7.912L222.485 7.2049L222.478 7.21157L222.472 7.21836L223.192 7.912ZM223.192 41.96L222.459 42.6405L222.466 42.6474L222.472 42.6541L223.192 41.96ZM260.312 41.96L259.592 41.2659L259.586 41.2727L259.579 41.2796L260.312 41.96ZM260.312 7.912L259.592 8.60564L259.598 8.61244L259.605 8.61911L260.312 7.912ZM466.262 187.88L465.786 188.76L465.797 188.766L466.262 187.88ZM441.43 92.136L440.547 91.6661L440.541 91.6773L441.43 92.136ZM464.982 66.536L464.447 65.6911L464.441 65.6953L464.982 66.536ZM536.918 66.024L536.384 66.8695L536.395 66.8761L536.918 66.024ZM558.934 91.368L558.023 91.7807L558.029 91.7924L558.934 91.368ZM566.614 136.936V137.936H567.614V136.936H566.614ZM477.431 136.936V135.936H476.279L476.441 137.077L477.431 136.936ZM479.83 146.408L478.902 146.779L478.909 146.798L478.918 146.817L479.83 146.408ZM488.79 157.416L489.327 156.572L489.327 156.572L488.79 157.416ZM514.39 157.672L514.955 158.497L514.963 158.491L514.39 157.672ZM522.838 148.968L523.178 148.028L522.432 147.758L522 148.423L522.838 148.968ZM561.75 163.048L562.663 163.457L563.103 162.474L562.09 162.108L561.75 163.048ZM541.014 187.112L541.551 187.956L541.562 187.949L541.014 187.112ZM477.668 113.64L476.681 113.48L476.493 114.64H477.668V113.64ZM479.318 106.728L478.376 106.394L478.372 106.403L478.369 106.412L479.318 106.728ZM522.159 113.64V114.64H523.296L523.151 113.512L522.159 113.64ZM520.278 105.448L519.322 105.742L519.327 105.759L519.333 105.775L520.278 105.448ZM513.366 95.208L512.726 95.9762L512.749 95.9953L512.773 96.013L513.366 95.208ZM487.51 95.208L488.065 96.0401L488.073 96.0342L487.51 95.208ZM278.034 59.88V58.88H276.591L277.098 60.2311L278.034 59.88ZM327.954 193L327.018 193.351L327.261 194H327.954V193ZM327.442 59.88L328.401 59.5963L328.189 58.88H327.442V59.88ZM350.994 139.496L350.035 139.78L350.247 140.496H350.994V139.496ZM353.554 139.496V140.496H354.287L354.508 139.797L353.554 139.496ZM378.642 59.88V58.88H377.909L377.688 59.5795L378.642 59.88ZM426.514 59.88L427.45 60.2311L427.957 58.88H426.514V59.88ZM376.594 193V194H377.287L377.53 193.351L376.594 193ZM147.87 8.67999V7.67999H146.87V8.67999H147.87ZM147.87 193H146.87V194H147.87V193ZM191.39 8.67999H192.39V7.67999H191.39V8.67999ZM191.39 193V194H192.39V193H191.39ZM12.776 185.832L12.1106 186.579L12.1198 186.587L12.776 185.832ZM63.72 191.208L64.1868 192.092L64.1963 192.087L64.2056 192.082L63.72 191.208ZM78.312 175.848V174.848H77.7076L77.4266 175.383L78.312 175.848ZM80.36 175.848L81.3582 175.788L81.3021 174.848H80.36V175.848ZM81.384 193L80.3858 193.06L80.4419 194H81.384V193ZM121.832 193V194H122.883L122.831 192.95L121.832 193ZM120.296 162.28H119.296V162.305L119.297 162.33L120.296 162.28ZM105.192 70.888L104.504 71.6136L104.51 71.6194L105.192 70.888ZM31.464 63.208L31.8967 64.1095L31.9042 64.1059L31.464 63.208ZM11.496 80.36L10.658 79.8143L10.658 79.8143L11.496 80.36ZM3.816 105.192L2.81669 105.155L2.77828 106.192H3.816V105.192ZM45.544 105.192V106.192H46.47L46.5411 105.269L45.544 105.192ZM50.152 94.696L50.8359 95.4255L50.8374 95.4242L50.152 94.696ZM72.936 94.952L72.2289 95.6591L72.2396 95.6698L72.2506 95.6802L72.936 94.952ZM77.288 113.217L77.3757 114.213L78.288 114.132V113.217H77.288ZM46.312 115.944L46.3971 116.94L46.3997 116.94L46.312 115.944ZM13.032 128.232L13.6801 128.994L13.6815 128.992L13.032 128.232ZM77.288 136.304H78.288V135.178L77.1696 135.311L77.288 136.304ZM59.112 138.472L59.2288 139.465L59.2304 139.465L59.112 138.472ZM74.984 153.32L75.9031 153.714L75.9064 153.706L75.9095 153.699L74.984 153.32ZM67.816 161.768L68.3375 162.621L68.3438 162.617L68.35 162.613L67.816 161.768ZM46.056 161L45.3003 161.655L45.3237 161.682L45.3489 161.707L46.056 161ZM46.568 143.08L47.2229 143.836L47.2369 143.824L47.2503 143.811L46.568 143.08ZM221.12 193V59.88H219.12V193H221.12ZM263.64 192H220.12V194H263.64V192ZM262.64 59.88V193H264.64V59.88H262.64ZM220.12 60.88H263.64V58.88H220.12V60.88ZM217.536 25.064C217.536 18.4769 219.667 13.014 223.912 8.60564L222.472 7.21836C217.842 12.026 215.536 17.9978 215.536 25.064H217.536ZM223.925 41.2796C219.658 36.6847 217.536 31.2954 217.536 25.064H215.536C215.536 31.8033 217.851 37.678 222.459 42.6405L223.925 41.2796ZM241.88 48.128C234.247 48.128 228.297 45.8136 223.912 41.2659L222.472 42.6541C227.303 47.6638 233.812 50.128 241.88 50.128V48.128ZM259.579 41.2796C255.369 45.8135 249.514 48.128 241.88 48.128V50.128C249.948 50.128 256.38 47.6639 261.045 42.6405L259.579 41.2796ZM266.224 25.064C266.224 31.2837 264.025 36.6687 259.592 41.2659L261.032 42.6541C265.815 37.694 268.224 31.815 268.224 25.064H266.224ZM259.605 8.61911C264.016 13.0299 266.224 18.4885 266.224 25.064H268.224C268.224 17.9862 265.824 12.0101 261.019 7.2049L259.605 8.61911ZM241.88 2C249.525 2 255.384 4.23633 259.592 8.60564L261.032 7.21836C256.365 2.37167 249.937 0 241.88 0V2ZM223.899 8.61911C228.282 4.23604 234.237 2 241.88 2V0C233.822 0 227.318 2.37196 222.485 7.2049L223.899 8.61911ZM465.797 188.766C476.371 194.312 488.481 197.072 502.102 197.072V195.072C488.757 195.072 476.974 192.37 466.727 186.994L465.797 188.766ZM441.069 164.303C446.963 174.878 455.207 183.036 465.786 188.76L466.738 187C456.496 181.46 448.526 173.575 442.815 163.329L441.069 164.303ZM432.238 126.44C432.238 141.08 435.167 153.714 441.069 164.303L442.815 163.329C437.112 153.096 434.238 140.813 434.238 126.44H432.238ZM440.541 91.6773C435.004 102.405 432.238 113.998 432.238 126.44H434.238C434.238 114.306 436.933 103.03 442.319 92.5946L440.541 91.6773ZM464.441 65.6953C454.228 72.2731 446.259 80.9342 440.547 91.6661L442.313 92.6058C447.865 82.1751 455.598 73.7695 465.524 67.3767L464.441 65.6953ZM501.078 55.808C487.092 55.808 474.869 59.0908 464.447 65.6912L465.517 67.3808C475.575 61.0105 487.416 57.808 501.078 57.808V55.808ZM537.452 65.1785C527.531 58.9125 515.389 55.808 501.078 55.808V57.808C515.098 57.808 526.849 60.8475 536.384 66.8695L537.452 65.1785ZM559.845 90.9553C554.818 79.8604 547.348 71.2551 537.441 65.1718L536.395 66.8761C545.944 72.7396 553.152 81.0302 558.023 91.7807L559.845 90.9553ZM567.614 129.768C567.614 114.975 565.034 102.025 559.84 90.9435L558.029 91.7924C563.074 102.557 565.614 115.206 565.614 129.768H567.614ZM567.614 136.936V129.768H565.614V136.936H567.614ZM477.431 137.936H566.614V135.936H477.431V137.936ZM480.758 146.037C479.705 143.403 478.923 140.326 478.421 136.795L476.441 137.077C476.96 140.73 477.777 143.967 478.902 146.779L480.758 146.037ZM489.327 156.572C485.77 154.309 482.899 150.809 480.743 145.999L478.918 146.817C481.199 151.905 484.3 155.744 488.253 158.26L489.327 156.572ZM502.102 160C497.143 160 492.898 158.845 489.327 156.572L488.253 158.26C492.191 160.766 496.821 162 502.102 162V160ZM513.825 156.847C510.788 158.925 506.902 160 502.102 160V162C507.2 162 511.507 160.856 514.955 158.497L513.825 156.847ZM522 148.423C519.853 151.726 517.127 154.535 513.817 156.853L514.963 158.491C518.479 156.03 521.386 153.036 523.676 149.513L522 148.423ZM562.09 162.108L523.178 148.028L522.498 149.908L561.41 163.988L562.09 162.108ZM541.562 187.949C551.103 181.704 558.144 173.536 562.663 163.457L560.837 162.639C556.481 172.357 549.699 180.232 540.466 186.275L541.562 187.949ZM502.102 197.072C518.76 197.072 531.943 194.069 541.551 187.956L540.477 186.268C531.311 192.101 518.553 195.072 502.102 195.072V197.072ZM478.655 113.8C479.059 111.303 479.597 109.052 480.267 107.044L478.369 106.412C477.66 108.538 477.099 110.896 476.681 113.48L478.655 113.8ZM522.159 112.64H477.668V114.64H522.159V112.64ZM519.333 105.775C520.186 108.24 520.799 110.903 521.167 113.768L523.151 113.512C522.767 110.525 522.125 107.727 521.223 105.121L519.333 105.775ZM512.773 96.013C515.818 98.2565 518.01 101.477 519.322 105.742L521.234 105.154C519.815 100.544 517.4 96.9381 513.959 94.4029L512.773 96.013ZM501.078 92.368C506.031 92.368 509.879 93.6039 512.726 95.9762L514.006 94.4398C510.709 91.6921 506.365 90.368 501.078 90.368V92.368ZM488.073 96.0342C491.636 93.6051 495.954 92.368 501.078 92.368V90.368C495.62 90.368 490.893 91.6908 486.947 94.3817L488.073 96.0342ZM480.26 107.062C482.088 101.913 484.708 98.2778 488.065 96.04L486.955 94.3759C483.144 96.9168 480.303 100.961 478.376 106.394L480.26 107.062ZM277.098 60.2311L327.018 193.351L328.89 192.649L278.97 59.5289L277.098 60.2311ZM327.442 58.88H278.034V60.88H327.442V58.88ZM351.953 139.212L328.401 59.5963L326.483 60.1637L350.035 139.78L351.953 139.212ZM353.554 138.496H350.994V140.496H353.554V138.496ZM377.688 59.5795L352.6 139.195L354.508 139.797L379.596 60.1805L377.688 59.5795ZM426.514 58.88H378.642V60.88H426.514V58.88ZM377.53 193.351L427.45 60.2311L425.578 59.5289L375.658 192.649L377.53 193.351ZM327.954 194H376.594V192H327.954V194ZM146.87 8.67999V193H148.87V8.67999H146.87ZM191.39 7.67999H147.87V9.67999H191.39V7.67999ZM192.39 193V8.67999H190.39V193H192.39ZM147.87 194H191.39V192H147.87V194ZM42.472 195.072C30.7285 195.072 21.0741 191.723 13.4322 185.077L12.1198 186.587C20.1792 193.595 30.3222 197.072 42.472 197.072V195.072ZM63.2532 190.324C57.2739 193.479 50.3559 195.072 42.472 195.072V197.072C50.6307 197.072 57.8781 195.422 64.1868 192.092L63.2532 190.324ZM77.4266 175.383C73.924 182.055 69.1917 187.024 63.2344 190.334L64.2056 192.082C70.5363 188.565 75.532 183.295 79.1974 176.313L77.4266 175.383ZM80.36 174.848H78.312V176.848H80.36V174.848ZM82.3822 192.94L81.3582 175.788L79.3618 175.908L80.3858 193.06L82.3822 192.94ZM121.832 192H81.384V194H121.832V192ZM119.297 162.33L120.833 193.05L122.831 192.95L121.295 162.23L119.297 162.33ZM119.296 109.8V162.28H121.296V109.8H119.296ZM104.51 71.6194C114.336 80.7792 119.296 93.4619 119.296 109.8H121.296C121.296 93.0287 116.187 79.7701 105.874 70.1565L104.51 71.6194ZM62.44 57.808C80.8813 57.808 94.8499 62.4589 104.504 71.6136L105.88 70.1624C95.7368 60.5437 81.204 55.808 62.44 55.808V57.808ZM31.9042 64.1059C40.443 59.9202 50.6112 57.808 62.44 57.808V55.808C50.3755 55.808 39.893 57.9624 31.0238 62.3101L31.9042 64.1059ZM12.334 80.9056C17.0047 73.7329 23.5147 68.1329 31.8967 64.1095L31.0313 62.3065C22.3467 66.4751 15.5447 72.3098 10.658 79.8143L12.334 80.9056ZM4.81531 105.229C5.15031 96.184 7.65952 88.0843 12.334 80.9056L10.658 79.8143C5.77515 87.313 3.16435 95.768 2.81669 105.155L4.81531 105.229ZM45.544 104.192H3.816V106.192H45.544V104.192ZM49.4681 93.9664C46.5217 96.7287 44.9036 100.479 44.5469 105.115L46.5411 105.269C46.8671 101.031 48.321 97.7833 50.8359 95.4255L49.4681 93.9664ZM61.928 89.6C56.7886 89.6 52.5958 91.0227 49.4666 93.9678L50.8374 95.4242C53.5109 92.9079 57.1687 91.6 61.928 91.6V89.6ZM73.6431 94.2449C70.538 91.1398 66.6067 89.6 61.928 89.6V91.6C66.124 91.6 69.5313 92.9615 72.2289 95.6591L73.6431 94.2449ZM78.288 106.472C78.288 101.318 76.7701 97.1873 73.6214 94.2238L72.2506 95.6802C74.9045 98.178 76.288 101.727 76.288 106.472H78.288ZM78.288 113.217V106.472H76.288V113.217H78.288ZM46.3997 116.94L77.3757 114.213L77.2003 112.22L46.2243 114.948L46.3997 116.94ZM13.6815 128.992C21.6725 122.167 32.5476 118.123 46.3971 116.94L46.2269 114.948C32.087 116.155 20.7755 120.303 12.3825 127.472L13.6815 128.992ZM2 156.136C2 144.614 5.91002 135.606 13.6801 128.994L12.3839 127.47C4.11132 134.511 0 144.106 0 156.136H2ZM13.4414 185.085C5.84908 178.318 2 168.718 2 156.136H0C0 169.154 4.00158 179.351 12.1106 186.578L13.4414 185.085ZM77.1696 135.311L58.9936 137.479L59.2304 139.465L77.4064 137.297L77.1696 135.311ZM78.288 139.752V136.304H76.288V139.752H78.288ZM75.9095 153.699C77.5072 149.793 78.288 145.135 78.288 139.752H76.288C76.288 144.95 75.5328 149.337 74.0585 152.941L75.9095 153.699ZM68.35 162.613C71.7668 160.455 74.2888 157.481 75.9031 153.714L74.0649 152.926C72.6072 156.327 70.3505 158.984 67.282 160.923L68.35 162.613ZM56.296 165.584C61.0373 165.584 65.0701 164.618 68.3375 162.621L67.2946 160.915C64.4179 162.673 60.7708 163.584 56.296 163.584V165.584ZM45.3489 161.707C47.979 164.337 51.6797 165.584 56.296 165.584V163.584C52.0376 163.584 48.9116 162.441 46.7631 160.293L45.3489 161.707ZM41.728 151.784C41.728 155.593 42.9144 158.902 45.3003 161.655L46.8117 160.345C44.7603 157.978 43.728 155.143 43.728 151.784H41.728ZM45.9131 142.324C43.1199 144.745 41.728 147.929 41.728 151.784H43.728C43.728 148.471 44.8961 145.852 47.2229 143.836L45.9131 142.324ZM58.9952 137.479C53.1093 138.171 48.6749 139.746 45.8857 142.349L47.2503 143.811C49.5811 141.636 53.5094 140.138 59.2288 139.465L58.9952 137.479Z"
            fill="black"
            fillOpacity="0.5"
            mask="url(#path-1-outside-1_425_487)"
          />
        </svg>
      </div>
      <div id="info">
        <div className={classNames('active-user-info', {offline: !online})}>
          <div className="online-dot"></div>
          Active Users:&nbsp;
          <span id="active-user-count">
            {clientIDs.length > 0 ? clientIDs.length : 1}
          </span>
        </div>
      </div>
      <p className="featuredStatement">
        High-performance sync for multiplayer web apps
      </p>
      <img
        id="back-button"
        src="/icon-prompt-back.svg"
        onClick={() => onSetGameMode(false)}
      />
      {r && home && stage && winSize && online && (
        <Puzzle r={r} home={home} stage={stage} setBodyClass={setBodyClass} />
      )}
      {r && home && stage && docSize && myClientID && online && (
        <CursorField
          home={home}
          stage={stage}
          docSize={docSize}
          r={r}
          myClientID={myClientID}
          clientIDs={clientIDs}
          hideLocalArrow={
            bodyClasses.get('grabbing') === true ||
            bodyClasses.get('grab') === true
          }
          setBodyClass={setBodyClass}
        />
      )}
      {stage && winSize && online && (
        <TouchPrompt
          winSize={winSize}
          stage={stage}
          gameMode={gameMode}
          setGameMode={onSetGameMode}
        />
      )}
    </section>
  );
}
