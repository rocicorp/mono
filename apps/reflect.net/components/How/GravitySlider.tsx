import {useEffect, useState} from 'react';
import {Range, getTrackBackground} from 'react-range';
import style from './Slider.module.css';

const STEP = 1;
const MIN = -360;
const MAX = 360;
const DRIFT_RATE = 36; // adjust this to change the rate of drift
const INTERVAL_MS = 8;

export function GravitySlider({
  increment,
  degree,
}: {
  increment: (delta: number) => void;
  degree: number | null;
}) {
  const [value, setValue] = useState(0);
  const [drifting, setDrifting] = useState(false);
  const [touched, setTouched] = useState(false);

  useEffect(() => {
    if (degree !== null) {
      setValue(degree);
    }
  }, [degree]);

  useEffect(() => {
    const intervalCallback = () => {
      if (drifting && value !== 0) {
        const newValue = value > 0 ? value - DRIFT_RATE : value + DRIFT_RATE;
        // zero clamp the newValue while its drifting
        setValue(
          (newValue > 0 && newValue < DRIFT_RATE) ||
            (newValue < 0 && newValue > -DRIFT_RATE)
            ? 0
            : newValue,
        );
        setDrifting(newValue !== 0);
        if (value === 0) {
          clearInterval(intervalId);
        }
        if (touched && value !== 0) {
          increment(value);
        }
      } else if (touched) {
        increment(value);
      }
    };

    if (value === 0) {
      return;
    }

    const intervalId = setInterval(intervalCallback, INTERVAL_MS);
    return () => clearInterval(intervalId);
  }, [drifting, value, increment]);

  const speed = `${value.toFixed(1)}°`;

  return (
    <div
      className={style.speedSlider}
      style={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
      }}
    >
      <output className={style.speedValue} id="output">
        <span className={style.speedValueNumber}>{speed}</span>
      </output>
      <Range
        values={[value]}
        step={STEP}
        min={MIN}
        max={MAX}
        onChange={values => {
          if (touched) {
            setValue(values[0]);
            setDrifting(false);
          }
        }}
        onFinalChange={() => {
          if (touched) {
            setDrifting(true);
            setTouched(false);
          }
        }}
        renderTrack={({props, children}) => (
          <div
            onMouseDown={event => {
              props.onMouseDown(event);
              setTouched(true);
              setDrifting(false);
            }}
            onTouchStart={event => {
              props.onTouchStart(event);
              setTouched(true);
              setDrifting(false);
            }}
            style={{
              ...props.style,
              height: '36px',
              display: 'flex',
              width: '100%',
            }}
          >
            <div
              ref={props.ref}
              style={{
                height: '5px',
                width: '100%',
                borderRadius: '2px',
                background: getTrackBackground({
                  values: [value],
                  colors: ['#0A7AFF', '#D1D1D1'],
                  min: MIN,
                  max: MAX,
                }),
                alignSelf: 'center',
              }}
            >
              {children}
            </div>
          </div>
        )}
        renderThumb={({props}) => (
          <div
            {...props}
            style={{
              ...props.style,
              height: '0.875rem',
              width: '0.875rem',
              borderRadius: '50%',
              backgroundColor: '#FFF',
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              boxShadow:
                '0px 0.5px 4px 0px rgba(0,0,0,0.12), 0px 6px 13px 0px rgba(0,0,0,0.12)',
            }}
          ></div>
        )}
      />
    </div>
  );
}
