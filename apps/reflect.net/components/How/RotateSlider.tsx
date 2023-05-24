import {useEffect, useState} from 'react';
import {Range, getTrackBackground} from 'react-range';
import style from './Slider.module.css';
import {event} from 'nextjs-google-analytics';

const STEP = 1;
const MIN = 0;
const MAX = 360;

const RotateSlider = ({
  increment,
  degree,
}: {
  increment: (delta: number) => void;
  degree: number | undefined;
}) => {
  const [value, setValue] = useState(0);
  const [touched, setTouched] = useState(false);

  useEffect(() => {
    if (!touched) {
      setValue(degree ?? 0);
    }
  }, [degree]);

  useEffect(() => {
    if (touched) {
      increment(value);
    }
  }, [touched, value, increment]);

  const speed = `${value}°`;

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
          }
        }}
        onFinalChange={() => {
          if (touched) {
            setTouched(false);
          }
          event('demo_2_rotate', {
            category: 'How it Works',
            action: 'Adjust rotation of shape in demo 2',
            label: 'Demo 2',
          });
        }}
        renderTrack={({props, children}) => (
          <div
            onMouseDown={event => {
              props.onMouseDown(event);
              setTouched(true);
            }}
            onTouchStart={event => {
              props.onTouchStart(event);
              setTouched(true);
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
};

export default RotateSlider;
