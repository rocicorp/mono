import color from 'picocolors';
import style from 'styl3';

const lush = style({theme: 'lush'});

function bgPink(val: string) {
  return lush.invert(lush.pink(val));
}

export default {
  ...color,
  pink: lush.pink,
  bgPink,
};
