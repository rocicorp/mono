import {simpleHash} from './util';

// start color, end color
type Color = [number, number, number];
export const COLOR_PALATE: [Color, Color][] = [
  // Pink
  [
    [252, 73, 171],
    [223, 0, 122],
  ],
  // Light Blue
  [
    [95, 232, 255],
    [0, 197, 229],
  ],
  // Orange
  [
    [255, 153, 0],
    [211, 127, 0],
  ],
  // Green
  [
    [100, 255, 0],
    [94, 217, 15],
  ],
  // Blue
  [
    [57, 184, 255],
    [29, 157, 229],
  ],
  // Red
  [
    [255, 156, 156],
    [238, 126, 126],
  ],
  // Turquoise
  [
    [46, 214, 214],
    [39, 195, 195],
  ],
  // Magenta
  [
    [235, 10, 255],
    [213, 5, 232],
  ],
  // Citrine
  [
    [237, 200, 4],
    [219, 185, 9],
  ],
];

export function idToColor(id: string) {
  const h = simpleHash(id);
  const m = Math.abs(h % COLOR_PALATE.length);
  const [color] = COLOR_PALATE[m];
  return color;
}

export function colorToString(c: Color) {
  return `rgb(${c[0]}, ${c[1]}, ${c[2]})`;
}
