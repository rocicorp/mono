// TODO(arv): Add support for https://jsr.io/@unplugin/macros

export function seconds(s: number) {
  return s * 1000;
}

export function minutes(m: number) {
  return seconds(m * 60);
}

export function hours(h: number) {
  return minutes(h * 60);
}

export function days(d: number) {
  return hours(d * 24);
}
