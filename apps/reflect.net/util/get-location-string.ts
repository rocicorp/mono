export type Location = {
  country: string;
  city: string;
  region: string;
} | null;

export function getLocationString(location: Location) {
  if (location === null) return null;
  const {country, city} = location;
  const flagEmoji = String.fromCodePoint(
    ...country
      .toUpperCase()
      .split('')
      .map((char: string) => 127397 + char.charCodeAt(0)),
  );
  return `${decodeURI(city)} ${flagEmoji}`;
}
