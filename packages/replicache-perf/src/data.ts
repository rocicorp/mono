// Re-export shared test data utilities
export {
  jsonArrayTestData,
  jsonObjectTestData,
  makeRandomASCIIStrings,
  makeRandomStrings,
  randomASCIIString,
  randomBoolean,
  randomData,
  randomFloat64,
  randomInt32,
  randomObject,
  randomString,
  type RandomData,
  type RandomDataType,
  type RandomDatum,
  type TestDataObject,
} from '../../shared/src/test-data.ts';

type TmcwData = {
  type: string;
  features: {
    type: string;
    geometry: {
      type: string;
      coordinates: [number, number][];
    };
    properties: {
      rwdb_rr_id: number;
      mult_track: number;
      electric: number;
      other_code: number;
      category: number;
      disp_scale: string;
      add: number;
      featurecla: string;
      scalerank: number;
      natlscale: number;
      part: string;
      continent: string;
    };
  }[];
};

let tmcwUrl: string | URL | undefined;

/**
 * Overrides where {@link getTmcwData} loads its 9.7 MB fixture from. React
 * Native cannot resolve `import.meta.url` and the file is far too large to
 * bundle, so the RN runner serves it over HTTP and points us at it.
 */
export function setTmcwUrl(url: string | URL): void {
  tmcwUrl = url;
}

export async function getTmcwData(): Promise<TmcwData> {
  // tool/build.ts defines import.meta.url away for the React Native bundle
  // (Hermes cannot parse it), so without an override this would construct
  // `new URL('../resources/tmcw.json', '')` and throw a bare "Invalid URL".
  if (tmcwUrl === undefined && !import.meta.url) {
    throw new Error(
      'No tmcw fixture URL. On React Native the 9.7 MB fixture is served by the ' +
        'runner rather than bundled — call setTmcwUrl() (configure() does) ' +
        'before running a benchmark that needs it.',
    );
  }
  const response = await fetch(
    tmcwUrl ?? new URL('../resources/tmcw.json', import.meta.url),
  );
  return response.json();
}
