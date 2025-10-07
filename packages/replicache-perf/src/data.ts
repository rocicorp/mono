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
      /* eslint-disable @typescript-eslint/naming-convention */
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
      /* eslint-enable @typescript-eslint/naming-convention */
    };
  }[];
};

export async function getTmcwData(): Promise<TmcwData> {
  const response = await fetch(
    new URL('../resources/tmcw.json', import.meta.url),
  );
  return response.json();
}
