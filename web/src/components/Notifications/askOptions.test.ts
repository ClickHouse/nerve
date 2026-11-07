import { describe, expect, it } from 'vitest';
import { parseAskOptions } from './askOptions';

describe('parseAskOptions', () => {
  it('takes a list as it is', () => {
    expect(parseAskOptions(['Yes, all of these', ' No ', ''])).toEqual(['Yes, all of these', 'No']);
  });

  it('keeps a comma inside an option of a JSON array string', () => {
    expect(parseAskOptions('["Yes, all of these","No"]')).toEqual(['Yes, all of these', 'No']);
  });

  it('splits a plain string at commas', () => {
    expect(parseAskOptions('Yes, No , Maybe,')).toEqual(['Yes', 'No', 'Maybe']);
  });

  it('splits JSON that is not an array at commas', () => {
    expect(parseAskOptions('{"a": 1, "b": 2}')).toEqual(['{"a": 1', '"b": 2}']);
  });

  it('gives no options for nothing', () => {
    expect(parseAskOptions(undefined)).toEqual([]);
    expect(parseAskOptions(null)).toEqual([]);
    expect(parseAskOptions('  ')).toEqual([]);
    expect(parseAskOptions(3)).toEqual([]);
  });
});
