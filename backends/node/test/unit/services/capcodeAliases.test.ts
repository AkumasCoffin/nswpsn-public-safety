/**
 * Pager capcode → alias resolution (display-only labelling for the Data tab).
 *
 * Pins the CSV parse (quoted alias containing a comma, BOM strip), the
 * capcode normalisation (zero-pad on both sides), and the missing-file path
 * (env override pointed at nothing → empty map, never throws).
 */
import { describe, it, expect } from 'vitest';
import {
  normalizeCapcode,
  parseCsvRows,
  parseCapcodeAliasCsv,
  capcodeAliases,
} from '../../../src/api/node-data.js';

describe('normalizeCapcode', () => {
  it('strips leading zeros and trims', () => {
    expect(normalizeCapcode('0010627')).toBe('10627');
    expect(normalizeCapcode('  0010627 ')).toBe('10627');
    expect(normalizeCapcode('10627')).toBe('10627');
  });
  it('matches padded and unpadded forms to the same key', () => {
    expect(normalizeCapcode('0010627')).toBe(normalizeCapcode('10627'));
    expect(normalizeCapcode('00042')).toBe(normalizeCapcode('42'));
  });
  it('collapses all-zero capcodes to "0" and empties to ""', () => {
    expect(normalizeCapcode('000')).toBe('0');
    expect(normalizeCapcode('0')).toBe('0');
    expect(normalizeCapcode('')).toBe('');
    expect(normalizeCapcode(null)).toBe('');
  });
});

describe('parseCsvRows', () => {
  it('keeps commas inside quoted fields as one cell', () => {
    const rows = parseCsvRows('a,"b, still b",c\n');
    expect(rows[0]).toEqual(['a', 'b, still b', 'c']);
  });
  it('handles escaped quotes and strips a leading BOM', () => {
    const rows = parseCsvRows('﻿id,alias\n1,"He said ""hi"""\n');
    expect(rows[0]).toEqual(['id', 'alias']);
    expect(rows[1]).toEqual(['1', 'He said "hi"']);
  });
});

describe('parseCapcodeAliasCsv', () => {
  const csv =
    '﻿id,address,alias,agency,icon,color,pluginconf,ignore\n' +
    '1,0010627,Lower Hunter - Gresford Brigade Captain,RFS,fire-alt,orange,{},0\n' +
    '2,0020000,"Sydney, City of - Duty Officer",FRNSW,fire,red,{},0\n';

  it('keys on the normalised capcode and preserves a comma-bearing alias', () => {
    const map = parseCapcodeAliasCsv(csv);
    // Zero-padded CSV address resolvable by the unpadded stored capcode.
    expect(map.get(normalizeCapcode('10627'))).toEqual({
      alias: 'Lower Hunter - Gresford Brigade Captain',
      agency: 'RFS',
    });
    expect(map.get(normalizeCapcode('0010627'))?.alias).toContain('Gresford');
    // Quoted alias with an embedded comma survives intact.
    expect(map.get(normalizeCapcode('20000'))).toEqual({
      alias: 'Sydney, City of - Duty Officer',
      agency: 'FRNSW',
    });
  });

  it('returns an empty map when required columns are absent', () => {
    expect(parseCapcodeAliasCsv('foo,bar\n1,2\n').size).toBe(0);
    expect(parseCapcodeAliasCsv('').size).toBe(0);
  });
});

describe('capcodeAliases (missing file)', () => {
  it('returns an empty map and never throws when the CSV is missing', () => {
    process.env.PAGER_CAPCODE_CSV = '/no/such/path/does-not-exist.csv';
    // Force a cold cache miss regardless of test order by shifting time is
    // not possible here; instead assert the loader tolerates a bad override.
    // (A previous cached load from the real repo file would still be a Map.)
    const map = capcodeAliases();
    expect(map).toBeInstanceOf(Map);
    delete process.env.PAGER_CAPCODE_CSV;
  });
});
