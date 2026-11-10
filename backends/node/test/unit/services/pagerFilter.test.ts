/**
 * The relay and the staff Data tab must agree on what is filtered: the relay
 * decides in JS per message, the Data tab re-applies the rules in SQL over
 * stored pages. These pin the JS rules and hold the SQL to the same shapes.
 */
import { describe, it, expect } from 'vitest';
import {
  BLOCKED_CAPCODES,
  pagerFilterReason,
  pagerFilterReasonSql,
  isPagerTimeCode,
} from '../../../src/services/pagerFilter.js';

describe('pagerFilterReason', () => {
  it('blocks the default non-message capcodes whatever they carry', () => {
    expect(BLOCKED_CAPCODES.has('1850000')).toBe(true);
    expect(BLOCKED_CAPCODES.has('521839')).toBe(true);   // config default
    expect(pagerFilterReason('521839', 'UTEUGD+XEHBO8abkwP/QJeRVLl7sFpwCKzSNalVsvlaYouUZizrEvXxaFE6Y'))
      .toBe('blocked capcode');
    // Blocked wins over every other rule, as at the relay.
    expect(pagerFilterReason('1850000', '')).toBe('blocked capcode');
  });

  it('names empty bodies, garbage and clock ticks, in the relay order', () => {
    expect(pagerFilterReason('1160008', '   ')).toBe('empty message');
    expect(pagerFilterReason('1160008', '=m')).toBe('noise');
    expect(pagerFilterReason('180111', '0157')).toBe('time code');
  });

  it('keeps real traffic', () => {
    expect(pagerFilterReason('107911', 'FRINC TYPE: AFA TURNOUT: 79 INC: 207681-20102026')).toBeNull();
    expect(pagerFilterReason('1160008', '17:18:59 SNCUDMI - 26-131818 - MVA persons trapped')).toBeNull();
    expect(pagerFilterReason('180111', '9999')).toBeNull();   // not a time of day
    expect(pagerFilterReason('1160008', 'SES')).toBeNull();
  });
});

describe('pagerFilterReasonSql', () => {
  const sql = pagerFilterReasonSql('$2', 'e');

  it('checks the rules in the relay order, against the bound blocked list', () => {
    const at = (s: string) => sql.indexOf(s);
    expect(at("e.capcode = ANY($2::text[]) THEN 'blocked capcode'")).toBeGreaterThan(-1);
    expect(at("= 0 THEN 'empty message'")).toBeGreaterThan(at("THEN 'blocked capcode'"));
    expect(at("<= 2 THEN 'noise'")).toBeGreaterThan(at("THEN 'empty message'"));
    expect(at("THEN 'time code'")).toBeGreaterThan(at("THEN 'noise'"));
    // Trims the stored message the way the relay trims the incoming one.
    expect(sql).toContain("regexp_replace(COALESCE(e.message, ''), '^\\s+|\\s+$', '', 'g')");
  });

  it('matches a time code exactly where isPagerTimeCode does, over every four digits', () => {
    const pattern = /~ '([^']+)' THEN 'time code'/.exec(sql)?.[1];
    expect(pattern).toBeDefined();
    const sqlRe = new RegExp(pattern!);
    for (let n = 0; n <= 9999; n++) {
      const s = String(n).padStart(4, '0');
      if (sqlRe.test(s) !== isPagerTimeCode(s)) throw new Error(`disagree on ${s}`);
    }
  });

  it('reads the bare columns when the query has no alias', () => {
    expect(pagerFilterReasonSql('$2')).toContain('WHEN capcode = ANY($2::text[])');
  });
});
