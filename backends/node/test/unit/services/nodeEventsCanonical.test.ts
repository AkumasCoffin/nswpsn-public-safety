/**
 * canonicalPair: which (system, wacn) a reception is filed under.
 *
 * The case that matters here: an event that arrives with its network LABEL
 * but no numeric identity. An agent can emit events before its control
 * channel has identified the system, and filed under (NULL, NULL, talkgroup)
 * they formed a second group beside the real one — the same talkgroup listed
 * twice on a node's talkgroup table, the duplicate showing no site.
 */
import { describe, it, expect } from 'vitest';
import { canonicalPair } from '../../../src/services/nodeEvents.js';

const NSWPSN = { system: 721, wacn: 781824, events: 50_000 };
const canon = new Map([['NSWPSN', NSWPSN]]);

describe('canonicalPair', () => {
  it('files a labelled event with no numeric identity under its network', () => {
    // The production case: label present, system and wacn both null.
    const r = canonicalPair(canon, 'NSWPSN', null, null);
    expect(r).toEqual({ system: 721, wacn: 781824, corrected: true });
  });

  it('leaves a matching identity alone', () => {
    const r = canonicalPair(canon, 'NSWPSN', 721, 781824);
    expect(r).toEqual({ system: 721, wacn: 781824, corrected: false });
  });

  it('still corrects a mismatched identity to the network\'s pair', () => {
    const r = canonicalPair(canon, 'NSWPSN', 999, 1);
    expect(r).toEqual({ system: 721, wacn: 781824, corrected: true });
  });

  it('cannot file an unlabelled event anywhere', () => {
    // No label means no network to look up — the event keeps whatever it had,
    // null included. Inventing an identity here would be a guess.
    expect(canonicalPair(canon, null, null, null))
      .toEqual({ system: null, wacn: null, corrected: false });
  });

  it('does not adopt a network it has barely heard from', () => {
    // Below the evidence floor the canonical pair is not trusted, for the
    // null case exactly as for the mismatch case.
    const thin = new Map([['NEWNET', { system: 5, wacn: 6, events: 10 }]]);
    expect(canonicalPair(thin, 'NEWNET', null, null))
      .toEqual({ system: null, wacn: null, corrected: false });
  });

  it('an unknown label changes nothing', () => {
    expect(canonicalPair(canon, 'SOMEWHERE', null, null))
      .toEqual({ system: null, wacn: null, corrected: false });
  });
});
