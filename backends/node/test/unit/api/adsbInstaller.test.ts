/**
 * The ADS-B installer's decoder step.
 *
 * FlightAware publish one versioned repository package and delete the previous
 * one when they bump it. The installer pinned `_1.2_`, which they removed, so
 * every fresh ADS-B node printed `curl: (22) ... error: 404` and carried on
 * onto dump1090-mutability — a decoder that writes no stats.json, and so
 * reports no range at all. The install still "succeeded", which is what made
 * it survive: nothing about a working node said it had been downgraded.
 */
import { describe, it, expect } from 'vitest';
import { execFileSync } from 'node:child_process';
import { writeFileSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { _adsbLinuxInstaller } from '../../../src/api/feeder.js';

const script = _adsbLinuxInstaller('enrol-code-123');

describe('the FlightAware repository fetch', () => {
  it('does not pin the version FlightAware deleted', () => {
    expect(script).not.toContain('flightaware-apt-repository_1.2_all.deb');
  });

  it('tries several versions rather than one', () => {
    // The point of the change: a single pinned URL is a time bomb that goes
    // off silently, so the next bump must not need a code change.
    const list = /for v in ([0-9. ]+); do/.exec(script);
    expect(list).not.toBeNull();
    const versions = list![1]!.trim().split(/\s+/);
    expect(versions.length).toBeGreaterThan(1);
    // Newest first, so a live newer release wins over a live older one.
    const num = (v: string) => Number(v.replace('.', ''));
    expect([...versions].sort((a, b) => num(b) - num(a))).toEqual(versions);
    // 1.3 is the oldest one known to still exist; below it is all 404.
    expect(versions).toContain('1.3');
  });

  it('keeps curl quiet so a miss is not mistaken for a failed install', () => {
    // `curl: (22) The requested URL returned error: 404` in the middle of an
    // otherwise clean install is what made this look like a broken installer.
    const probe = /if curl -fsSL "\$\{FA_POOL\}[^\n]*/.exec(script);
    expect(probe).not.toBeNull();
    expect(probe![0]).toContain('2>/dev/null');
  });

  it('says so when it falls back, instead of failing silently', () => {
    expect(script).toMatch(/note: FlightAware's repository was unreachable/);
    expect(script).toMatch(/range statistics will be limited/);
  });

  it('still falls back rather than aborting the install', () => {
    // A node on mutability is degraded, not useless; refusing to install
    // would be worse than what it replaces.
    expect(script).toContain('dump1090-mutability');
  });
});

describe('the DVB driver that holds the dongle', () => {
  // A receiver was found crash-looping dump1090 every three seconds with
  // "usb_claim_interface error -6" — the DVB-T driver had the SDR and would
  // not release it. The installer wrote the blacklist (which only helps the
  // NEXT boot), called `modprobe -r` on one module, hid its output and
  // reported success, so the machine was left permanently unable to decode
  // with nothing said about it.

  it('removes the whole module stack, not just the leaf', () => {
    // rtl2832 is what dvb_usb_rtl28xxu sits on; removing the base while the
    // leaf is loaded fails, so order and coverage both matter.
    const loop = /for m in ([a-z0-9_ ]+); do/.exec(script);
    expect(loop).not.toBeNull();
    const mods = loop![1]!.trim().split(/\s+/);
    expect(mods[0]).toBe('dvb_usb_rtl28xxu');
    expect(mods).toContain('rtl2832');
    expect(mods).toContain('rtl2830');
  });

  it('still blacklists, because eviction does not survive a reboot', () => {
    expect(script).toContain('/etc/modprobe.d/blacklist-nswpsn-rtl.conf');
    expect(script).toContain('blacklist dvb_usb_rtl28xxu');
  });

  it('proves the SDR opens rather than assuming it did', () => {
    // The whole failure was a silent one: modprobe -r says nothing when it
    // cannot remove a module that is in use.
    expect(script).toMatch(/rtl_test -t/);
    expect(script).toContain('SDR_OK');
  });

  it('names the remedy when the driver still has the dongle', () => {
    expect(script).toMatch(/WARNING: the SDR could not be opened/);
    expect(script).toContain('lsmod');
    // A reboot is the fix once the blacklist is in place, and saying so is the
    // difference between a broken node and a two-minute one.
    expect(script).toMatch(/REBOOT/);
    expect(script).toMatch(/dump1090 will crash-loop/);
  });

  it('does not abort the install over it', () => {
    // The node still enrols and starts; it simply cannot decode yet. Failing
    // the install would throw away the working half.
    const warn = script.slice(script.indexOf('SDR_OK'));
    expect(warn.slice(0, warn.indexOf('cat > /etc/systemd'))).not.toContain('exit 1');
  });
});

describe('the generated script', () => {
  it('is syntactically valid shell', () => {
    // The probe URL lives inside a JS template literal, so `${FA_POOL}` has to
    // reach the file escaped. Getting that wrong yields a script that is
    // plausible on screen and broken on the node — this is the check that
    // catches it.
    const dir = mkdtempSync(join(tmpdir(), 'adsb-installer-'));
    const path = join(dir, 'install.sh');
    writeFileSync(path, script);
    expect(() => execFileSync('bash', ['-n', path], { stdio: 'pipe' })).not.toThrow();
  });

  it('leaves the shell variables unexpanded by JavaScript', () => {
    // If the escaping were dropped, these would have been interpolated away to
    // empty strings at build time and the loop would fetch a bare slash.
    expect(script).toContain('${FA_POOL}/flightaware-apt-repository_${v}_all.deb');
    expect(script).not.toContain('flightaware-apt-repository__all.deb');
  });
});
