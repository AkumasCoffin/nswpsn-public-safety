/**
 * Tests for centralwatchBrowser.fetchImagesBatchViaDom.
 *
 * The DOM path runs entirely inside `page.evaluate` — i.e. inside the
 * Chromium page context. To test it without spawning chromium we install
 * a stub `page` whose `evaluate` directly executes the script source
 * (a stringified IIFE) inside a JSDOM-ish shim that we mount onto the
 * Node global. The shim provides just enough of the DOM (document,
 * createElement('img'), createElement('canvas'), addEventListener) for
 * the in-page logic to run.
 *
 * We test:
 *   - Happy path (all loads fire, canvas paints, bytes flow back).
 *   - Mixed success/failure (some images error, some succeed).
 *   - Per-image timeout (image neither loads nor errors).
 *   - Empty input short-circuits.
 *   - isReady=false short-circuits.
 *
 * The actual DOM mechanics (Sec-Fetch-Dest header, canvas-tainting on a
 * real cross-origin image) are integration-level concerns we can't
 * exercise without a real browser.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { centralwatchBrowser } from '../../../src/services/centralwatchBrowser.js';

// ---------------------------------------------------------------------------
// Minimal DOM shim — only what the in-page IIFE touches.
// ---------------------------------------------------------------------------

interface FakeImg {
  tag: 'img';
  crossOrigin: string;
  style: { display: string };
  _src: string;
  src: string;
  naturalWidth: number;
  naturalHeight: number;
  width: number;
  height: number;
  _listeners: Map<string, Array<() => void>>;
  addEventListener: (ev: string, fn: () => void) => void;
  removeEventListener: (ev: string, fn: () => void) => void;
  remove: () => void;
  // Test helpers (not on real Image)
  _fire: (ev: 'load' | 'error') => void;
}

interface FakeCanvas {
  tag: 'canvas';
  style: { display: string };
  width: number;
  height: number;
  getContext: (kind: string) => { drawImage: () => void };
  toDataURL: (mime: string, q?: number) => string;
  remove: () => void;
}

type FakeNode = FakeImg | FakeCanvas;

let pendingImages: FakeImg[] = [];
let lastCanvas: FakeCanvas | null = null;

function makeImg(): FakeImg {
  const listeners = new Map<string, Array<() => void>>();
  const img: FakeImg = {
    tag: 'img',
    crossOrigin: '',
    style: { display: '' },
    _src: '',
    get src() {
      return this._src;
    },
    set src(v: string) {
      this._src = v;
    },
    naturalWidth: 640,
    naturalHeight: 480,
    width: 640,
    height: 480,
    _listeners: listeners,
    addEventListener(ev, fn) {
      const arr = listeners.get(ev) ?? [];
      arr.push(fn);
      listeners.set(ev, arr);
    },
    removeEventListener(ev, fn) {
      const arr = listeners.get(ev) ?? [];
      const i = arr.indexOf(fn);
      if (i >= 0) arr.splice(i, 1);
    },
    remove() {
      // Detach
    },
    _fire(ev) {
      const arr = listeners.get(ev) ?? [];
      for (const fn of [...arr]) fn();
    },
  };
  pendingImages.push(img);
  return img;
}

function makeCanvas(): FakeCanvas {
  const canvas: FakeCanvas = {
    tag: 'canvas',
    style: { display: '' },
    width: 0,
    height: 0,
    getContext: () => ({ drawImage: () => undefined }),
    // Returns a 1x1 JPEG-ish data URL whose base64 payload is large
    // enough (>500 bytes) for the cache filter, so consumers see real
    // bytes flowing through.
    toDataURL: (_mime: string, _q?: number) => {
      const payload = 'A'.repeat(800); // 800 base64 chars => 600 bytes decoded
      return `data:image/jpeg;base64,${payload}`;
    },
    remove() {
      // Detach
    },
  };
  lastCanvas = canvas;
  return canvas;
}

function installDomShim(): void {
  const docFake = {
    createElement: (kind: string): FakeNode => {
      if (kind === 'img') return makeImg();
      if (kind === 'canvas') return makeCanvas();
      throw new Error(`unexpected createElement(${kind})`);
    },
    body: {
      appendChild: (_n: FakeNode) => undefined,
    },
  };
  (globalThis as unknown as { document: unknown }).document = docFake;
}

function uninstallDomShim(): void {
  delete (globalThis as unknown as { document?: unknown }).document;
  pendingImages = [];
  lastCanvas = null;
}

// ---------------------------------------------------------------------------
// Stub page — its `evaluate(fnSrc, arg)` compiles the in-page script with
// `new Function` and runs it in this Node context. The DOM shim above is
// what the script will see.
// ---------------------------------------------------------------------------

interface StubPage {
  evaluate: (fnSrc: string, arg: unknown) => Promise<unknown>;
}

function makeStubPage(): StubPage {
  return {
    async evaluate(fnSrc: string, arg: unknown): Promise<unknown> {
      // The fn source we generate is `async (imageList) => { ... }`.
      // Wrap it so `new Function` can produce a callable.
      // eslint-disable-next-line @typescript-eslint/no-implied-eval
      const fn = new Function('arg', `return (${fnSrc})(arg);`) as (
        a: unknown,
      ) => Promise<unknown>;
      return fn(arg);
    },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('centralwatchBrowser.fetchImagesBatchViaDom', () => {
  beforeEach(() => {
    installDomShim();
    centralwatchBrowser._testSetReady(true);
    centralwatchBrowser._testSetPage(makeStubPage());
  });

  afterEach(() => {
    centralwatchBrowser._testSetReady(false);
    centralwatchBrowser._testSetPage(null);
    uninstallDomShim();
    vi.useRealTimers();
  });

  it('returns [] short-circuit when not ready', async () => {
    centralwatchBrowser._testSetReady(false);
    const r = await centralwatchBrowser.fetchImagesBatchViaDom([
      { id: 'a', url: 'http://x/a' },
    ]);
    expect(r).toEqual([]);
  });

  it('returns [] short-circuit on empty input', async () => {
    const r = await centralwatchBrowser.fetchImagesBatchViaDom([]);
    expect(r).toEqual([]);
  });

  it('happy path: all images fire load → canvas → bytes flow back', async () => {
    const inputs = [
      { id: 'cam-1', url: 'http://example/cam-1' },
      { id: 'cam-2', url: 'http://example/cam-2' },
    ];
    const promise = centralwatchBrowser.fetchImagesBatchViaDom(inputs);
    // Let the script create its <img> elements and register listeners.
    await Promise.resolve();
    await Promise.resolve();
    expect(pendingImages.length).toBe(2);
    for (const img of pendingImages) img._fire('load');
    const out = await promise;
    expect(out.length).toBe(2);
    for (const r of out) {
      expect(r.ok).toBe(true);
      expect(r.bytes).toBeInstanceOf(Buffer);
      expect((r.bytes as Buffer).length).toBeGreaterThan(500);
      expect(r.contentType).toBe('image/jpeg');
    }
    expect(out.map((r) => r.id).sort()).toEqual(['cam-1', 'cam-2']);
  });

  it('mixed: load + error each surface their own status', async () => {
    const inputs = [
      { id: 'cam-ok', url: 'http://example/ok' },
      { id: 'cam-err', url: 'http://example/err' },
    ];
    const promise = centralwatchBrowser.fetchImagesBatchViaDom(inputs);
    await Promise.resolve();
    await Promise.resolve();
    expect(pendingImages.length).toBe(2);
    pendingImages[0]._fire('load');
    pendingImages[1]._fire('error');
    const out = await promise;
    const ok = out.find((r) => r.id === 'cam-ok');
    const err = out.find((r) => r.id === 'cam-err');
    expect(ok?.ok).toBe(true);
    expect(err?.ok).toBe(false);
    expect(err?.error).toBe('load-error');
  });

  it('per-image timeout: never-firing image becomes ok=false error=timeout', async () => {
    vi.useFakeTimers();
    const inputs = [{ id: 'cam-hung', url: 'http://example/hung' }];
    const promise = centralwatchBrowser.fetchImagesBatchViaDom(inputs);
    await Promise.resolve();
    await Promise.resolve();
    expect(pendingImages.length).toBe(1);
    // Don't fire load/error. Advance past the 15s per-image timeout.
    await vi.advanceTimersByTimeAsync(15_001);
    const out = await promise;
    expect(out.length).toBe(1);
    expect(out[0].ok).toBe(false);
    expect(out[0].error).toBe('timeout');
  });

  it('zero-size image (loaded but no dimensions) reports ok=false', async () => {
    const inputs = [{ id: 'cam-zero', url: 'http://example/zero' }];
    const promise = centralwatchBrowser.fetchImagesBatchViaDom(inputs);
    await Promise.resolve();
    await Promise.resolve();
    expect(pendingImages.length).toBe(1);
    pendingImages[0].naturalWidth = 0;
    pendingImages[0].naturalHeight = 0;
    pendingImages[0].width = 0;
    pendingImages[0].height = 0;
    pendingImages[0]._fire('load');
    const out = await promise;
    expect(out[0].ok).toBe(false);
    expect(out[0].error).toContain('zero-size');
  });

  it('cleans up the shared canvas after the run', async () => {
    const inputs = [{ id: 'cam-1', url: 'http://example/cam-1' }];
    const promise = centralwatchBrowser.fetchImagesBatchViaDom(inputs);
    await Promise.resolve();
    await Promise.resolve();
    expect(lastCanvas).not.toBeNull();
    const removeSpy = vi.spyOn(
      lastCanvas as unknown as { remove: () => void },
      'remove',
    );
    pendingImages[0]._fire('load');
    await promise;
    expect(removeSpy).toHaveBeenCalled();
  });
});
