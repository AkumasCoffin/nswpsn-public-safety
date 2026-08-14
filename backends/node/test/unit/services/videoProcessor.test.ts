/**
 * The Wire video pipeline: the ffmpeg argv builders (pure) and the background
 * worker (fake pool + fake R2, real control flow).
 *
 * The load-bearing property under test is that failure is NON-DESTRUCTIVE: the
 * raw upload is only deleted after the DB row points at the transcoded object,
 * so a transcode bug can never lose a contributor's footage.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

type Call = { sql: string; params?: unknown[] };
const calls: Call[] = [];
const txCalls: Call[] = [];
let resultQueue: Array<{ rows: unknown[]; rowCount?: number }> = [];

const fakeClient = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    txCalls.push({ sql, ...(params ? { params } : {}) });
    if (/^\s*(BEGIN|COMMIT|ROLLBACK)/i.test(sql)) return { rows: [], rowCount: 0 };
    return resultQueue.shift() ?? { rows: [], rowCount: 0 };
  }),
  release: vi.fn(),
};

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.shift() ?? { rows: [], rowCount: 0 };
  }),
  connect: vi.fn(async () => fakeClient),
};

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => fakePool),
  getWriterPool: vi.fn(async () => fakePool),
}));

/** Order matters: proves the raw delete happens AFTER the DB update. */
const r2Ops: string[] = [];
let downloadOk = true;
let uploadOk = true;

vi.mock('../../../src/services/wire.js', () => ({
  r2Configured: () => true,
  downloadR2ToFile: vi.fn(async (key: string) => {
    r2Ops.push(`download:${key}`);
    return downloadOk;
  }),
  uploadFileToR2: vi.fn(async (_p: string, key: string) => {
    r2Ops.push(`upload:${key}`);
    return uploadOk;
  }),
  deleteR2Object: vi.fn(async (key: string) => {
    r2Ops.push(`delete:${key}`);
  }),
  newVideoKey: () => 'wire/videos/NEW.mp4',
  newPosterKey: () => 'wire/img/NEWPOSTER.jpg',
}));

let transcodeOk = true;
let ffmpegOn = true;
const transcodeCalls: Array<{ watermarkText?: string | null }> = [];

vi.mock('../../../src/services/videoTranscode.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/videoTranscode.js')>();
  return {
    ...actual,
    ffmpegAvailable: () => ffmpegOn,
    transcodeVideo: vi.fn(async (opts: { watermarkText?: string | null }) => {
      transcodeCalls.push(opts);
      r2Ops.push('transcode');
      if (!transcodeOk) throw new Error('ffmpeg exploded');
      return { durationSeconds: 12, width: 1920, height: 1080, watermarked: !!opts.watermarkText };
    }),
  };
});

const { processPendingVideosOnce } = await import('../../../src/services/videoProcessor.js');
const { buildFilter, buildTranscodeArgs, parseProbe } =
  await import('../../../src/services/videoTranscode.js');

const PENDING_ROW = {
  id: 'm1', r2_key: 'wire/videos/RAW.mp4', poster_r2_key: null,
  parent_type: 'media_post', parent_id: 'p1',
};

/** Queue: claim → parent watermark lookup → update. Then an empty claim ends it. */
function queueOneJob(parent: { watermark: boolean; author_name?: string | null }) {
  resultQueue = [
    { rows: [PENDING_ROW] },   // claim
    { rows: [parent] },        // watermarkFor
    { rows: [], rowCount: 1 }, // UPDATE ... process_state='done'
    { rows: [] },              // next claim: nothing left
  ];
}

beforeEach(() => {
  calls.length = 0; txCalls.length = 0; r2Ops.length = 0; transcodeCalls.length = 0;
  resultQueue = [];
  downloadOk = uploadOk = transcodeOk = ffmpegOn = true;
  vi.clearAllMocks();
});

describe('videoProcessor worker', () => {
  it('claims only pending videos', async () => {
    resultQueue = [{ rows: [] }];
    await processPendingVideosOnce();
    const claim = txCalls.find((c) => /FROM wire_media/i.test(c.sql));
    expect(claim?.sql).toMatch(/kind = 'video'/);
    expect(claim?.sql).toMatch(/process_state = 'pending'/);
    expect(claim?.sql).toMatch(/FOR UPDATE SKIP LOCKED/);
  });

  it('transcodes, writes a NEW key, then deletes the raw upload', async () => {
    queueOneJob({ watermark: true, author_name: 'akuma' });
    const n = await processPendingVideosOnce();
    expect(n).toBe(1);

    const upd = calls.find((c) => /UPDATE wire_media/i.test(c.sql) && /process_state    = 'done'/.test(c.sql));
    expect(upd).toBeTruthy();
    // New key, not an overwrite of the raw one.
    expect(upd?.params?.[1]).toBe('wire/videos/NEW.mp4');
    expect(upd?.params?.[2]).toBe('wire/img/NEWPOSTER.jpg');
    // Probed dimensions/duration are backfilled (the client never sets them).
    expect(upd?.params?.slice(3, 6)).toEqual([1920, 1080, 12]);

    // The ordering that makes failure non-destructive.
    expect(r2Ops).toEqual([
      'download:wire/videos/RAW.mp4',
      'transcode',
      'upload:wire/videos/NEW.mp4',
      'upload:wire/img/NEWPOSTER.jpg',
      'delete:wire/videos/RAW.mp4',
    ]);
  });

  it('passes the author name as the watermark only when the post opted in', async () => {
    queueOneJob({ watermark: true, author_name: 'akuma' });
    await processPendingVideosOnce();
    expect(transcodeCalls[0]?.watermarkText).toBe('akuma');

    transcodeCalls.length = 0;
    queueOneJob({ watermark: false, author_name: 'akuma' });
    await processPendingVideosOnce();
    expect(transcodeCalls[0]?.watermarkText).toBeNull();
  });

  it('leaves the original in place and increments attempts when ffmpeg fails', async () => {
    transcodeOk = false;
    resultQueue = [
      { rows: [PENDING_ROW] },
      { rows: [{ watermark: false }] },
      { rows: [], rowCount: 1 }, // recordFailure
      { rows: [] },
    ];
    const n = await processPendingVideosOnce();
    expect(n).toBe(0);

    // Nothing was deleted — the raw upload is still playable.
    expect(r2Ops.some((o) => o.startsWith('delete:'))).toBe(false);
    const fail = calls.find((c) => /process_attempts = process_attempts \+ 1/.test(c.sql));
    expect(fail).toBeTruthy();
    expect(fail?.sql).toMatch(/'failed'/);
    expect(fail?.params?.[2]).toBe(3); // parked after 3 attempts
  });

  it('does not delete anything when the upload of the result fails', async () => {
    uploadOk = false;
    resultQueue = [
      { rows: [PENDING_ROW] },
      { rows: [{ watermark: false }] },
      { rows: [], rowCount: 1 },
      { rows: [] },
    ];
    await processPendingVideosOnce();
    expect(r2Ops.some((o) => o.startsWith('delete:'))).toBe(false);
    expect(calls.some((c) => /process_state    = 'done'/.test(c.sql))).toBe(false);
  });

  it('does nothing at all when ffmpeg is unavailable', async () => {
    ffmpegOn = false;
    resultQueue = [{ rows: [PENDING_ROW] }];
    const n = await processPendingVideosOnce();
    expect(n).toBe(0);
    expect(txCalls).toHaveLength(0);
    expect(r2Ops).toHaveLength(0);
  });
});

describe('ffmpeg argv', () => {
  it('never upscales and always strips metadata', () => {
    const args = buildTranscodeArgs('in.mp4', 'out.mp4', null);
    expect(args).toContain('-map_metadata');
    expect(args[args.indexOf('-map_metadata') + 1]).toBe('-1');
    expect(args[args.indexOf('-vf') + 1]).toBe("scale='min(1920,iw)':-2");
    expect(args).toContain('+faststart');
  });

  it('adds drawtext only when watermarking', () => {
    expect(buildFilter(null)).not.toContain('drawtext');
    expect(buildFilter('   ')).not.toContain('drawtext');
    expect(buildFilter('akuma')).toContain("drawtext=text='akuma'");
  });

  it('escapes filter metacharacters in the username', () => {
    // A raw colon or quote would break the filter graph (or inject into it).
    const f = buildFilter("a:b'c");
    expect(f).toContain("text='a\\:b\\'c'");
  });

  it('passes an explicit fontfile when one was resolved', () => {
    expect(buildFilter('akuma', '/usr/share/fonts/x.ttf'))
      .toContain("fontfile='/usr/share/fonts/x.ttf'");
    // Windows drive colons survive escaping.
    expect(buildFilter('akuma', 'C:\\Windows\\Fonts\\arial.ttf'))
      .toContain("fontfile='C\\:/Windows/Fonts/arial.ttf'");
  });

  it('parses duration and dimensions out of the ffmpeg banner', () => {
    const stderr = `  Duration: 00:01:05.20, start: 0.000000, bitrate: 8000 kb/s
  Stream #0:0: Video: h264 (High), yuv420p, 1920x1080 [SAR 1:1 DAR 16:9], 30 fps`;
    expect(parseProbe(stderr)).toEqual({ durationSeconds: 65, width: 1920, height: 1080 });
  });
});
