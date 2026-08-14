/**
 * ffmpeg wrapper for Wire video post-processing.
 *
 * One pass does four jobs that would otherwise need separate solutions:
 *   - normalises bitrate/resolution (a 50MB phone clip becomes ~8-15MB)
 *   - burns the contributor's watermark in (photos already get a real one;
 *     video previously only had a cosmetic overlay that vanished on download)
 *   - strips ALL metadata, closing the video GPS/EXIF gap
 *   - cuts a poster frame server-side, so it no longer depends on the
 *     uploader's browser being able to decode the codec
 *
 * The binary comes from `ffmpeg-static` (installed by the existing npm step in
 * deploy.sh — no sudo/apt, same shape as Playwright's Chromium). FFMPEG_PATH
 * overrides it; VIDEO_TRANSCODE_DISABLED turns the whole feature off.
 */
import { execFile } from 'node:child_process';
import { createRequire } from 'node:module';
import { promisify } from 'node:util';
import { log } from '../lib/log.js';

const execFileAsync = promisify(execFile);
const require = createRequire(import.meta.url);

/** Output ceiling. Never upscales — a 720p source stays 720p. */
const MAX_WIDTH = 1920;
const VIDEO_BITRATE = '2500k';
const VIDEO_MAXRATE = '3000k';
const VIDEO_BUFSIZE = '5000k';
const AUDIO_BITRATE = '128k';
/** Hard ceiling per clip. A 50MB input at `veryfast` is normally well under. */
const TRANSCODE_TIMEOUT_MS = 10 * 60_000;

let _ffmpegPath: string | null | undefined;

/**
 * Absolute path to the ffmpeg binary, or null when it can't be found (in which
 * case the whole pipeline no-ops and videos are served as uploaded).
 */
export function ffmpegPath(): string | null {
  if (_ffmpegPath !== undefined) return _ffmpegPath;
  const override = (process.env['FFMPEG_PATH'] ?? '').trim();
  if (override) {
    _ffmpegPath = override;
    return _ffmpegPath;
  }
  try {
    // ffmpeg-static default-exports the path (null on an unsupported platform).
    // A sync require() rather than await import() so this stays a plain
    // predicate — and so a missing/broken install degrades to "no transcoding"
    // instead of throwing somewhere unexpected.
    const mod = require('ffmpeg-static') as string | { default?: string } | null;
    const p = typeof mod === 'string' ? mod : (mod?.default ?? null);
    _ffmpegPath = p || null;
  } catch (err) {
    log.warn({ err }, 'video: ffmpeg-static unavailable — transcoding disabled');
    _ffmpegPath = null;
  }
  return _ffmpegPath;
}

/** Whether video transcoding can run at all. */
export function ffmpegAvailable(): boolean {
  if ((process.env['VIDEO_TRANSCODE_DISABLED'] ?? '') === 'true') return false;
  return !!ffmpegPath();
}

/** For tests. */
export function _resetFfmpegPathCache(): void {
  _ffmpegPath = undefined;
}

export interface TranscodeResult {
  durationSeconds: number | null;
  width: number | null;
  height: number | null;
  /** False when a watermark was asked for but drawtext couldn't render it. */
  watermarked?: boolean;
}

/**
 * Escape a string for ffmpeg's drawtext filter. Colons and backslashes are
 * filter-syntax metacharacters, and single quotes terminate the quoted value —
 * an unescaped username would break the filter graph (or inject into it).
 */
function escapeDrawText(text: string): string {
  return text
    .replace(/\\/g, '\\\\')
    .replace(/:/g, '\\:')
    .replace(/'/g, "\\'")
    .replace(/%/g, '\\%');
}

/**
 * Path to escape for use as a drawtext `fontfile` value — on top of the normal
 * escaping, a Windows drive letter's colon has to survive.
 */
function escapeFontFile(p: string): string {
  return p.replace(/\\/g, '/').replace(/:/g, '\\:').replace(/'/g, "\\'");
}

/** Build the video filter chain: downscale-if-needed, then optional watermark. */
export function buildFilter(watermarkText: string | null, fontFile?: string | null): string {
  // -2 keeps the height even (H.264 requires it) and preserves aspect.
  // min(MAX_WIDTH, iw) is what makes this never upscale.
  const parts = [`scale='min(${MAX_WIDTH},iw)':-2`];
  const text = (watermarkText ?? '').trim();
  if (text) {
    // Sized relative to the frame so it reads the same on any resolution —
    // matching how the photo watermark scales.
    const font = (fontFile ?? '').trim();
    parts.push(
      `drawtext=${font ? `fontfile='${escapeFontFile(font)}':` : ''}` +
        `text='${escapeDrawText(text)}'` +
        `:fontcolor=white@0.82:fontsize=h/28` +
        `:shadowcolor=black@0.65:shadowx=1:shadowy=1` +
        `:x=w-tw-(h/36):y=h-th-(h/36)`,
    );
  }
  return parts.join(',');
}

/**
 * Candidate fonts for the watermark, in preference order. Only consulted when
 * bare `drawtext` fails — some static ffmpeg builds ship without a fontconfig
 * default, and there is no font in this repo to bundle.
 */
const FONT_CANDIDATES = [
  '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
  '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
  '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
  '/usr/share/fonts/dejavu/DejaVuSans.ttf',
  'C:/Windows/Fonts/arial.ttf',
];

let _fontProbe: { ok: boolean; fontFile: string | null } | undefined;

/**
 * Work out, once, whether drawtext can render at all and whether it needs an
 * explicit fontfile. Cheap (a one-frame null render), and the result is cached
 * for the process. `ok: false` means watermarking is impossible here — the
 * video is still normalised, just unmarked, which beats failing the job.
 */
async function resolveFont(): Promise<{ ok: boolean; fontFile: string | null }> {
  if (_fontProbe) return _fontProbe;
  const bin = ffmpegPath();
  if (!bin) return (_fontProbe = { ok: false, fontFile: null });

  const override = (process.env['FFMPEG_FONT_FILE'] ?? '').trim();
  const tryFont = async (font: string | null): Promise<boolean> => {
    try {
      await execFileAsync(bin, [
        '-hide_banner', '-loglevel', 'error',
        '-f', 'lavfi', '-i', 'color=c=black:s=64x64:d=1',
        '-vf', `drawtext=${font ? `fontfile='${escapeFontFile(font)}':` : ''}text='x':fontsize=12`,
        '-frames:v', '1', '-f', 'null', '-',
      ], { timeout: 20_000, maxBuffer: 1024 * 1024 });
      return true;
    } catch {
      return false;
    }
  };

  for (const font of [override || null, null, ...FONT_CANDIDATES]) {
    if (font === null && override) continue; // already covered by the override
    if (await tryFont(font)) {
      _fontProbe = { ok: true, fontFile: font };
      if (font) log.info({ fontFile: font }, 'video: watermark font resolved');
      return _fontProbe;
    }
  }
  log.warn('video: drawtext unusable (no font found) — videos will be transcoded without a watermark');
  return (_fontProbe = { ok: false, fontFile: null });
}

/** For tests. */
export function _resetFontProbeCache(): void {
  _fontProbe = undefined;
}

/** The argv for the main transcode, exported so tests can assert it. */
export function buildTranscodeArgs(
  input: string, output: string, watermarkText: string | null, fontFile?: string | null,
): string[] {
  return [
    '-hide_banner', '-loglevel', 'error',
    '-y',
    '-i', input,
    '-vf', buildFilter(watermarkText, fontFile),
    '-c:v', 'libx264',
    '-preset', 'veryfast',
    '-b:v', VIDEO_BITRATE, '-maxrate', VIDEO_MAXRATE, '-bufsize', VIDEO_BUFSIZE,
    '-pix_fmt', 'yuv420p',        // widest player compatibility
    '-c:a', 'aac', '-b:a', AUDIO_BITRATE,
    '-map_metadata', '-1',        // strip GPS / device / creation metadata
    '-movflags', '+faststart',    // playable before fully downloaded
    output,
  ];
}

/** The argv for the poster frame. */
export function buildPosterArgs(
  input: string, poster: string, watermarkText: string | null, fontFile?: string | null,
): string[] {
  // `thumbnail` picks a representative frame rather than a fixed timestamp —
  // the old browser capture used 0.1s, which is very often black or a fade.
  return [
    '-hide_banner', '-loglevel', 'error',
    '-y',
    '-i', input,
    '-vf', `thumbnail,${buildFilter(watermarkText, fontFile)}`,
    '-frames:v', '1',
    '-map_metadata', '-1',
    poster,
  ];
}

/** Pull duration/width/height out of ffmpeg's stderr banner for the input. */
export function parseProbe(stderr: string): TranscodeResult {
  const out: TranscodeResult = { durationSeconds: null, width: null, height: null };
  const dur = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(stderr);
  if (dur) {
    out.durationSeconds = Math.round(
      Number(dur[1]) * 3600 + Number(dur[2]) * 60 + Number(dur[3]),
    );
  }
  // The first "1920x1080" in a Video: stream line.
  const dim = /Video:.*?[\s,](\d{2,5})x(\d{2,5})/.exec(stderr);
  if (dim) {
    out.width = Number(dim[1]);
    out.height = Number(dim[2]);
  }
  return out;
}

/** Probe the source for duration/dimensions (the client never sets these). */
async function probe(input: string): Promise<TranscodeResult> {
  const bin = ffmpegPath();
  if (!bin) return { durationSeconds: null, width: null, height: null };
  try {
    // ffmpeg with no output exits non-zero but prints the banner we want.
    await execFileAsync(bin, ['-hide_banner', '-i', input], { timeout: 30_000, maxBuffer: 8 * 1024 * 1024 });
    return { durationSeconds: null, width: null, height: null };
  } catch (err) {
    const stderr = String((err as { stderr?: string })?.stderr ?? '');
    return parseProbe(stderr);
  }
}

/**
 * Transcode `input` to `output` and cut `poster`. Throws on failure so the
 * caller can record the error and leave the original file untouched.
 */
export async function transcodeVideo(opts: {
  input: string;
  output: string;
  poster: string;
  watermarkText?: string | null;
}): Promise<TranscodeResult> {
  const bin = ffmpegPath();
  if (!bin) throw new Error('ffmpeg not available');
  let wm = opts.watermarkText ?? null;
  let fontFile: string | null = null;

  if (wm) {
    const font = await resolveFont();
    // Better an unmarked-but-normalised video than a failed job.
    if (!font.ok) wm = null;
    else fontFile = font.fontFile;
  }

  const meta = await probe(opts.input);

  await execFileAsync(bin, buildTranscodeArgs(opts.input, opts.output, wm, fontFile), {
    timeout: TRANSCODE_TIMEOUT_MS,
    maxBuffer: 8 * 1024 * 1024,
  });

  // A missing poster must not fail the job — the video is the payload.
  try {
    await execFileAsync(bin, buildPosterArgs(opts.input, opts.poster, wm, fontFile), {
      timeout: 60_000,
      maxBuffer: 8 * 1024 * 1024,
    });
  } catch (err) {
    log.warn({ err }, 'video: poster extraction failed (video still transcoded)');
  }

  // Report the OUTPUT dimensions, which are what's actually stored.
  const outMeta = await probe(opts.output);
  return {
    durationSeconds: outMeta.durationSeconds ?? meta.durationSeconds,
    width: outMeta.width ?? meta.width,
    height: outMeta.height ?? meta.height,
    watermarked: !!wm,
  };
}
