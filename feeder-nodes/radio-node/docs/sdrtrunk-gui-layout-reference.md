# SDR-Trunk GUI layout reference (for the node-panel rebuild)

Verified from D:\working-dir\sdrtrunk source. Use to match Tuners / Channels /
Events / Spectrum exactly.

## Now Playing (channel activity) table — ChannelMetadataModel
Columns in order: **Status | Decoder | From | Alias | To | Alias | Channel |
Frequency | Channel Name**. (Two "Alias" headers = from-alias, to-alias.)
Frequency = `#.00000` MHz. Empty id = `-----`.
Status colours (bg/fg): ACTIVE cyan/blue, CALL blue/yellow, CONTROL orange/blue,
DATA green/blue, ENCRYPTED magenta/white, FADE ltgray/dkgray, IDLE white/dkgray,
RESET pink/yellow, TEARDOWN dkgray/white.

## Events tab — DecodeEventModel (10 cols)
**Time | Duration | Event | From | Alias | To | Alias | Channel | Frequency |
Details**. Duration = `0.0` seconds (blank if 0). Frequency = `0.00000` MHz.
Top filter bar "Event Filter Editor" (filters: Decoded Call, Call Encrypted,
Command, Data, Registration, All Other). Sortable, center-aligned.
Messages tab (separate): **Time | Protocol | Timeslot | Message** (time
`yyyy:MM:dd HH:mm:ss`).

## Tuner view — DiscoveredTunerModel
Left table cols: **Status | Class | Type | Frequency | Channels**. Frequency
`0.00000 MHz`; Channels = `N (LOCKED|UNLOCKED)`; Status ERROR=red, DISABLED=grey.
Right = per-tuner editor: Tuner ID, status + locked note, button row
(Enable/Disable, View Spectrum, New Spectrum, Restart, Record), frequency control
+ PPM spinner (0.1 step) + Auto-PPM checkbox + measured-PPM label + min/max freq +
Reset, then device gain/sample-rate controls. Layout is table-left / editor-right.

## Spectral display / waterfall — SpectralDisplayPanel
Vertical split ~50/50: TOP = FFT line over an overlay (freq grid/labels/cursor);
BOTTOM = waterfall.
- FFT trace: filled area, vertical gradient (translucent white at mid-height →
  translucent green at bottom) + outline line; bottom 20px reserved for the freq
  axis with a baseline. NO numeric dB axis; full height ≈ 90 dB, 0 dB (full-scale)
  at top.
- Freq axis: labels centered along the BOTTOM, MHz value only (`0.0`–`0.00000`
  decimals by zoom, e.g. `419.5875`), vertical label lines (ltgray) + major(9px)/
  minor(3px) ticks. Cursor = orange vertical line + `000.00000` MHz readout.
- Waterfall colour map (256-idx, AUTO-scaled to each frame's mean, NOT a fixed dB
  window): 0-15 dark blue (0,0,127); 16-31 blue (0,0,191); 32-59 ramp toward
  yellow (r+9,g+9,b-6/step); 60-187 (255, g 255→ down 2/step, 0) yellow→orange→
  red; 188-255 red. To match: normalize each FFT frame to its own mean, map to
  this ramp. Default FFT 4096 bins, 4-frame averaging.
- Controls are a right-click menu (no toolbar): Pause (waterfall), FFT Width
  {512,1024,2048,4096,8192,16384,32768}, Frame Rate {14,16,18,20,25,30,40,50},
  Window type, Averaging slider 1-20 (def 4), Smoothing, Zoom 1x-64x (2^0..6),
  colour pickers.

## Layout note for the web panel
Pin the spectrum to a FIXED region at the top (its own height) and let the event
log / tables scroll in their own panes — the spectrum must never be squashed by
log/table growth.
