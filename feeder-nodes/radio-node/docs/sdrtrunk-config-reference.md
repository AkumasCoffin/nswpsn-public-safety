# SDR-Trunk PlaylistV2 config reference (verified against real playlists)

Source: SDR-Trunk fork `feature/node-control`. Reader uses
`FAIL_ON_UNKNOWN_PROPERTIES=false` and child-element order is NOT enforced on
read — so the renderer only needs to be *correct*, not byte-identical. `type=` on
decode/source configs is a **plain** attribute (NOT `xsi:type`).

## `<channel>` skeleton
Attributes (all optional): `system`, `site`, `name`, `enabled` (= **auto-start**
flag, not processing on/off), `order` (auto-start order).
Child element order (match it, not enforced): `alias_list_name` (CHILD element,
always emitted, empty allowed) → `event_log_configuration` → `source_configuration`
→ `aux_decode_configuration` → `decode_configuration` → `record_configuration`.
The empty ones are self-closing (`<event_log_configuration/>` etc.).

```xml
<channel system="NSW PSN" name="Control 1" enabled="true" order="1" site="Site 1">
  <alias_list_name>NSW PSN</alias_list_name>
  <event_log_configuration/>
  <source_configuration type="sourceConfigTuner" frequency="419587500" source_type="TUNER"/>
  <aux_decode_configuration/>
  <decode_configuration .../>
  <record_configuration/>
</channel>
```

## source_configuration
Single: `<source_configuration type="sourceConfigTuner" frequency="<Hz>" source_type="TUNER"/>`
(optional `preferred_tuner="<name>"`).
Multiple (rolling control): `type="sourceConfigTunerMultipleFrequency"
source_type="TUNER_MULTIPLE_FREQUENCIES"` with repeated `<frequency>Hz</frequency>`
children + optional `frequency_rotation_delay="<ms>"`.

## decode_configuration — per decoder (type value + fields)

### p25p1 — `decodeConfigP25Phase1`
`<decode_configuration type="decodeConfigP25Phase1" modulation="C4FM" ignore_data_calls="false" traffic_channel_pool_size="20"/>`
- `modulation`: `C4FM` (Normal) | `CQPSK` (Simulcast/LSM). default C4FM.
- `ignore_data_calls`: bool, default false.
- `traffic_channel_pool_size`: int 0–50, default 20.
(P25P1 is also used for P25 systems with FDMA control that follow Phase-2 traffic.)

### p25p2 — `decodeConfigP25Phase2`
```xml
<decode_configuration type="decodeConfigP25Phase2" auto_detect_scramble_parameters="false" ignore_data_calls="false" traffic_channel_pool_size="20">
  <scramble_parameters wacn="12345" system="291" nac="659"/>
</decode_configuration>
```
- `auto_detect_scramble_parameters`: bool, default false (true if scramble omitted).
- `scramble_parameters` CHILD (attrs `wacn`,`system`,`nac` ints) — only when manually set; auto-detected from control channel otherwise.
- `ignore_data_calls` bool; `traffic_channel_pool_size` int 0–50 def 20.

### dmr — `decodeConfigDMR`
```xml
<decode_configuration type="decodeConfigDMR" ignore_crc="false" use_compressed_talkgroups="false" ignore_data_calls="true" traffic_channel_pool_size="20">
  <timeslot lsn="187" downlink="166408000" uplink="0"/>
</decode_configuration>
```
- `ignore_crc` bool def false; `use_compressed_talkgroups` bool def false (Hytera).
- `ignore_data_calls` bool default **true**; `traffic_channel_pool_size` int 0–50 def 20.
- `timeslot` CHILD element(s): `lsn` (int, carries the LCN), `downlink` (long Hz), `uplink` (long Hz, def 0).

### nbfm — `decodeConfigNBFM` (⚠ NEW noise-squelch schema on this branch)
`<decode_configuration type="decodeConfigNBFM" bandwidth="BW_12_5" talkgroup="1" audioFilter="true" squelchNoiseOpenThreshold="0.1" squelchNoiseCloseThreshold="0.19" squelchHysteresisOpenThreshold="4" squelchHysteresisCloseThreshold="6"/>`
- User-facing: `bandwidth` (`BW_7_5`|`BW_12_5`|`BW_25_0`, def BW_12_5), `talkgroup` (1–65535, def 1), `audioFilter` (bool, def true).
- The 4 squelch* fields are NOT in the GUI — emit defaults (0.1 / 0.19 / 4 / 6). (Old `autoTrack`/`squelch` attrs are replaced; reader ignores them if present.)

### am — `decodeConfigAM`
`<decode_configuration type="decodeConfigAM" bandwidth="BW_15_0" talkgroup="1" autoTrack="true" squelch="-78"/>`
- `bandwidth`: `BW_3_0`|`BW_5_0`|`BW_8_33`|`BW_15_0`|`BW_25_0`, def BW_15_0.
- `talkgroup` int 1–65535 def 1; `squelch` int dB def -78; `autoTrack` bool def true.

## decoderConfig JSON contract (backend config_override.channels[i].decoderConfig)
Discriminated by `channel.decoder`. All fields optional (renderer fills SDR-Trunk defaults):
- p25p1: `{ modulation, ignoreDataCalls, trafficPoolSize }`
- p25p2: `{ ignoreDataCalls, trafficPoolSize, autoDetectScramble, scramble:{wacn,system,nac} }`
- dmr:   `{ ignoreDataCalls, ignoreCrc, useCompressedTalkgroups, trafficPoolSize, timeslots:[{lcn,downlink,uplink}] }`
- nbfm:  `{ bandwidth, talkgroup, audioFilter }`
- am:    `{ bandwidth, talkgroup, squelch, autoTrack }`

## Tuner config (per device — for the tuner panel; not yet implemented)
Common (all): `frequency` (Hz), `frequency_correction` (PPM ±1000 step 0.1 def 0),
`auto_ppm_correction_enabled` (def true), `min_frequency`/`max_frequency`.
All RTL: `sample_rate` (17 values 0.230–2.880 MHz, def 2.4M) + `bias_t`.
Gain models are DEVICE-SPECIFIC:
- RTL R820T/R828D: `master_gain`(30 steps)/`mixer_gain`(16)/`lna_gain`(16)/`vga_gain`(16).
- RTL E4000: `master_gain`(MANUAL+14)/`mixer_gain`(2)/`lna_gain`(13)/`if_gain`(55). mixer/lna/if only when master=MANUAL.
- RTL FC0013: `lna_gain`(24) + `agc` toggle.
- Airspy / HydraSDR: gain mode LINEARITY|SENSITIVITY|CUSTOM + master(1–22)/if(0–15)/mixer(0–15)/lna(0–14) sliders + `mixer_agc`/`lna_agc` (HydraSDR +`bias_t`). sample_rate device-reported, def 10M.
- Airspy HF+: `attenuation`(0..48 dB step 6) + `agc`/`lna` toggles.
- HackRF: `sample_rate`(1.75–20M) + `lna_gain`(0..40 step 8) + `vga_gain`(0..62 step 2) + `amplifier_enabled`.
- SDRplay RSP: `sampleRate` + `lna`(state idx) + `gr`(20–59 dB def 50) + `agcMode`(DISABLE|ENABLE); variants add bias_t/rf_notch/dab_notch/antenna/hdr_mode.
- FCD Pro/Pro+: gain dropdowns/toggles, NO sample_rate (fixed-rate).

Tuner live-control today is via the control-server API (gain/ppm/samplerate/autoppm);
device-specific gain axes (lnaGain/vgaGain/etc.) are accepted by the fork's
`/tuners/{id}/gain` endpoint.
