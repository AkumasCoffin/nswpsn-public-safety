# NSW boundary overlays

Simplified GeoJSON used by the map's "Region" and "LGA" border toggles. Loaded
lazily (only when a toggle is first switched on) and drawn as thin outlines.

## Files

- `nsw-lga.geojson` — 132 NSW Local Government Areas. Property: `name` (LGA name).
- `nsw-regions.geojson` — the 10 NSW planning regions (Greater Sydney, Hunter,
  Central Coast, Illawarra-Shoalhaven, North Coast, New England North West,
  Central West and Orana, Riverina Murray, South East and Tablelands, Far West).
  Property: `name` (region name).

## Sources (CC BY 4.0)

- **LGA** — NSW Spatial Services, *NSW Administrative Boundaries Theme*
  (`LocalGovernmentArea`, layer 8), FeatureServer on portal.spatial.nsw.gov.au.
- **Regions** — NSW Dept of Planning, Housing and Infrastructure,
  *Planning Regional Growth Boundary* (Administrative_Boundary MapServer, layer 2),
  mapprod3.environment.nsw.gov.au.

## Regenerating

Both were pulled as GeoJSON (`outSR=4326`) and simplified with mapshaper to keep
the browser payload small:

```
mapshaper <raw>.geojson -rename-fields name=<srcField> -filter-fields name \
  -simplify 3% keep-shapes -o precision=0.0001 format=geojson <out>.geojson
```

Raw full-resolution downloads are ~67 MB (LGA) / ~28 MB (regions); simplified they
are ~1.2 MB / ~0.45 MB (≈305 KB / 123 KB gzipped).
