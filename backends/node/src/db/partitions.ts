/**
 * Range partitions whose upper bound has passed a cutoff.
 *
 * Shared by the two retention jobs that drop whole partitions instead of
 * deleting rows: the archive_* tables (services/cleanup.ts, monthly) and
 * node_radio_events (services/nodeEventsPruner.ts, daily). Postgres keeps a
 * partition's bound in pg_class.relpartbound; pg_get_expr prints it as
 * "FOR VALUES FROM (...) TO ('<timestamptz>')", and the upper bound is what
 * decides whether every row in it is old enough. A MINVALUE lower bound
 * matches too — the regex only reads the TO side.
 */
export interface PartitionRow {
  parent: string;
  partition: string;
  range_to: string;
  row_count: string;
}

type Queryable = {
  query: (sql: string, params?: unknown[]) => Promise<{ rows: PartitionRow[] }>;
};

export async function partitionsEndingBy(
  client: Queryable,
  parents: readonly string[],
  cutoffIso: string,
): Promise<PartitionRow[]> {
  const sql = `
    WITH parts AS (
      SELECT
        parent.relname::text AS parent,
        child.relname::text AS partition,
        pg_get_expr(child.relpartbound, child.oid) AS bound
      FROM pg_inherits
      JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
      JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid
      WHERE parent.relname = ANY($1::text[])
    )
    SELECT
      parent,
      partition,
      -- bound is "FOR VALUES FROM ('YYYY-MM-DD ...') TO ('YYYY-MM-DD ...')".
      -- Pull the second timestamp.
      regexp_replace(bound, $2, '\\1') AS range_to,
      0::text AS row_count
    FROM parts
    WHERE bound LIKE 'FOR VALUES FROM%'
      AND regexp_replace(bound, $2, '\\1')::timestamptz <= $3::timestamptz
    ORDER BY 3
  `;
  const re =
    "FOR VALUES FROM \\(.+\\) TO \\('([^']+)'\\)";
  const r = await client.query(sql, [[...parents], re, cutoffIso]);
  return r.rows;
}
