require("dotenv").config();

const { Pool } = require("pg");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_PAGE_SIZE = 1000;
const UPSERT_BATCH_SIZE = 250;

function sanitizeConnectionString(rawUrl) {
  if (!rawUrl || typeof rawUrl !== "string") return "";
  try {
    const parsed = new URL(rawUrl.trim());
    parsed.searchParams.delete("sslmode");
    return parsed.toString();
  } catch {
    return rawUrl.trim();
  }
}

function shouldUseSsl(host, rawUrl = "") {
  const lowerUrl = String(rawUrl || "").toLowerCase();
  if (lowerUrl.includes("sslmode=disable")) return false;
  if (lowerUrl.includes("sslmode=require") || lowerUrl.includes("sslmode=verify")) {
    return { rejectUnauthorized: false };
  }
  const cleanHost = String(host || "").trim().toLowerCase();
  if (
    !cleanHost ||
    cleanHost === "localhost" ||
    cleanHost === "127.0.0.1" ||
    cleanHost === "::1" ||
    cleanHost.endsWith(".railway.internal") ||
    cleanHost.endsWith(".internal")
  ) {
    return false;
  }
  return { rejectUnauthorized: false };
}

function resolveTargetPoolConfig() {
  const rawUrl = (
    process.env.INDEX_DATABASE_URL ||
    process.env.RAILWAY_DATABASE_URL ||
    process.env.DATABASE_PUBLIC_URL ||
    process.env.DATABASE_URL ||
    ""
  ).trim();

  if (rawUrl) {
    const u = new URL(rawUrl);
    return {
      hostLabel: `${u.hostname}${u.port ? `:${u.port}` : ""} / ${u.pathname.replace(/^\//, "")}`,
      poolConfig: {
        connectionString: sanitizeConnectionString(rawUrl),
        ssl: shouldUseSsl(u.hostname, rawUrl),
      },
    };
  }

  const host = String(process.env.PGHOST || "").trim();
  const user = String(process.env.PGUSER || process.env.POSTGRES_USER || "").trim();
  const password = String(process.env.PGPASSWORD || process.env.POSTGRES_PASSWORD || "").trim();
  const database = String(process.env.PGDATABASE || process.env.POSTGRES_DB || "").trim();
  const port = Number(process.env.PGPORT || 5432) || 5432;

  if (host && user && database) {
    return {
      hostLabel: `${host}:${port} / ${database}`,
      poolConfig: {
        host,
        port,
        user,
        password,
        database,
        ssl: shouldUseSsl(host),
      },
    };
  }

  throw new Error(
    "No target Railway Postgres configured. Set DATABASE_URL, INDEX_DATABASE_URL, or PGHOST/PGUSER/PGPASSWORD/PGDATABASE."
  );
}

async function createConnectedTargetPool() {
  const { hostLabel, poolConfig } = resolveTargetPoolConfig();
  console.log(`[migrator] Target Index PostgreSQL: ${hostLabel}`);

  let pool = new Pool({ ...poolConfig, max: 5, connectionTimeoutMillis: 15000 });
  try {
    await pool.query("SELECT 1");
    return pool;
  } catch (err) {
    const msg = String(err?.message || "").toLowerCase();
    if (msg.includes("does not support ssl") && poolConfig.ssl !== false) {
      await pool.end().catch(() => {});
      pool = new Pool({ ...poolConfig, ssl: false, max: 5, connectionTimeoutMillis: 15000 });
      await pool.query("SELECT 1");
      return pool;
    }
    await pool.end().catch(() => {});
    throw err;
  }
}

function isNotFoundValue(val) {
  if (!val) return true;
  const s = String(val).trim().toLowerCase();
  return s === "" || s === "not found" || s === "not available" || s === "n/a" || s === "none";
}

function normalizeCategories(value) {
  if (Array.isArray(value)) {
    return value.map((c) => String(c || "").trim()).filter((c) => c && c.toLowerCase() !== "lossless repack");
  }
  if (typeof value === "string") {
    return value
      .replace(/^\{|\}$/g, "")
      .split(",")
      .map((s) => s.replace(/^"|"$/g, "").trim())
      .filter((c) => c && c.toLowerCase() !== "lossless repack");
  }
  return [];
}

function normalizeScreenshotCount(row) {
  if (typeof row?.screenshot_count === "number" && !Number.isNaN(row.screenshot_count)) {
    return Math.max(0, Math.floor(row.screenshot_count));
  }
  if (Array.isArray(row?.screenshots)) return row.screenshots.filter(Boolean).length;
  return 0;
}

function toIndexRecord(row, sourceDb) {
  const hasDesc =
    typeof row?.has_description === "boolean"
      ? row.has_description
      : !isNotFoundValue(row?.description);

  return {
    id: String(row.id),
    title: String(row.title || "Untitled").trim(),
    slug: String(row.slug || "").trim().toLowerCase(),
    categories: normalizeCategories(row.categories),
    size_repack: row.size_repack ? String(row.size_repack).trim() : null,
    cover_url: row.cover_url ? String(row.cover_url).trim() : null,
    repacker: row.repacker ? String(row.repacker).trim() : null,
    featured: Boolean(row.featured),
    trending: Boolean(row.trending),
    is_published: row.is_published !== false,
    downloads_count: Number(row.downloads_count) || 0,
    has_description: Boolean(hasDesc),
    screenshot_count: normalizeScreenshotCount(row),
    source_db: sourceDb,
    created_at: row.created_at ? new Date(row.created_at).toISOString() : new Date().toISOString(),
  };
}

async function fetchKoyebGames() {
  const koyebUrl = String(process.env.KOYEB_DATABASE_URL || "").trim();
  if (!koyebUrl) return [];

  const pool = new Pool({
    connectionString: sanitizeConnectionString(koyebUrl),
    ssl: { rejectUnauthorized: false },
    max: 3,
    connectionTimeoutMillis: 12000,
  });

  try {
    const { rows } = await pool.query(`
      SELECT
        id, title, slug, categories, size_repack, cover_url, repacker,
        featured, trending, is_published, downloads_count, created_at,
        (description IS NOT NULL AND btrim(description) <> '' AND lower(btrim(description)) NOT IN ('not found','not available','n/a','none')) AS has_description,
        COALESCE(array_length(screenshots, 1), 0)::int AS screenshot_count
      FROM games
      ORDER BY created_at DESC, id ASC
    `);
    return (rows || []).map((r) => toIndexRecord(r, "postgres"));
  } finally {
    await pool.end().catch(() => {});
  }
}

async function fetchSupabaseGames() {
  const directUrl = (process.env.POSTGRES_URL_NON_POOLING || process.env.POSTGRES_URL || "").trim();
  if (directUrl) {
    const directPool = new Pool({
      connectionString: sanitizeConnectionString(directUrl),
      ssl: { rejectUnauthorized: false },
      max: 2,
      connectionTimeoutMillis: 12000,
    });
    try {
      const { rows } = await directPool.query(`
        SELECT
          id, title, slug, categories, size_repack, cover_url, repacker,
          featured, trending, is_published, downloads_count, created_at,
          (description IS NOT NULL AND btrim(description) <> '' AND lower(btrim(description)) NOT IN ('not found','not available','n/a','none')) AS has_description,
          COALESCE(array_length(screenshots, 1), 0)::int AS screenshot_count
        FROM games
        ORDER BY created_at DESC, id ASC
      `);
      return (rows || []).map((r) => toIndexRecord(r, "supabase"));
    } catch (err) {
      console.warn("[migrator] Direct SQL fallback to Supabase REST:", err.message);
    } finally {
      await directPool.end().catch(() => {});
    }
  }

  const supaUrl = process.env.SUPABASE_URL;
  const supaKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  if (!supaUrl || !supaKey) return [];

  const supabase = createClient(supaUrl, supaKey, { auth: { persistSession: false } });
  const allRows = [];
  let from = 0;

  while (true) {
    const to = from + SUPABASE_PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from("games")
      .select("id,title,slug,categories,size_repack,cover_url,repacker,featured,trending,is_published,downloads_count,created_at,description,screenshots")
      .order("created_at", { ascending: false })
      .order("id", { ascending: true })
      .range(from, to);

    if (error) throw new Error(`Supabase fetch failed: ${error.message}`);
    const batch = Array.isArray(data) ? data : [];
    allRows.push(...batch.map((r) => toIndexRecord(r, "supabase")));
    if (batch.length < SUPABASE_PAGE_SIZE) break;
    from += SUPABASE_PAGE_SIZE;
  }
  return allRows;
}

async function run() {
  const startedAt = Date.now();
  const targetPool = await createConnectedTargetPool();

  try {
    await targetPool.query(`
      CREATE TABLE IF NOT EXISTS games_index (
        id uuid PRIMARY KEY,
        title text NOT NULL,
        slug text UNIQUE NOT NULL,
        categories text[] NOT NULL DEFAULT '{}',
        size_repack text,
        cover_url text,
        repacker text,
        featured boolean NOT NULL DEFAULT false,
        trending boolean NOT NULL DEFAULT false,
        is_published boolean NOT NULL DEFAULT true,
        downloads_count integer NOT NULL DEFAULT 0,
        has_description boolean NOT NULL DEFAULT false,
        screenshot_count integer NOT NULL DEFAULT 0,
        source_db text NOT NULL DEFAULT 'postgres',
        created_at timestamptz NOT NULL DEFAULT now()
      );
      CREATE INDEX IF NOT EXISTS games_index_created_at_idx ON games_index (created_at DESC);
      CREATE INDEX IF NOT EXISTS games_index_downloads_idx ON games_index (downloads_count DESC);
      CREATE INDEX IF NOT EXISTS games_index_slug_idx ON games_index (lower(slug));
      CREATE INDEX IF NOT EXISTS games_index_published_idx ON games_index (is_published);
      CREATE INDEX IF NOT EXISTS games_index_trending_idx ON games_index (trending);
    `);

    console.log("[migrator] Fetching lightweight game index from Koyeb & Supabase...");
    const [pgRows, supaRows] = await Promise.all([fetchKoyebGames(), fetchSupabaseGames()]);

    const mergedMap = new Map();
    const seenSlugs = new Set();

    for (const row of pgRows) {
      if (!row.id || !row.slug) continue;
      mergedMap.set(row.id, row);
      seenSlugs.add(row.slug.toLowerCase());
    }

    let supaKept = 0;
    for (const row of supaRows) {
      if (!row.id || !row.slug) continue;
      const slugKey = row.slug.toLowerCase();
      if (mergedMap.has(row.id) || seenSlugs.has(slugKey)) continue;
      mergedMap.set(row.id, row);
      seenSlugs.add(slugKey);
      supaKept += 1;
    }

    const allRows = Array.from(mergedMap.values());
    console.log(`[migrator] Writing ${allRows.length} rows (PG: ${pgRows.length}, Supabase: ${supaKept})...`);

    const client = await targetPool.connect();
    try {
      await client.query("BEGIN");
      for (let i = 0; i < allRows.length; i += UPSERT_BATCH_SIZE) {
        const batch = allRows.slice(i, i + UPSERT_BATCH_SIZE);
        const values = [];
        const placeholders = [];

        batch.forEach((rec, idx) => {
          const base = idx * 15;
          placeholders.push(
            `($${base + 1}::uuid, $${base + 2}, $${base + 3}, $${base + 4}::text[], $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}::boolean, $${base + 9}::boolean, $${base + 10}::boolean, $${base + 11}::int, $${base + 12}::boolean, $${base + 13}::int, $${base + 14}, $${base + 15}::timestamptz)`
          );
          values.push(
            rec.id, rec.title, rec.slug, rec.categories, rec.size_repack,
            rec.cover_url, rec.repacker, rec.featured, rec.trending, rec.is_published,
            rec.downloads_count, rec.has_description, rec.screenshot_count,
            rec.source_db, rec.created_at
          );
        });

        await client.query(
          `DELETE FROM games_index WHERE lower(slug) = ANY($1::text[]) AND id <> ALL($2::uuid[])`,
          [batch.map((b) => b.slug.toLowerCase()), batch.map((b) => b.id)]
        );

        await client.query(
          `INSERT INTO games_index (
            id, title, slug, categories, size_repack, cover_url, repacker,
            featured, trending, is_published, downloads_count,
            has_description, screenshot_count, source_db, created_at
          ) VALUES ${placeholders.join(", ")}
          ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            categories = EXCLUDED.categories,
            size_repack = EXCLUDED.size_repack,
            cover_url = EXCLUDED.cover_url,
            repacker = EXCLUDED.repacker,
            featured = EXCLUDED.featured,
            trending = EXCLUDED.trending,
            is_published = EXCLUDED.is_published,
            downloads_count = GREATEST(games_index.downloads_count, EXCLUDED.downloads_count),
            has_description = EXCLUDED.has_description,
            screenshot_count = EXCLUDED.screenshot_count,
            source_db = EXCLUDED.source_db,
            created_at = EXCLUDED.created_at`,
          values
        );
      }

      if (allRows.length > 0) {
        await client.query(`DELETE FROM games_index WHERE id <> ALL($1::uuid[])`, [
          allRows.map((r) => r.id),
        ]);
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }

    console.log(`[migrator] Completed transfer of ${allRows.length} games in ${Date.now() - startedAt}ms.`);
  } finally {
    await targetPool.end().catch(() => {});
  }
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("[migrator] FAILED:", err);
    process.exit(1);
  });
