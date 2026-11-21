try { require("dotenv").config(); } catch {}
const { Pool } = require("pg");
const { createClient } = require("@supabase/supabase-js");

const FULL_BATCH_SIZE = 100;
const INDEX_BATCH_SIZE = 250;
const SUPABASE_PAGE_SIZE = 500;

function log(msg) {
  console.log(`[migrator ${new Date().toISOString()}] ${msg}`);
}

function parseValidPgUrl(rawUrl) {
  const trimmed = String(rawUrl || "").trim();
  if (!trimmed) return null;
  try {
    const parsed = new URL(trimmed);
    if (!parsed.hostname) return null;
    if (parsed.protocol !== "postgres:" && parsed.protocol !== "postgresql:") {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function isInternalHost(host) {
  const h = String(host || "").toLowerCase();
  return (
    h === "localhost" ||
    h === "127.0.0.1" ||
    h.endsWith(".railway.internal") ||
    h.endsWith(".internal")
  );
}

function isSslHandshakeError(err) {
  const msg = String(err?.message || "").toLowerCase();
  return (
    msg.includes("does not support ssl") ||
    msg.includes("econnreset") ||
    msg.includes("connection terminated") ||
    (msg.includes("no pg_hba.conf entry") && msg.includes("ssl off"))
  );
}

async function connectPgWithRetry(label, rawUrl, { max = 5, attempts = 5 } = {}) {
  const u = parseValidPgUrl(rawUrl);
  if (!u) throw new Error(`Invalid or missing PostgreSQL URL for ${label}.`);

  const cleanUrl = new URL(u.toString());
  cleanUrl.searchParams.delete("sslmode");

  const baseConfig = {
    connectionString: cleanUrl.toString(),
    ssl: isInternalHost(u.hostname) ? false : { rejectUnauthorized: false },
    max,
    connectionTimeoutMillis: 15000,
  };

  let lastErr = null;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    log(`[attempt ${attempt}/${attempts}] Connecting to ${label} (${u.hostname}:${u.port || 5432})...`);
    let pool = new Pool(baseConfig);
    try {
      await pool.query("SELECT 1");
      log(`Connected to ${label}.`);
      return pool;
    } catch (err) {
      lastErr = err;
      if (isSslHandshakeError(err) && baseConfig.ssl !== false) {
        await pool.end().catch(() => {});
        pool = new Pool({ ...baseConfig, ssl: false });
        try {
          await pool.query("SELECT 1");
          log(`Connected to ${label} without SSL.`);
          return pool;
        } catch (retryErr) {
          lastErr = retryErr;
        }
      }
      log(`${label} attempt ${attempt} failed: ${lastErr.message}`);
      await pool.end().catch(() => {});
    }
    if (attempt < attempts) {
      await new Promise((r) => setTimeout(r, 3000));
    }
  }

  throw lastErr || new Error(`Could not connect to ${label}.`);
}

function normalizeArrayField(value, removeLossless = false) {
  let list = [];
  if (Array.isArray(value)) {
    list = value.map((c) => String(c || "").trim()).filter(Boolean);
  } else if (typeof value === "string") {
    list = value
      .replace(/^\{|\}$/g, "")
      .split(",")
      .map((s) => s.replace(/^"|"$/g, "").trim())
      .filter(Boolean);
  }
  if (removeLossless) {
    list = list.filter((c) => c.toLowerCase() !== "lossless repack");
  }
  return list;
}

function isNotFoundValue(val) {
  if (!val) return true;
  const s = String(val).trim().toLowerCase();
  return s === "" || s === "not found" || s === "not available" || s === "n/a" || s === "none";
}

function toFullGameRecord(row) {
  return {
    id: String(row.id),
    title: String(row.title || "Untitled").trim(),
    slug: String(row.slug || "").trim().toLowerCase(),
    categories: normalizeArrayField(row.categories, true),
    version: row.version ? String(row.version).trim() : null,
    size_repack: row.size_repack ? String(row.size_repack).trim() : null,
    description: row.description ? String(row.description).trim() : null,
    cover_url: row.cover_url ? String(row.cover_url).trim() : null,
    screenshots: normalizeArrayField(row.screenshots, false),
    system_requirements: row.system_requirements ? String(row.system_requirements).trim() : null,
    repacker: row.repacker ? String(row.repacker).trim() : null,
    download_url: String(row.download_url || "").trim(),
    featured: Boolean(row.featured),
    trending: Boolean(row.trending),
    is_published: row.is_published !== false,
    downloads_count: Number(row.downloads_count) || 0,
    created_at: row.created_at ? new Date(row.created_at).toISOString() : new Date().toISOString(),
  };
}

async function fetchAllFullGamesFromSupabase() {
  const directUrl = String(
    process.env.POSTGRES_URL_NON_POOLING || process.env.POSTGRES_URL || ""
  ).trim();

  if (directUrl && parseValidPgUrl(directUrl)) {
    log("Fetching full games from Supabase via direct SQL...");
    let pool = null;
    try {
      pool = await connectPgWithRetry("Supabase SQL", directUrl, { max: 2, attempts: 2 });
      const { rows } = await pool.query(`
        SELECT
          id, title, slug, categories, version, size_repack, description,
          cover_url, screenshots, system_requirements, repacker, download_url,
          featured, trending, is_published, downloads_count, created_at
        FROM games
        ORDER BY created_at DESC, id ASC
      `);
      log(`Fetched ${rows.length} full games from Supabase SQL.`);
      return rows.map(toFullGameRecord);
    } catch (err) {
      log(`Direct Supabase SQL unavailable (${err.message}), falling back to REST API...`);
    } finally {
      if (pool) await pool.end().catch(() => {});
    }
  }

  const supaUrl = String(process.env.SUPABASE_URL || "").trim();
  const supaKey = String(
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
      process.env.SUPABASE_SERVICE_KEY ||
      process.env.SUPABASE_ANON_KEY ||
      ""
  ).trim();

  if (!supaUrl || !supaKey) {
    throw new Error(
      "Missing Supabase credentials. Set POSTGRES_URL_NON_POOLING or SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY."
    );
  }

  log("Fetching full games from Supabase REST API...");
  const supabase = createClient(supaUrl, supaKey, { auth: { persistSession: false } });
  const allRows = [];
  let from = 0;

  while (true) {
    const to = from + SUPABASE_PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from("games")
      .select(
        "id,title,slug,categories,version,size_repack,description,cover_url,screenshots,system_requirements,repacker,download_url,featured,trending,is_published,downloads_count,created_at"
      )
      .order("created_at", { ascending: false })
      .order("id", { ascending: true })
      .range(from, to);

    if (error) throw new Error(`Supabase fetch failed: ${error.message}`);
    const batch = Array.isArray(data) ? data : [];
    allRows.push(...batch.map(toFullGameRecord));
    log(`Supabase REST fetched ${allRows.length} full games...`);
    if (batch.length < SUPABASE_PAGE_SIZE) break;
    from += SUPABASE_PAGE_SIZE;
  }

  return allRows;
}

const KOYEB_SCHEMA_SQL = `
  CREATE EXTENSION IF NOT EXISTS "pgcrypto";
  CREATE TABLE IF NOT EXISTS games (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    title text NOT NULL,
    slug text UNIQUE NOT NULL,
    categories text[] NOT NULL DEFAULT '{}',
    version text,
    size_repack text,
    description text,
    cover_url text,
    screenshots text[] NOT NULL DEFAULT '{}',
    system_requirements text,
    repacker text,
    download_url text NOT NULL,
    featured boolean NOT NULL DEFAULT false,
    trending boolean NOT NULL DEFAULT false,
    is_published boolean NOT NULL DEFAULT true,
    downloads_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS games_created_at_idx ON games (created_at DESC);
  CREATE INDEX IF NOT EXISTS games_downloads_idx ON games (downloads_count DESC);
  CREATE INDEX IF NOT EXISTS games_slug_idx ON games (lower(slug));
`;

const INDEX_SCHEMA_SQL = `
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
`;

async function writeFullGamesToKoyeb(koyebPool, games) {
  await koyebPool.query(KOYEB_SCHEMA_SQL);
  log(`Writing ${games.length} full games to Koyeb PostgreSQL...`);

  const client = await koyebPool.connect();
  try {
    await client.query("BEGIN");

    for (let i = 0; i < games.length; i += FULL_BATCH_SIZE) {
      const batch = games.slice(i, i + FULL_BATCH_SIZE);
      const values = [];
      const placeholders = [];

      batch.forEach((g, idx) => {
        const b = idx * 17;
        placeholders.push(
          `($${b + 1}::uuid, $${b + 2}, $${b + 3}, $${b + 4}::text[], $${b + 5}, $${b + 6}, $${b + 7}, $${b + 8}, $${b + 9}::text[], $${b + 10}, $${b + 11}, $${b + 12}, $${b + 13}::boolean, $${b + 14}::boolean, $${b + 15}::boolean, $${b + 16}::int, $${b + 17}::timestamptz)`
        );
        values.push(
          g.id,
          g.title,
          g.slug,
          g.categories,
          g.version,
          g.size_repack,
          g.description,
          g.cover_url,
          g.screenshots,
          g.system_requirements,
          g.repacker,
          g.download_url,
          g.featured,
          g.trending,
          g.is_published,
          g.downloads_count,
          g.created_at
        );
      });

      await client.query(
        `DELETE FROM games WHERE lower(slug) = ANY($1::text[]) AND id <> ALL($2::uuid[])`,
        [batch.map((g) => g.slug.toLowerCase()), batch.map((g) => g.id)]
      );

      await client.query(
        `INSERT INTO games (
          id, title, slug, categories, version, size_repack, description,
          cover_url, screenshots, system_requirements, repacker, download_url,
          featured, trending, is_published, downloads_count, created_at
        ) VALUES ${placeholders.join(", ")}
        ON CONFLICT (id) DO UPDATE SET
          title = EXCLUDED.title,
          slug = EXCLUDED.slug,
          categories = EXCLUDED.categories,
          version = COALESCE(EXCLUDED.version, games.version),
          size_repack = EXCLUDED.size_repack,
          description = EXCLUDED.description,
          cover_url = EXCLUDED.cover_url,
          screenshots = EXCLUDED.screenshots,
          system_requirements = EXCLUDED.system_requirements,
          repacker = EXCLUDED.repacker,
          download_url = EXCLUDED.download_url,
          featured = EXCLUDED.featured,
          trending = EXCLUDED.trending,
          is_published = EXCLUDED.is_published,
          downloads_count = GREATEST(games.downloads_count, EXCLUDED.downloads_count),
          created_at = EXCLUDED.created_at`,
        values
      );

      if ((i + batch.length) % 1000 === 0 || i + batch.length === games.length) {
        log(`Koyeb progress: ${i + batch.length}/${games.length} full games written...`);
      }
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function syncGamesIndex(indexPool, games) {
  await indexPool.query(INDEX_SCHEMA_SQL);
  log(`Syncing ${games.length} lightweight index rows to Index PostgreSQL...`);

  const client = await indexPool.connect();
  try {
    await client.query("BEGIN");

    for (let i = 0; i < games.length; i += INDEX_BATCH_SIZE) {
      const batch = games.slice(i, i + INDEX_BATCH_SIZE);
      const values = [];
      const placeholders = [];

      batch.forEach((g, idx) => {
        const b = idx * 15;
        placeholders.push(
          `($${b + 1}::uuid, $${b + 2}, $${b + 3}, $${b + 4}::text[], $${b + 5}, $${b + 6}, $${b + 7}, $${b + 8}::boolean, $${b + 9}::boolean, $${b + 10}::boolean, $${b + 11}::int, $${b + 12}::boolean, $${b + 13}::int, $${b + 14}, $${b + 15}::timestamptz)`
        );
        values.push(
          g.id,
          g.title,
          g.slug,
          g.categories,
          g.size_repack,
          g.cover_url,
          g.repacker,
          g.featured,
          g.trending,
          g.is_published,
          g.downloads_count,
          !isNotFoundValue(g.description),
          Array.isArray(g.screenshots) ? g.screenshots.filter(Boolean).length : 0,
          "postgres",
          g.created_at
        );
      });

      await client.query(
        `DELETE FROM games_index WHERE lower(slug) = ANY($1::text[]) AND id <> ALL($2::uuid[])`,
        [batch.map((g) => g.slug.toLowerCase()), batch.map((g) => g.id)]
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

    const allIds = games.map((g) => g.id);
    if (allIds.length > 0) {
      await client.query(`DELETE FROM games_index WHERE id <> ALL($1::uuid[])`, [allIds]);
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function run() {
  const startedAt = Date.now();
  log("Starting Repack X Full Migrator (Supabase -> Koyeb PostgreSQL)...");

  const koyebUrl = String(process.env.KOYEB_DATABASE_URL || "").trim();
  if (!koyebUrl) {
    throw new Error("KOYEB_DATABASE_URL is required.");
  }

  const koyebPool = await connectPgWithRetry("Koyeb PostgreSQL", koyebUrl, { max: 5, attempts: 3 });

  try {
    const rawGames = await fetchAllFullGamesFromSupabase();
    const dedupMap = new Map();
    const seenSlugs = new Set();

    for (const g of rawGames) {
      if (!g.id || !g.slug) continue;
      const slugKey = g.slug.toLowerCase();
      if (dedupMap.has(g.id) || seenSlugs.has(slugKey)) continue;
      dedupMap.set(g.id, g);
      seenSlugs.add(slugKey);
    }

    const uniqueGames = Array.from(dedupMap.values());
    await writeFullGamesToKoyeb(koyebPool, uniqueGames);
    log(`Koyeb migration complete (${uniqueGames.length} full games).`);

    const indexUrl = String(
      process.env.INDEX_DATABASE_URL ||
        process.env.DATABASE_PUBLIC_URL ||
        process.env.RAILWAY_DATABASE_URL ||
        ""
    ).trim();

    if (indexUrl && parseValidPgUrl(indexUrl)) {
      const indexPool = await connectPgWithRetry("Index PostgreSQL", indexUrl, { max: 5, attempts: 5 });
      try {
        await syncGamesIndex(indexPool, uniqueGames);
        log("Index PostgreSQL sync complete.");
      } finally {
        await indexPool.end().catch(() => {});
      }
    } else {
      log("INDEX_DATABASE_URL not set — skipped Index DB sync (you can click Transfer games in Admin Dashboard).");
    }

    log(`ALL DONE in ${Date.now() - startedAt}ms.`);
  } finally {
    await koyebPool.end().catch(() => {});
  }

  process.exit(0);
}

run().catch((err) => {
  console.error("[migrator] FAILED:", err);
  process.exit(1);
});
