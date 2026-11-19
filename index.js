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

function resolveTargetPoolConfig() {
  const candidates = [
    ["INDEX_DATABASE_URL", process.env.INDEX_DATABASE_URL],
    ["RAILWAY_DATABASE_URL", process.env.RAILWAY_DATABASE_URL],
    ["DATABASE_PUBLIC_URL", process.env.DATABASE_PUBLIC_URL],
    ["DATABASE_URL", process.env.DATABASE_URL],
  ];

  for (const [label, raw] of candidates) {
    const u = parseValidPgUrl(raw);
    if (!u) continue;

    const cleanUrl = new URL(u.toString());
    cleanUrl.searchParams.delete("sslmode");

    const host = (u.hostname || "").toLowerCase();
    const isInternal =
      host === "localhost" ||
      host === "127.0.0.1" ||
      host.endsWith(".railway.internal") ||
      host.endsWith(".internal");

    return {
      label: `${label} (${u.hostname}:${u.port || 5432})`,
      poolConfig: {
        connectionString: cleanUrl.toString(),
        ssl: isInternal ? false : { rejectUnauthorized: false },
      },
    };
  }

  const host = String(
    process.env.PGHOST ||
      process.env.RAILWAY_PRIVATE_DOMAIN ||
      process.env.RAILWAY_TCP_PROXY_DOMAIN ||
      ""
  ).trim();
  const user = String(process.env.PGUSER || process.env.POSTGRES_USER || "postgres").trim();
  const password = String(process.env.PGPASSWORD || process.env.POSTGRES_PASSWORD || "").trim();
  const database = String(process.env.PGDATABASE || process.env.POSTGRES_DB || "railway").trim();
  const port = Number(process.env.PGPORT || process.env.RAILWAY_TCP_PROXY_PORT || 5432) || 5432;

  if (host && user && database) {
    const isInternal =
      host.toLowerCase() === "localhost" ||
      host.toLowerCase() === "127.0.0.1" ||
      host.toLowerCase().endsWith(".railway.internal") ||
      host.toLowerCase().endsWith(".internal");

    return {
      label: `PG_VARS (${host}:${port}/${database})`,
      poolConfig: {
        host,
        port,
        user,
        password,
        database,
        ssl: isInternal ? false : { rejectUnauthorized: false },
      },
    };
  }

  throw new Error(
    "No valid target PostgreSQL host found. Set DATABASE_PUBLIC_URL, INDEX_DATABASE_URL, or PGHOST/PGUSER/PGPASSWORD/PGDATABASE."
  );
}
