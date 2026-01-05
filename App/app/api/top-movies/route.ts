import { NextResponse } from 'next/server';
import { Pool } from 'pg';

declare global {
  var __pgPool: any;
}

const connString = process.env.MOTHERDUCK_DATABASE_URL || process.env.DATABASE_URL;

if (!global.__pgPool) {
  global.__pgPool = new Pool({
    connectionString: connString,
    max: 10,
    idleTimeoutMillis: 30000,
  });
}
const pool = global.__pgPool;

export async function GET() {
  if (!connString) {
    return NextResponse.json(
      { error: 'Missing database connection string' },
      { status: 500 }
    );
  }

  let client;
  try {
    client = await pool.connect();
    const result = await client.query(`
      SELECT id, title, release_date, vote_average, popularity
      FROM movies
      WHERE YEAR(CAST(release_date AS DATE)) = 2025
      ORDER BY vote_average DESC
      LIMIT 10;
    `);
    return NextResponse.json(result.rows);
  } catch (err) {
    console.error('Database error:', err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Database error' },
      { status: 500 }
    );
  } finally {
    if (client) client.release();
  }
}