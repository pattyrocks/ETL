import { NextResponse } from 'next/server';
import duckdb from 'duckdb';
import path from 'path';

declare global {
  var __duckdbInstance: duckdb.Database | undefined;
}

function getDuckDB(): duckdb.Database {
  if (!global.__duckdbInstance) {
    const dbPath = path.join(process.cwd(), '../../TMDB');
    global.__duckdbInstance = new duckdb.Database(dbPath);
  }
  return global.__duckdbInstance;
}

export async function GET() {
  return new Promise((resolve) => {
    try {
      const db = getDuckDB();
      const conn = db.connect();

      conn.all(
        `
        SELECT id, title, release_date, vote_average, popularity
        FROM movies
        WHERE YEAR(CAST(release_date AS DATE)) = 2025
        ORDER BY vote_average DESC
        LIMIT 10;
        `,
        (err: Error | null, result: any[]) => {
          if (err) {
            console.error('Database error:', err);
            resolve(
              NextResponse.json(
                { error: err.message },
                { status: 500 }
              )
            );
          } else {
            resolve(NextResponse.json(result));
          }
        }
      );
    } catch (err) {
      console.error('Database error:', err);
      resolve(
        NextResponse.json(
          { error: err instanceof Error ? err.message : 'Database error' },
          { status: 500 }
        )
      );
    }
  });
}