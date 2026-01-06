import { NextResponse } from 'next/server';
import duckdb from 'duckdb';

let dbInstance: duckdb.Database | null = null;

function getDuckDB(): Promise<duckdb.Database> {
  return new Promise((resolve, reject) => {
    if (dbInstance) {
      resolve(dbInstance);
      return;
    }

    const dbPath = '/Users/patyrocks/Documents/pyground/ETL/TMDB';
    console.log('Initializing DuckDB at:', dbPath);
    
    const db = new duckdb.Database(dbPath, (err) => {
      if (err) {
        console.error('Failed to open database:', err);
        reject(err);
      } else {
        console.log('Database opened successfully');
        dbInstance = db;
        resolve(db);
      }
    });
  });
}

export async function GET() {
  try {
    const db = await getDuckDB();
    
    return new Promise((resolve) => {
      db.all(
        `
        SELECT id, title, release_date, vote_average, popularity
        FROM movies
        WHERE release_date LIKE '2025%'
        ORDER BY vote_average DESC
        LIMIT 10;
        `,
        (err: Error | null, result: any[]) => {
          if (err) {
            console.error('Query error:', err);
            resolve(
              NextResponse.json(
                { error: err.message },
                { status: 500 }
              )
            );
          } else {
            console.log('Query success, rows:', result?.length || 0);
            // Serialize with BigInt support
            const jsonString = JSON.stringify(result, (_, value) =>
              typeof value === 'bigint' ? value.toString() : value
            );
            resolve(
              new Response(jsonString, {
                status: 200,
                headers: { 'Content-Type': 'application/json' },
              })
            );
          }
        }
      );
    });
  } catch (err) {
    console.error('Database initialization error:', err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Database error' },
      { status: 500 }
    );
  }
}