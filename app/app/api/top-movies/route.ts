import { NextResponse } from 'next/server';
import * as duckdb from '@duckdb/duckdb-wasm';

let dbInstance: duckdb.AsyncDuckDB | null = null;

async function getDuckDB(): Promise<duckdb.AsyncDuckDB> {
  if (dbInstance) {
    return dbInstance;
  }

  const motherduckToken = process.env.MOTHERDUCK_DATABASE_TOKEN;
  if (!motherduckToken) {
    throw new Error('MOTHERDUCK_DATABASE_TOKEN environment variable is not set');
  }

  console.log('Connecting to MotherDuck...');
  
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
  
  const worker = new Worker(bundle.mainWorker!);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  
  const conn = await db.connect();
  await conn.query(`SET motherduck_token='${motherduckToken}'`);
  await conn.close();
  
  dbInstance = db;
  console.log('Connected to MotherDuck successfully');
  return db;
}

export async function GET() {
  try {
    const db = await getDuckDB();
    const conn = await db.connect();
    
    const result = await conn.query(`
      SELECT id, title, release_date, vote_average, popularity
      FROM md:your_database_name.movies
      WHERE release_date LIKE '2025%'
      ORDER BY vote_average DESC
      LIMIT 10;
    `);
    
    await conn.close();
    
    const rows = result.toArray().map(row => Object.fromEntries(row));
    
    return NextResponse.json(rows);
  } catch (err) {
    console.error('Database error:', err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Database error' },
      { status: 500 }
    );
  }
}