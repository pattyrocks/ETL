import { NextResponse } from 'next/server';
import { Database } from 'duckdb';

export async function GET() {
  return new Promise((resolve) => {
    const token = process.env.tmdb_MOTHERDUCK_TOKEN;
    const databaseName = process.env.MOTHERDUCK_DATABASE_NAME || 'TMDB';
    
    console.log('Environment check:', {
      hasToken: !!token,
      databaseName: databaseName
    });
    
    if (!token) {
      resolve(NextResponse.json(
        { error: 'MotherDuck token not found' },
        { status: 500 }
      ));
      return;
    }

    console.log('Connecting to MotherDuck...');
    
    // Create database with MotherDuck connection string
    const connectionString = `md:${databaseName}?motherduck_token=${token}`;
    const db = new Database(connectionString, (err) => {
      if (err) {
        console.error('Connection error:', err);
        resolve(NextResponse.json(
          { error: `Connection failed: ${err.message}` },
          { status: 500 }
        ));
        return;
      }

      console.log('Connected, executing query...');
      
      const query = `
        SELECT id, title, release_date, vote_average, popularity 
        FROM movies 
        WHERE EXTRACT(YEAR FROM release_date) = 2025 
        ORDER BY vote_average DESC 
        LIMIT 10
      `;

      db.all(query, (err: Error | null, rows: any[]) => {
        if (err) {
          console.error('Query error:', err);
          db.close();
          resolve(NextResponse.json(
            { error: `Query failed: ${err.message}` },
            { status: 500 }
          ));
          return;
        }

        console.log('Query successful, rows:', rows?.length);
        db.close();
        resolve(NextResponse.json(rows || []));
      });
    });
  });
}