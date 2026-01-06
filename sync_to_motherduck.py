import { NextResponse } from 'next/server';
import { Database } from 'duckdb';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

// Add BigInt serialization support
(BigInt.prototype as any).toJSON = function() {
  return this.toString();
};

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
        WHERE YEAR(release_date) = 2025 
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
        
        // Use JSON.stringify with BigInt support, then parse back
        const jsonString = JSON.stringify(rows || [], (key, value) =>
          typeof value === 'bigint' ? value.toString() : value
        );
        
        resolve(new NextResponse(jsonString, {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
          },
        }));
      });
    });
  });
}