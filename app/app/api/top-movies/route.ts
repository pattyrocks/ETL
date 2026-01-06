import { NextResponse } from 'next/server';

// Try edge runtime with fetch-based approach
export const runtime = 'edge';

export async function GET() {
  const token = process.env.tmdb_MOTHERDUCK_TOKEN;
  const db = process.env.MOTHERDUCK_DATABASE_NAME || 'TMDB';
  
  if (!token) {
    return NextResponse.json({ error: 'Token missing' }, { status: 500 });
  }

  try {
    // MotherDuck's SQL API endpoint
    const res = await fetch(`https://api.motherduck.com/v0/databases/${db}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        sql: `SELECT id::VARCHAR as id, title, release_date, vote_average, popularity 
              FROM movies 
              WHERE YEAR(release_date) = 2025 
              ORDER BY vote_average DESC 
              LIMIT 10`
      })
    });

    const data = await res.json();
    return NextResponse.json(data.rows || data);
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}