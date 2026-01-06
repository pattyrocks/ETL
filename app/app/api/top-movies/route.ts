import { NextResponse } from 'next/server';

export async function GET() {
  try {
    const motherduckToken = process.env.MOTHERDUCK_DATABASE_TOKEN;
    if (!motherduckToken) {
      throw new Error('MOTHERDUCK_DATABASE_TOKEN environment variable is not set');
    }

    const query = `
      SELECT id, title, release_date, vote_average, popularity
      FROM movies
      WHERE release_date LIKE '2025%'
      ORDER BY vote_average DESC
      LIMIT 10;
    `;

    const response = await fetch('https://api.motherduck.com/v1/query', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${motherduckToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: query,
        database: 'tmdb'
      }),
    });

    if (!response.ok) {
      throw new Error(`MotherDuck API error: ${response.statusText}`);
    }

    const data = await response.json();
    
    return NextResponse.json(data.rows || data);
  } catch (err) {
    console.error('Database error:', err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Database error' },
      { status: 500 }
    );
  }
}