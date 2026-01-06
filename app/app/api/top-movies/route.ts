import { NextResponse } from 'next/server';

export async function GET() {
  try {
    const motherduckToken = process.env.MOTHERDUCK_TOKEN;
    const motherduckDatabase = process.env.MOTHERDUCK_DATABASE || 'tmdb';
    
    if (!motherduckToken) {
      throw new Error('MOTHERDUCK_TOKEN not found. Check Vercel integration.');
    }

    // Use the MotherDuck REST API endpoint provided by the integration
    const response = await fetch(`https://api.motherduck.com/api/v0/sql`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${motherduckToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        sql: `
          SELECT id, title, release_date, vote_average, popularity
          FROM ${motherduckDatabase}.movies
          WHERE release_date LIKE '2025%'
          ORDER BY vote_average DESC
          LIMIT 10;
        `
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('MotherDuck API error:', errorText);
      throw new Error(`MotherDuck API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    
    return NextResponse.json(data.data || data);
  } catch (err) {
    console.error('Database error:', err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Database error' },
      { status: 500 }
    );
  }
}