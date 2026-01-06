import { NextResponse } from 'next/server';

export async function GET() {
  try {
    const motherduckToken = process.env.MOTHERDUCK_TOKEN;
    const motherduckDatabase = process.env.MOTHERDUCK_DATABASE || 'tmdb';
    
    console.log('Environment check:', {
      hasToken: !!motherduckToken,
      database: motherduckDatabase,
      tokenPrefix: motherduckToken?.substring(0, 10) + '...'
    });
    
    if (!motherduckToken) {
      throw new Error('MOTHERDUCK_TOKEN not found. Check Vercel integration.');
    }

    const sqlQuery = `
      SELECT id, title, release_date, vote_average, popularity
      FROM ${motherduckDatabase}.movies
      WHERE release_date LIKE '2025%'
      ORDER BY vote_average DESC
      LIMIT 10;
    `;

    console.log('Executing query:', sqlQuery);

    // Try without "Bearer" prefix - MotherDuck might expect just the token
    const response = await fetch('https://api.motherduck.com/api/v0/sql', {
      method: 'POST',
      headers: {
        'Authorization': motherduckToken, // Remove "Bearer" prefix
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        sql: sqlQuery
      }),
    });

    console.log('Response status:', response.status);

    if (!response.ok) {
      const errorText = await response.text();
      console.error('MotherDuck API error:', errorText);
      throw new Error(`MotherDuck API error: ${response.status} ${response.statusText} - ${errorText}`);
    }

    const data = await response.json();
    console.log('Success! Row count:', data.data?.length || 0);
    
    return NextResponse.json(data.data || data);
  } catch (err) {
    console.error('Database error:', err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Database error' },
      { status: 500 }
    );
  }
}