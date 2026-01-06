import { NextResponse } from 'next/server';

export const runtime = 'edge';

export async function GET() {
  const token = process.env.tmdb_MOTHERDUCK_TOKEN;
  const db = process.env.MOTHERDUCK_DATABASE_NAME || 'TMDB';
  
  if (!token) {
    return NextResponse.json({ error: 'Token missing' }, { status: 500 });
  }

  try {
    // Alternative: Use token as query parameter
    const query = encodeURIComponent(`
      SELECT id::VARCHAR as id, title, release_date, vote_average, popularity 
      FROM ${db}.movies 
      WHERE YEAR(release_date) = 2025 
      ORDER BY vote_average DESC 
      LIMIT 10
    `);
    
    const res = await fetch(
      `https://api.motherduck.com/v1/query?token=${token}&query=${query}`,
      {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
        },
      }
    );

    if (!res.ok) {
      const errorText = await res.text();
      console.error('MotherDuck API error:', errorText);
      return NextResponse.json(
        { error: `API error: ${res.status}`, details: errorText }, 
        { status: res.status }
      );
    }

    const data = await res.json();
    return NextResponse.json(data.rows || data);
    
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}