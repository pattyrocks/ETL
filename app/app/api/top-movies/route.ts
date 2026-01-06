import { NextResponse } from 'next/server';

export const runtime = 'edge';

export async function GET() {
  const token = process.env.tmdb_MOTHERDUCK_TOKEN;
  const db = process.env.MOTHERDUCK_DATABASE_NAME || 'TMDB';
  
  console.log('Token check:', {
    hasToken: !!token,
    tokenLength: token?.length,
    tokenStart: token?.substring(0, 10) + '...'
  });
  
  if (!token) {
    return NextResponse.json({ error: 'Token missing' }, { status: 500 });
  }

  try {
    const sqlQuery = `
      SELECT id::VARCHAR as id, title, release_date, vote_average, popularity 
      FROM ${db}.movies 
      WHERE YEAR(release_date) = 2025 
      ORDER BY vote_average DESC 
      LIMIT 10
    `.trim();
    
    // Try with token in query parameter (MotherDuck's alternative auth method)
    const res = await fetch('https://api.motherduck.com/sql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        // Try without Bearer prefix
        'Authorization': token,
      },
      body: JSON.stringify({
        query: sqlQuery,
        // Add token in body as alternative
        token: token
      })
    });

    if (!res.ok) {
      const errorText = await res.text();
      console.error('MotherDuck API error:', res.status, errorText);
      
      // Try alternative approach with motherduck_token parameter
      const res2 = await fetch('https://api.motherduck.com/sql', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: sqlQuery,
          motherduck_token: token
        })
      });
      
      if (!res2.ok) {
        const errorText2 = await res2.text();
        return NextResponse.json(
          { error: 'Authentication failed', details: errorText2 }, 
          { status: res2.status }
        );
      }
      
      const data2 = await res2.json();
      return NextResponse.json(data2.rows || data2.data || data2);
    }

    const data = await res.json();
    return NextResponse.json(data.rows || data.data || data);
    
  } catch (error: any) {
    console.error('Query error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

// Package.json scripts
{
  "buildCommand": "cd app && npm run build",
  "devCommand": "cd app && npm run dev",
  "installCommand": "cd app && npm install",
  "framework": "nextjs",
  "outputDirectory": "app/.next"
}