'use client';

import Link from 'next/link';

export default function Home() {
  return (
    <div className="flex items-center justify-center min-h-screen bg-gradient-to-br from-zinc-900 to-black">
      <div className="text-center">
        <h1 className="text-6xl font-bold text-white mb-4">Hello World!</h1>
        <p className="text-xl text-zinc-400 mb-8">TMDB ETL Application</p>
        <Link
          href="/movies"
          className="inline-block px-6 py-3 bg-amber-600 hover:bg-amber-700 text-white font-semibold rounded-lg transition"
        >
          View Top Movies 2025
        </Link>
      </div>
    </div>
  );
}
