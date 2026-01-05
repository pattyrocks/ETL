'use client';

import { useEffect, useState } from 'react';

interface Movie {
  id: number;
  title: string;
  release_date: string;
  vote_average: number;
  popularity: number;
}

export default function MoviesPage() {
  const [movies, setMovies] = useState<Movie[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchMovies = async () => {
      try {
        const res = await fetch('/api/top-movies');
        if (!res.ok) throw new Error('Failed to fetch movies');
        const data = await res.json();
        setMovies(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    };

    fetchMovies();
  }, []);

  return (
    <div className="min-h-screen bg-gradient-to-br from-zinc-900 via-black to-zinc-900 px-6 py-12">
      <div className="max-w-6xl mx-auto">
        <h1 className="text-4xl font-bold text-white mb-2">🎬 Top Movies 2025</h1>
        <p className="text-zinc-400 mb-8">Top 10 highest-rated movies from 2025</p>

        {loading && (
          <div className="flex items-center justify-center py-12">
            <p className="text-zinc-300 text-lg">Loading movies...</p>
          </div>
        )}

        {error && (
          <div className="bg-red-900/20 border border-red-700 rounded-lg p-4 mb-6">
            <p className="text-red-400">Error: {error}</p>
          </div>
        )}

        {!loading && movies.length > 0 && (
          <div className="overflow-x-auto rounded-lg border border-zinc-700 bg-zinc-900/50 backdrop-blur">
            <table className="w-full">
              <thead>
                <tr className="border-b border-zinc-700 bg-zinc-800/50">
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white">Rank</th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white">Title</th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white">Release Date</th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white">Rating</th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white">Popularity</th>
                </tr>
              </thead>
              <tbody>
                {movies.map((movie, index) => (
                  <tr
                    key={movie.id}
                    className="border-b border-zinc-700 hover:bg-zinc-800/30 transition"
                  >
                    <td className="px-6 py-4 text-sm text-zinc-300 font-semibold">
                      #{index + 1}
                    </td>
                    <td className="px-6 py-4 text-sm text-white font-medium truncate">
                      {movie.title}
                    </td>
                    <td className="px-6 py-4 text-sm text-zinc-400">
                      {movie.release_date || 'N/A'}
                    </td>
                    <td className="px-6 py-4 text-sm">
                      <div className="flex items-center gap-2">
                        <div className="w-16 bg-zinc-700 rounded-full h-2">
                          <div
                            className="bg-gradient-to-r from-yellow-400 to-amber-500 h-2 rounded-full"
                            style={{
                              width: `${(movie.vote_average / 10) * 100}%`,
                            }}
                          />
                        </div>
                        <span className="text-amber-400 font-semibold">
                          {movie.vote_average.toFixed(1)}
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-4 text-sm text-zinc-400">
                      {movie.popularity.toFixed(0)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {!loading && movies.length === 0 && !error && (
          <div className="text-center py-12">
            <p className="text-zinc-400">No movies found for 2025.</p>
          </div>
        )}
      </div>
    </div>
  );
}