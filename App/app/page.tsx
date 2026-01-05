'use client';

import Image from "next/image";
import { useEffect, useState } from "react";

export default function Home() {
  const [movies, setMovies] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchMovies = async () => {
      try {
        const res = await fetch("/api/movies");
        if (!res.ok) throw new Error("Failed to fetch movies");
        const data = await res.json();
        setMovies(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setLoading(false);
      }
    };

    fetchMovies();
  }, []);

  return (
    <div className="flex items-center justify-center min-h-screen bg-gradient-to-br from-zinc-900 to-black">
      <div className="text-center">
        <h1 className="text-6xl font-bold text-white mb-4">Hello World!</h1>
        <p className="text-xl text-zinc-400">TMDB ETL Application</p>
      </div>
    </div>
  );
}
