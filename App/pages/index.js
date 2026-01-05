import { useEffect, useState } from 'react'

export default function Home() {
  const [movies, setMovies] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch('/api/movies')
      .then((r) => r.json())
      .then((data) => {
        setMovies(data || [])
      })
      .catch(() => setMovies([]))
      .finally(() => setLoading(false))
  }, [])

  return (
    <main style={{ padding: 24, fontFamily: 'Inter, system-ui, -apple-system' }}>
      <h1 style={{ marginBottom: 12 }}>TMDB — Movies</h1>
      {loading && <p>Loading...</p>}
      {!loading && movies.length === 0 && <p>No movies found.</p>}
      <ul style={{ listStyle: 'none', padding: 0 }}>
        {movies.map((m) => (
          <li key={m.id} style={{ padding: 8, borderBottom: '1px solid #eee' }}>
            <strong>{m.title}</strong>
            {m.release_date && <span style={{ marginLeft: 8, color: '#666' }}>({m.release_date?.slice(0,4)})</span>}
          </li>
        ))}
      </ul>
    </main>
  )
}
