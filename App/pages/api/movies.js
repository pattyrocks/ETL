import { Pool } from 'pg'

const connString = process.env.MOTHERDUCK_DATABASE_URL || process.env.DATABASE_URL

// reuse a global pool to avoid creating a new client per request (works in serverless & long-running)
if (!global.__pgPool) {
  global.__pgPool = new Pool({
    connectionString: connString,
    max: 10,
    idleTimeoutMillis: 30000,
  })
}
const pool = global.__pgPool

export default async function handler(req, res) {
  if (!connString) {
    res.status(500).json({ error: 'Missing MOTHERDUCK_DATABASE_URL or DATABASE_URL environment variable' })
    return
  }

  let client
  try {
    client = await pool.connect()
    const q = 'SELECT id, title, release_date FROM movies ORDER BY release_date DESC NULLS LAST LIMIT 20;'
    const result = await client.query(q)
    res.status(200).json(result.rows)
  } catch (err) {
    res.status(500).json({ error: err.message })
  } finally {
    if (client) client.release()
  }
}
