import { Client } from 'pg'

export default async function handler(req, res) {
  const conn = process.env.MOTHERDUCK_DATABASE_URL || process.env.DATABASE_URL
  if (!conn) {
    res.status(500).json({ error: 'Missing MOTHERDUCK_DATABASE_URL environment variable' })
    return
  }

  const client = new Client({ connectionString: conn })
  await client.connect()
  try {
    const q = 'SELECT id, title, release_date FROM movies ORDER BY release_date DESC NULLS LAST LIMIT 20;'
    const result = await client.query(q)
    res.status(200).json(result.rows)
  } catch (err) {
    res.status(500).json({ error: err.message })
  } finally {
    await client.end()
  }
}
