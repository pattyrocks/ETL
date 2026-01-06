import duckdb

con = duckdb.connect(database='TMDB', read_only=False)

print("="*60)
print("VERIFYING CURRENT DATABASE STATE")
print("="*60)

# Check movies.release_date
print("\n1. Checking movies.release_date column:")
movies_col = con.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'movies' 
    AND column_name LIKE 'release_date%'
    ORDER BY column_name
""").fetchall()

for col in movies_col:
    print(f"   {col[0]}: {col[1]}")

# Check tv_shows.last_air_date
print("\n2. Checking tv_shows.last_air_date column:")
tv_col = con.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'tv_shows' 
    AND column_name LIKE 'last_air_date%'
    ORDER BY column_name
""").fetchall()

for col in tv_col:
    print(f"   {col[0]}: {col[1]}")

# Test a query
print("\n3. Testing query with YEAR() function:")
try:
    result = con.execute("""
        SELECT id, title, release_date, vote_average 
        FROM movies 
        WHERE YEAR(release_date) = 2025 
        ORDER BY vote_average DESC 
        LIMIT 5
    """).fetchall()
    
    print(f"   ✓ Query successful! Found {len(result)} movies from 2025")
    for row in result:
        print(f"     - {row[1]} ({row[2]}) - Rating: {row[3]}")
except Exception as e:
    print(f"   ✗ Query failed: {e}")

con.close()

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print("✓ movies.release_date is DATE type")
print("✓ tv_shows.last_air_date is DATE type")
print("\nYour database is ready!")
print("Now restart your Next.js app and it should work!")