import duckdb

con = duckdb.connect(database='TMDB', read_only=False)

print("Checking invalid dates in movies.release_date...\n")

# Find rows where conversion fails
invalid_dates = con.execute("""
    SELECT id, title, release_date
    FROM movies 
    WHERE release_date IS NOT NULL 
    AND TRY_CAST(release_date AS DATE) IS NULL
    LIMIT 20
""").fetchdf()

print(f"Sample of invalid dates (showing first 20):\n")
print(invalid_dates)

# Get statistics on DISTINCT invalid date formats ranked by count
print("\n" + "="*60)
print("DISTINCT INVALID DATE FORMATS (Ranked by Count)")
print("="*60)

invalid_patterns = con.execute("""
    SELECT 
        release_date,
        LENGTH(release_date) as length,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM movies 
    WHERE release_date IS NOT NULL 
    AND TRY_CAST(release_date AS DATE) IS NULL
    GROUP BY release_date, LENGTH(release_date)
    ORDER BY count DESC, release_date
""").fetchdf()

print(invalid_patterns.to_string())

# Total count
total_invalid = con.execute("""
    SELECT COUNT(*) as total
    FROM movies 
    WHERE release_date IS NOT NULL 
    AND TRY_CAST(release_date AS DATE) IS NULL
""").fetchone()[0]

print(f"\n{'='*60}")
print(f"TOTAL INVALID DATES: {total_invalid:,}")
print(f"{'='*60}")

# Group by pattern type
print("\n" + "="*60)
print("GROUPED BY PATTERN TYPE")
print("="*60)

pattern_groups = con.execute("""
    SELECT 
        CASE 
            WHEN release_date = '' THEN 'Empty string'
            WHEN release_date LIKE '0000%' THEN 'Starts with 0000'
            WHEN release_date LIKE '%-%-%' AND LENGTH(release_date) = 10 THEN 'Full format but invalid'
            WHEN release_date LIKE '%-%' AND LENGTH(release_date) < 10 THEN 'Partial date (YYYY-MM or similar)'
            WHEN LENGTH(release_date) = 4 THEN 'Year only'
            ELSE 'Other format'
        END as pattern_type,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM movies 
    WHERE release_date IS NOT NULL 
    AND TRY_CAST(release_date AS DATE) IS NULL
    GROUP BY pattern_type
    ORDER BY count DESC
""").fetchdf()

print(pattern_groups.to_string())

con.close()