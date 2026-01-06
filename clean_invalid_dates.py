import duckdb

con = duckdb.connect(database='TMDB', read_only=False)

print("="*60)
print("CLEANING INVALID DATES IN movies.release_date")
print("="*60)

# Step 1: Check current state
print("\n1. Checking current invalid dates...")
total_invalid = con.execute("""
    SELECT COUNT(*) 
    FROM movies 
    WHERE release_date IS NOT NULL 
    AND release_date != ''
    AND TRY_CAST(release_date AS DATE) IS NULL
""").fetchone()[0]

empty_count = con.execute("""
    SELECT COUNT(*) 
    FROM movies 
    WHERE release_date = ''
""").fetchone()[0]

print(f"   Invalid dates (non-empty): {total_invalid:,}")
print(f"   Empty string dates: {empty_count:,}")

# Step 2: Set empty strings to NULL
print("\n2. Converting empty strings to NULL...")
con.execute("""
    UPDATE movies 
    SET release_date = NULL 
    WHERE release_date = ''
""")
print(f"   ✓ Converted {empty_count:,} empty strings to NULL")

# Step 3: Set other invalid dates to NULL (optional - uncomment if you want this)
# This will set all non-convertible dates to NULL
print("\n3. Would you like to set all other invalid dates to NULL as well?")
print("   This includes dates like '0000-00-00', partial dates, etc.")
response = input("   Convert all invalid dates to NULL? (yes/no): ").strip().lower()

if response in ['yes', 'y']:
    con.execute("""
        UPDATE movies 
        SET release_date = NULL 
        WHERE release_date IS NOT NULL 
        AND TRY_CAST(release_date AS DATE) IS NULL
    """)
    print(f"   ✓ Converted {total_invalid:,} invalid dates to NULL")
else:
    print("   Skipped - only empty strings were converted")

# Step 4: Verify
print("\n4. Verifying cleanup...")
remaining_invalid = con.execute("""
    SELECT COUNT(*) 
    FROM movies 
    WHERE release_date IS NOT NULL 
    AND TRY_CAST(release_date AS DATE) IS NULL
""").fetchone()[0]

null_count = con.execute("""
    SELECT COUNT(*) 
    FROM movies 
    WHERE release_date IS NULL
""").fetchone()[0]

print(f"   Remaining invalid dates: {remaining_invalid:,}")
print(f"   NULL dates: {null_count:,}")

# Step 5: Show sample of what's left
if remaining_invalid > 0:
    print("\n5. Sample of remaining invalid dates:")
    samples = con.execute("""
        SELECT release_date, COUNT(*) as count
        FROM movies 
        WHERE release_date IS NOT NULL 
        AND TRY_CAST(release_date AS DATE) IS NULL
        GROUP BY release_date
        ORDER BY count DESC
        LIMIT 10
    """).fetchdf()
    print(samples.to_string())

con.close()

print("\n" + "="*60)
print("CLEANUP COMPLETE")
print("="*60)
print("\nNext step: Run convert_all_dates.py again to convert the column")