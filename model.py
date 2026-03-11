import duckdb

con = duckdb.connect("health_enforcement.duckdb")

print("\nTables")
print(con.execute("SHOW TABLES").df())


print(con.execute("SELECT COUNT(*) FROM fact_enforcement_actions").df())

print(con.execute("SELECT * FROM fact_enforcement_actions LIMIT 5").df())