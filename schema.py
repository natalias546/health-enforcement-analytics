import duckdb

con = duckdb.connect("health_enforcement.duckdb")

con.execute("""
    CREATE OR REPLACE TABLE fact_enforcement_actions AS
    SELECT * FROM read_csv_auto('cleaned/enforcement_actions.csv')
""")

con.execute("""
    CREATE OR REPLACE TABLE dim_ltc_narratives AS
    SELECT * FROM read_csv_auto('cleaned/ltc_narratives.csv')
""")

con.execute("""
    CREATE OR REPLACE TABLE dim_facility_types AS
    SELECT * FROM read_csv_auto('cleaned/lookup_facility_types.csv')
""")