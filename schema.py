import duckdb

con = duckdb.connect("health_enforcement.duckdb")

con.execute("""
    CREATE OR REPLACE TABLE fact_enforcement_actions AS
    SELECT * FROM read_csv_auto('cleaned/enforcement_actions.csv')
""")

con.execute("""
    CREATE OR REPLACE TABLE fact_ltc_narratives AS
    SELECT FACID,FACILITY_NAME,PENALTY_NUMBER,CLASS_ASSESSED_INITIAL,PENALTY_ISSUE_DATE,
    EVENTID,TOP_KEYWORDS
             FROM read_csv_auto('cleaned/ltc_narratives.csv')
""")


con.execute("""
    CREATE OR REPLACE TABLE dim_fac_code AS
    SELECT VARIABLE,VALUE,DESCRIPTION,DEFINITION
    FROM read_csv_auto('cleaned/lookup_facility_types.csv')
    WHERE VARIABLE ='FAC_TYPE_CODE'
""")


con.execute("""
    CREATE OR REPLACE TABLE dim_district_office AS
    SELECT VARIABLE,VALUE,DESCRIPTION 
    FROM read_csv_auto('cleaned/lookup_facility_types.csv')
    WHERE VARIABLE ='District_Office'
""")


con.execute("""
    CREATE OR REPLACE TABLE enriched_mart AS
    SELECT
        a.FACID,
        a.FACILITY_NAME,
        a.FAC_TYPE_CODE,
        a.FAC_FDR,
        a.DISTRICT_OFFICE,
        a.LTC,
        f.DEFINITION                                        AS FACILITY_TYPE_DESC,
        a.PENALTY_NUMBER,
        a.PENALTY_ISSUE_DATE,
        a.PENALTY_TYPE,
        a.PENALTY_CATEGORY,
        a.DISPOSITION,
        a.CLASS_ASSESSED_INITIAL,
        a.CLASS_ASSESSED_FINAL,
        a.DEATH_RELATED,
        a.APPEALED,
        a.TOTAL_AMOUNT_INITIAL,
        a.TOTAL_AMOUNT_DUE_FINAL,
        a.TOTAL_BALANCE_DUE,
        a.TOTAL_COLLECTED_AMOUNT,
        a.HIGHEST_PRIORITY,
        a.COMPLAINT_COUNT,
        n.TOP_KEYWORDS

    FROM fact_enforcement_actions a
    LEFT JOIN dim_fac_code f
        ON a.FAC_TYPE_CODE = f.VALUE
    LEFT JOIN fact_ltc_narratives n
        ON a.PENALTY_NUMBER = n.PENALTY_NUMBER
""")


# Validate
print(con.execute("SELECT COUNT(*) FROM enriched_mart").fetchone())
print(con.execute("SELECT COUNT(DISTINCT FACID) FROM enriched_mart").fetchone())
print(con.execute("SELECT * FROM enriched_mart LIMIT 3").df())


