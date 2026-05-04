import re
import mysql.connector
import snowflake.connector
import time

# ======================
# MYSQL CONNECTION
# ======================
mysql_conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3308,
    user="cdc_user",
    password="cdc123",
    database="xxxx"
)
print("mysql connected...")

# ======================
# SNOWFLAKE CONNECTION
# ======================
print("Connecting to Snowflake...")

sf_conn = snowflake.connector.connect(
    user="xxxxx",
    password="xxxx",
    account="xxxx",
    warehouse="xxxx",
    database="xxxx",
    schema="xxx",
    role="ACCOUNTADMIN",
    insecure_mode=True
)

print("Snowflake connected")


# ======================
# WATERMARK FUNCTIONS
# ======================
def get_watermark():
    cursor = sf_conn.cursor()
    cursor.execute("""
        SELECT last_updated
        FROM hospital_pipeline.bronze.cdc_watermark
        WHERE table_name = 'patient'
    """)
    row = cursor.fetchone()
    return row[0] if row else "2020-01-01 00:00:00"


def update_watermark(new_time):
    cursor = sf_conn.cursor()
    cursor.execute("""
        UPDATE hospital_pipeline.bronze.cdc_watermark
        SET last_updated = %s
        WHERE table_name = 'patient'
    """, (new_time,))


# ======================
# FETCH CHANGES
# ======================
def fetch_changes(last_sync_time):
    cursor = mysql_conn.cursor(dictionary=True)

    query = """
    SELECT id, name, age, sex, updated_at
    FROM patient
    WHERE updated_at > %s
    ORDER BY updated_at
    """

    cursor.execute(query, (last_sync_time,))
    return cursor.fetchall()

def clean_age(age):
    if age is None:
        return None

    import re
    match = re.search(r"\d+", str(age))
    return int(match.group()) if match else None

# ======================
# SCD TYPE 2 LOGIC
# ======================
def load_to_stage(rows):
    cursor = sf_conn.cursor()

    try:
    
        cursor.execute("TRUNCATE TABLE hospital_pipeline.bronze.patients_stage")

        insert_sql = """
        INSERT INTO hospital_pipeline.bronze.patients_stage
        (patient_id, name, age, sex, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        """

        data = [
            (
                r["id"],
                r["name"],
                clean_age(r["age"]),
                r["sex"],
                r["updated_at"]
            )
            for r in rows
        ]

        cursor.executemany(insert_sql, data)

    finally:
        cursor.close()

# ======================
# MERGED
# ======================

def merge_scd():
    cursor = sf_conn.cursor()

    try:
        cursor.execute("""
        MERGE INTO hospital_pipeline.silver.patients_scd AS target
        USING hospital_pipeline.bronze.patients_stage AS source
        ON target.patient_id = source.patient_id
           AND target.is_current = TRUE

        WHEN MATCHED AND (
            target.name != source.name OR
            target.age != source.age OR
            target.sex != source.sex
        )
        THEN UPDATE SET
            target.end_date = CURRENT_TIMESTAMP,
            target.is_current = FALSE

        WHEN NOT MATCHED THEN
        INSERT (
            patient_id, name, age, sex,
            start_date, end_date, is_current
        )
        VALUES (
            source.patient_id,
            source.name,
            source.age,
            source.sex,
            CURRENT_TIMESTAMP,
            NULL,
            TRUE
        )
        """)

    finally:
        cursor.close()

# ======================
# MAIN CDC LOOP
# ======================
def run_cdc():

    print("CDC MERGE Pipeline Started")

    while True:

        print("Getting watermark...")
        last_sync_time = get_watermark()

        print("Fetching changes from MySQL...")
        rows = fetch_changes(last_sync_time)

        print(f"📦 Loading {len(rows)} records")

        if rows:
            load_to_stage(rows)     # bulk load
            merge_scd()             # fast SCD

            new_watermark = rows[-1]["updated_at"]
            update_watermark(new_watermark)

            print("MERGE completed + watermark updated")

        else:
            print("No changes found")

        time.sleep(5)
        
# ======================
# START PIPELINE
# ======================
run_cdc()
