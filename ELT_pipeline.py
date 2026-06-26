import pandas as pd
from sqlalchemy import create_engine, text
import os
import requests
import tempfile
import time
from prefect import task, flow

# ==========================================
# ⚙️ Configuration
# ==========================================
DB_HOST = "omni_postgres" if os.getenv("IN_DOCKER") else "localhost"
DB_USER = os.getenv("POSTGRES_USER", "engineer")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password123")
DB_DB = os.getenv("POSTGRES_DB", "data_warehouse")
DB_PORT = "5432" if os.getenv("IN_DOCKER") else "5440"

PG_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_DB}"

def wait_for_db(engine, retries=10, delay=5):
    """ฟังก์ชันชะลอการทำงานจนกว่า Database จะพร้อม (Best Practice)"""
    for i in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                print("✅ Database is ready!")
                return
        except Exception:
            print(f"⏳ Waiting for database... (Attempt {i+1}/{retries})")
            time.sleep(delay)
    raise ConnectionError("❌ Database connection failed after multiple retries.")

def wait_for_prefect(retries=12, delay=5):
    """ฟังก์ชันชะลอการทำงานจนกว่า Prefect API จะพร้อม"""
    prefect_api_url = os.getenv("PREFECT_API_URL")
    if not prefect_api_url:
        print("⚠️ PREFECT_API_URL not set. Skipping Prefect health check.")
        return
    health_check_url = prefect_api_url.replace("/api", "/api/health")

    for i in range(retries):
        try:
            response = requests.get(health_check_url)
            if response.status_code == 200:
                print("✅ Prefect API is ready!")
                return
        except requests.ConnectionError:
            pass # เพิกเฉยต่อ Connection Error แล้วลองใหม่
        print(f"⏳ Waiting for Prefect API... (Attempt {i+1}/{retries})")
        time.sleep(delay)
    raise ConnectionError("❌ Prefect API connection failed after multiple retries.")

# ==========================================
# 🥉 Task 1: Extract & Load to Bronze (Dynamic Full Load)
# ==========================================
@task(log_prints=True, retries=2)
def load_raw_to_postgres():
    sources = { "chinook": "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite", "nw": "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db" }
    
    target_engine = create_engine(PG_URI)
    wait_for_db(target_engine) # เช็กว่า DB พร้อมก่อนทำงาน

    for prefix, url in sources.items():
        print(f"📥 Downloading {prefix} database...")
        r = requests.get(url)
        
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
            tmp.write(r.content)
            tmp.flush()
            tmp_path = tmp.name
        
        try:
            src_engine = create_engine(f"sqlite:///{tmp_path}")
            tables_df = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", src_engine)
            tables = tables_df['name'].tolist()
            
            for table in tables:
                if table.startswith('sqlite_'):
                    continue
                print(f"⏳ Extracting raw {table}...")
                df = pd.read_sql(f'SELECT * FROM "{table}"', src_engine)
                df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                for col in df.columns:
                    if 'date' in col.lower():
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                target_name = f"{prefix}_raw_{table.lower().replace(' ', '_')}"
                df.to_sql(target_name, target_engine, if_exists="replace", index=False)
                print(f"✅ {target_name} loaded: {len(df)} rows")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

# ==========================================
# 🥇 Task 2: In-Database Transform (Full Schema)
# ==========================================
@task(log_prints=True)
def transform_star_schema_in_db():
    print("⏳ Executing In-Database Transformation from SQL files...")
    target_engine = create_engine(PG_URI)
    sql_dir = "sql_transforms"
    
    with target_engine.begin() as conn:
        sql_files = sorted([f for f in os.listdir(sql_dir) if f.endswith('.sql')])
        for sql_file in sql_files:
            print(f"⚡ Executing: {sql_file}")
            with open(os.path.join(sql_dir, sql_file), 'r') as f:
                conn.execute(text(f.read()))
    print("✅ All transformations completed!")

# ==========================================
#  Orchestration Flow
# ==========================================
@flow(name="OmniCorp ELT Pipeline (Full ERD)")
def main_flow():
    load_raw_to_postgres()
    transform_star_schema_in_db()

if __name__ == "__main__":
    print("🚀 Starting Initial Automatic Run...")
    # รอให้ Service ที่จำเป็น (DB และ Prefect) พร้อมทำงานก่อน
    if os.getenv("IN_DOCKER"):
        wait_for_prefect()
    main_flow() # รันครั้งแรกทันที
    
    print("📡 Deploying to Prefect Server and waiting for Quick Runs...")
    # การใช้ .serve() จะช่วยให้โชว์ในแท็บ Deployment และเปิดสแตนด์บายไว้
    main_flow.serve(name="OmniCorp-Manual-Trigger", tags=["ELT", "DataWarehouse"])
