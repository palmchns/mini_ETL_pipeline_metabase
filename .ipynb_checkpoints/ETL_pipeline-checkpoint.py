import pandas as pd
from sqlalchemy import create_engine
import os
import requests
from prefect import task, flow
from dotenv import load_dotenv

load_dotenv()

# เช็กว่ารันอยู่ใน Docker หรือไม่เพื่อเลือก Host ที่ถูกต้อง
DB_HOST = "postgres_dw" if os.getenv("IN_DOCKER") else "localhost"
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_PORT = "5432" if os.getenv("IN_DOCKER") else os.getenv("POSTGRES_PORT")

PG_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@task(log_prints=True, retries=3)
def download_db():
    sources = {
        "chinook.db": "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite",
        "northwind.db": "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db"
    }
    for name, url in sources.items():
        if not os.path.exists(name):
            print(f"📥 Downloading {name}...")
            r = requests.get(url)
            with open(name, "wb") as f:
                f.write(r.content)
    return list(sources.keys())

@task(log_prints=True)
def etl_task(db_file, tables, prefix):
    src_engine = create_engine(f"sqlite:///{db_file}")
    target_engine = create_engine(PG_URI)
    
    for table in tables:
        df = pd.read_sql(f'SELECT * FROM "{table}"', src_engine)
        # Clean ข้อมูลเบื้องต้น: ตัวเล็ก, ไม่มีช่องว่าง
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]
        target_name = f"{prefix}_{table.lower().replace(' ', '_')}"
        df.to_sql(target_name, target_engine, if_exists="replace", index=False)
        print(f"✅ {target_name} loaded: {len(df)} rows")

@flow(name="End-to-End Data Pipeline")
def main_flow():
    download_db()
    # Northwind
    etl_task("northwind.db", ["Orders", "Products", "Customers"], "nw")
    # Chinook
    etl_task("chinook.db", ["Invoice", "Track", "Customer"], "cnk")

if __name__ == "__main__":
    main_flow()