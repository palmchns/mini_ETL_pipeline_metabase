import pandas as pd
from sqlalchemy import create_engine
import os
import requests
from prefect import task, flow
from dotenv import load_dotenv

# โหลดตัวแปรจากไฟล์ .env
load_dotenv()

# เช็กว่ารันอยู่ใน Docker หรือไม่เพื่อเลือก Host ที่ถูกต้อง
DB_HOST = "postgres_dw" if os.getenv("IN_DOCKER") else "localhost"
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
# ถ้าไม่มี POSTGRES_PORT ใน .env จะใช้ 5435 เป็นค่า Default
DB_PORT = "5432" if os.getenv("IN_DOCKER") else os.getenv("POSTGRES_PORT", "5435")

# แนะนำให้ใส่ +psycopg2 เพื่อความเสถียรของ SQLAlchemy
PG_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

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
        else:
            print(f"✅ {name} already exists.")
    return list(sources.keys())

@task(log_prints=True)
def etl_task(db_file, tables, prefix):
    # ใช้ Absolute Path เพื่อให้ SQLAlchemy หาไฟล์เจอเสมอไม่ว่าจะรันจาก Path ไหน
    db_path = os.path.abspath(db_file)
    src_engine = create_engine(f"sqlite:///{db_path}")
    target_engine = create_engine(PG_URI)
    
    for table in tables:
        print(f"⏳ Extracting {table} from {db_file}...")
        df = pd.read_sql(f'SELECT * FROM "{table}"', src_engine)
        
        # Clean ข้อมูลเบื้องต้น: คอลัมน์ตัวเล็ก, ไม่มีช่องว่าง
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]
        
        # แปลงข้อมูลวันที่ให้เป็น Datetime (สำคัญมากสำหรับการทำ Dashboard ใน Metabase)
        for col in df.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        target_name = f"{prefix}_{table.lower().replace(' ', '_')}"
        df.to_sql(target_name, target_engine, if_exists="replace", index=False)
        print(f"✅ {target_name} loaded: {len(df)} rows")

@flow(name="End-to-End Data Pipeline")
def main_flow():
    # 1. โหลดฐานข้อมูล
    download_db()
    
    # 2. จัดการย้าย Northwind (ดึงครบทุกตาราง ยกเว้น sqlite_sequence)
    nw_tables = [
        "Categories", "CustomerCustomerDemo", "CustomerDemographics", 
        "Customers", "Employees", "EmployeeTerritories", "Order Details", 
        "Orders", "Products", "Regions", "Shippers", "Suppliers", "Territories"
    ]
    etl_task("northwind.db", nw_tables, "nw")
    
    # 3. จัดการย้าย Chinook (ครบทั้ง 11 ตาราง)
    cnk_tables = [
        "Album", "Artist", "Customer", "Employee", "Genre", 
        "Invoice", "InvoiceLine", "MediaType", "Playlist", 
        "PlaylistTrack", "Track"
    ]
    etl_task("chinook.db", cnk_tables, "cnk")

if __name__ == "__main__":
    main_flow()