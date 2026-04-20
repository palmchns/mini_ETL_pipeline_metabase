import os
import requests
import tempfile
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from prefect import task, flow

# ==========================================
# ⚙️ Configuration & Database Setup
# ==========================================
DB_HOST = "postgres_dw" if os.getenv("IN_DOCKER") else "localhost"
DB_USER = os.getenv("POSTGRES_USER", "engineer")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password123")
DB_DB = os.getenv("POSTGRES_DB", "data_warehouse")
DB_PORT = "5432" if os.getenv("IN_DOCKER") else "5435"

PG_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_DB}"

# สร้างโฟลเดอร์จำลอง Data Lake (Bronze Layer) ในเครื่อง
LAKE_PATH = "./data/bronze"
os.makedirs(LAKE_PATH, exist_ok=True)

# ==========================================
# 🥉 Task 1: Extract & Load ALL Tables to Bronze (Parquet)
# ==========================================
@task(log_prints=True, retries=2)
def extract_to_bronze_parquet():
    """
    โหลด SQLite ลง Temp File -> สแกนหา 'ทุกตาราง' อัตโนมัติ -> เซฟเป็น Parquet (Data Lake) -> ลบ Temp File
    """
    sources = {
        "chinook": "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite",
        "northwind": "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db"
    }
    
    parquet_files = []

    for source_name, url in sources.items():
        print(f"📥 Downloading {source_name} to Temporary File...")
        r = requests.get(url)
        
        # ใช้ delete=False เพื่อให้ SQLAlchemy เข้าถึงไฟล์ได้
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.write(r.content)
        tmp.close() 
        
        try:
            src_engine = create_engine(f"sqlite:///{tmp.name}")
            
            # 🛠️ ใช้ Inspector สแกนหาทุกตารางที่มีใน Database อัตโนมัติ
            inspector = inspect(src_engine)
            tables = inspector.get_table_names()
            
            print(f"🔍 Found {len(tables)} tables in {source_name}...")
            
            for table in tables:
                df = pd.read_sql(f'SELECT * FROM "{table}"', src_engine)
                
                # Clean ชื่อคอลัมน์ให้อ่านง่ายและเป็นมาตรฐาน (ตัวเล็กทั้งหมด, แทนที่ช่องว่างด้วย _)
                df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                
                # จัดการชื่อไฟล์
                clean_table_name = table.lower().replace(' ', '_')
                file_name = f"{source_name}_{clean_table_name}.parquet"
                file_path = os.path.join(LAKE_PATH, file_name)
                
                # เซฟลง Data Lake เป็นไฟล์ Parquet
                df.to_parquet(file_path, index=False)
                parquet_files.append((source_name, clean_table_name, file_path))
                print(f"   ✅ Saved {table} -> {file_name}")
                
        finally:
            # ทำลาย Temp File ทิ้งทันทีเพื่อไม่ให้รกเครื่อง
            os.unlink(tmp.name)
            
    return parquet_files

# ==========================================
# 🥇 Task 2: ELT to Star Schema (Postgres)
# ==========================================
@task(log_prints=True)
def load_staging_and_transform_elt(parquet_files):
    """
    เลือกเฉพาะ Parquet ที่ใช้ -> โหลดเข้า Staging -> ใช้ SQL ประมวลผลเป็น Star Schema -> ลบ Staging
    """
    target_engine = create_engine(PG_URI)
    
    # กำหนดเฉพาะตารางที่จำเป็นต้องใช้ในการทำ Star Schema เพื่อประหยัดพื้นที่ Staging
    tables_needed = {
        "chinook": ["customer", "track", "genre", "invoice", "invoiceline"],
        "northwind": ["customers", "products", "categories", "orders", "order_details"]
    }
    
    loaded_stg_tables = []

    # 1. โหลดข้อมูลจาก Parquet เข้าตารางชั่วคราว (Staging) เฉพาะตารางที่ต้องใช้
    print("🚀 Loading necessary Parquet data to Postgres Staging tables...")
    for source, table, path in parquet_files:
        if table in tables_needed.get(source, []):
            df = pd.read_parquet(path)
            stg_table_name = f"stg_{source}_{table}"
            df.to_sql(stg_table_name, target_engine, if_exists="replace", index=False)
            loaded_stg_tables.append(stg_table_name)
            print(f"   -> Loaded {stg_table_name}")

    print("⚡ Executing In-Database Transformation (ELT)...")
    with target_engine.begin() as conn:
        
        # ---------------------------------------------------------
        # 2. สร้างตารางและทำ UPSERT (DimCustomer)
        # ---------------------------------------------------------
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                integration_id VARCHAR(100) UNIQUE,
                customer_name VARCHAR(255),
                company VARCHAR(255),
                city VARCHAR(100),
                country VARCHAR(100)
            );
        """))
        
        # Insert/Update ฝั่ง Chinook
        conn.execute(text("""
            INSERT INTO dim_customer (integration_id, customer_name, company, city, country)
            SELECT 'CHN_' || customerid, firstname || ' ' || lastname, company, city, country 
            FROM stg_chinook_customer
            ON CONFLICT (integration_id) DO UPDATE 
            SET customer_name = EXCLUDED.customer_name, company = EXCLUDED.company, 
                city = EXCLUDED.city, country = EXCLUDED.country;
        """))
        
        # Insert/Update ฝั่ง Northwind
        conn.execute(text("""
            INSERT INTO dim_customer (integration_id, customer_name, company, city, country)
            SELECT 'NWD_' || customerid, contactname, companyname, city, country 
            FROM stg_northwind_customers
            ON CONFLICT (integration_id) DO UPDATE 
            SET customer_name = EXCLUDED.customer_name, company = EXCLUDED.company, 
                city = EXCLUDED.city, country = EXCLUDED.country;
        """))
        print("✅ DimCustomer Updated (UPSERT)")

        # ---------------------------------------------------------
        # 3. สร้างตารางและทำ UPSERT (DimProduct)
        # ---------------------------------------------------------
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_product (
                product_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                integration_id VARCHAR(100) UNIQUE,
                product_name VARCHAR(255),
                category_name VARCHAR(255)
            );
        """))
        
        conn.execute(text("""
            INSERT INTO dim_product (integration_id, product_name, category_name)
            SELECT 'CHN_' || t.trackid, t.name, g.name 
            FROM stg_chinook_track t
            LEFT JOIN stg_chinook_genre g ON t.genreid = g.genreid
            ON CONFLICT (integration_id) DO UPDATE 
            SET product_name = EXCLUDED.product_name, category_name = EXCLUDED.category_name;
        """))
        
        conn.execute(text("""
            INSERT INTO dim_product (integration_id, product_name, category_name)
            SELECT 'NWD_' || p.productid, p.productname, c.categoryname 
            FROM stg_northwind_products p
            LEFT JOIN stg_northwind_categories c ON p.categoryid = c.categoryid
            ON CONFLICT (integration_id) DO UPDATE 
            SET product_name = EXCLUDED.product_name, category_name = EXCLUDED.category_name;
        """))
        print("✅ DimProduct Updated (UPSERT)")

        # ---------------------------------------------------------
        # 4. สร้างตาราง FactSales (และ Reload ข้อมูล)
        # ---------------------------------------------------------
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                sales_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                date_key INT,
                product_key INT,
                customer_key INT,
                quantity NUMERIC,
                unit_price NUMERIC,
                sales_amount NUMERIC,
                source_system VARCHAR(50)
            );
        """))
        
        # เคลียร์ Fact Table เก่าก่อนเพื่อความ Idempotent (รันซ้ำกี่รอบข้อมูลก็ไม่เบิ้ล)
        conn.execute(text("TRUNCATE TABLE fact_sales RESTART IDENTITY;"))
        
        # รวมยอดขาย Chinook
        conn.execute(text("""
            INSERT INTO fact_sales (date_key, product_key, customer_key, quantity, unit_price, sales_amount, source_system)
            SELECT 
                CAST(TO_CHAR(CAST(i.invoicedate AS TIMESTAMP), 'YYYYMMDD') AS INT) AS date_key,
                dp.product_key,
                dc.customer_key,
                il.quantity,
                il.unitprice,
                (il.quantity * il.unitprice) AS sales_amount,
                'Chinook'
            FROM stg_chinook_invoiceline il
            JOIN stg_chinook_invoice i ON il.invoiceid = i.invoiceid
            LEFT JOIN dim_product dp ON dp.integration_id = 'CHN_' || il.trackid
            LEFT JOIN dim_customer dc ON dc.integration_id = 'CHN_' || i.customerid;
        """))

        # รวมยอดขาย Northwind
        conn.execute(text("""
            INSERT INTO fact_sales (date_key, product_key, customer_key, quantity, unit_price, sales_amount, source_system)
            SELECT 
                CAST(TO_CHAR(CAST(o.orderdate AS TIMESTAMP), 'YYYYMMDD') AS INT) AS date_key,
                dp.product_key,
                dc.customer_key,
                od.quantity,
                od.unitprice,
                (od.quantity * od.unitprice) AS sales_amount,
                'Northwind'
            FROM stg_northwind_order_details od
            JOIN stg_northwind_orders o ON od.orderid = o.orderid
            LEFT JOIN dim_product dp ON dp.integration_id = 'NWD_' || od.productid
            LEFT JOIN dim_customer dc ON dc.integration_id = 'NWD_' || o.customerid;
        """))
        print("✅ FactSales Reloaded")

        # ---------------------------------------------------------
        # 5. ลบตาราง Staging เพื่อประหยัดพื้นที่ Postgres (Housekeeping)
        # ---------------------------------------------------------
        for stg_table in loaded_stg_tables:
            conn.execute(text(f"DROP TABLE IF EXISTS {stg_table};"))
        print("🧹 Cleaned up staging tables.")

# ==========================================
#  Orchestration Flow
# ==========================================
@flow(name="End-to-End OmniCorp Pipeline (Data Lakehouse)")
def main_flow():
    # 1. โหลดข้อมูลลงโฟลเดอร์เครื่องเป็น Parquet (ครบทั้ง 24 ตาราง)
    parquet_files = extract_to_bronze_parquet()
    
    # 2. ทำ ELT แปลงเป็น Star Schema ใน Postgres (ใช้แค่ 10 ตารางที่จำเป็น)
    if parquet_files:
        load_staging_and_transform_elt(parquet_files)

if __name__ == "__main__":
    main_flow()