import pandas as pd
from sqlalchemy import create_engine, text
import os
import requests
import tempfile
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

# ==========================================
# 🥉 Task 1: Extract & Load to Bronze (Postgres Raw)
# ==========================================
@task(log_prints=True, retries=2)
def load_raw_to_postgres():
    """
    ดาวน์โหลด SQLite ลง Temp File ใน Docker -> อ่านด้วย Pandas -> โหลดเข้าตาราง _raw_ ใน Postgres
    """
    sources = {
        "chinook": ("https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite", 
                    ["Customer", "Track", "Genre", "Invoice", "InvoiceLine"]),
        "nw": ("https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db", 
               ["Customers", "Products", "Categories", "Orders", "Order Details"])
    }
    
    target_engine = create_engine(PG_URI)

    for prefix, (url, tables) in sources.items():
        print(f"📥 Downloading {prefix} database...")
        r = requests.get(url)
        
        # ใช้ Temp file ซึ่งจะหายไปเองเมื่อออกจาก block ไม่รกพื้นที่ใน Docker
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp:
            tmp.write(r.content)
            tmp.flush()
            
            src_engine = create_engine(f"sqlite:///{tmp.name}")
            
            for table in tables:
                print(f"⏳ Extracting raw {table}...")
                df = pd.read_sql(f'SELECT * FROM "{table}"', src_engine)
                
                # ทำความสะอาดชื่อคอลัมน์ให้อ่านง่าย
                df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                
                # แปลงวันที่
                for col in df.columns:
                    if 'date' in col.lower() or 'time' in col.lower():
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # โหลดเข้าตาราง raw ใน Postgres (รันครั้งเดียว ใช้ replace ได้เลย)
                target_name = f"{prefix}_raw_{table.lower().replace(' ', '_')}"
                df.to_sql(target_name, target_engine, if_exists="replace", index=False)
                print(f"✅ {target_name} loaded: {len(df)} rows")

# ==========================================
# 🥇 Task 2: In-Database Transform (Star Schema)
# ==========================================
@task(log_prints=True)
def transform_star_schema_in_db():
    """
    ใช้ SQL ประมวลผลจากตาราง Raw เพื่อสร้าง Star Schema ใน Postgres โดยตรง
    รวดเร็ว และไม่กิน RAM ของ Docker Container
    """
    print("⏳ Executing In-Database Transformation for Star Schema...")
    target_engine = create_engine(PG_URI)
    
    with target_engine.begin() as conn:
        
        # 1. สร้างและโหลด DimCustomer (ใช้ DROP & CREATE สำหรับ One-time run)
        conn.execute(text("DROP TABLE IF EXISTS dim_customer CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dim_customer (
                customer_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                integration_id VARCHAR(100),
                customer_name VARCHAR(255),
                company VARCHAR(255),
                city VARCHAR(100),
                country VARCHAR(100)
            );
        """))
        conn.execute(text("""
            INSERT INTO dim_customer (integration_id, customer_name, company, city, country)
            SELECT 'CHN_' || customerid, firstname || ' ' || lastname, company, city, country FROM chinook_raw_customer
            UNION ALL
            SELECT 'NWD_' || customerid, contactname, companyname, city, country FROM nw_raw_customers;
        """))
        print("✅ dim_customer created")

        # 2. สร้างและโหลด DimProduct
        conn.execute(text("DROP TABLE IF EXISTS dim_product CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dim_product (
                product_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                integration_id VARCHAR(100),
                product_name VARCHAR(255),
                category_name VARCHAR(255)
            );
        """))
        conn.execute(text("""
            INSERT INTO dim_product (integration_id, product_name, category_name)
            SELECT 'CHN_' || t.trackid, t.name, g.name 
            FROM chinook_raw_track t LEFT JOIN chinook_raw_genre g ON t.genreid = g.genreid
            UNION ALL
            SELECT 'NWD_' || p.productid, p.productname, c.categoryname 
            FROM nw_raw_products p LEFT JOIN nw_raw_categories c ON p.categoryid = c.categoryid;
        """))
        print("✅ dim_product created")

        # 3. สร้างและโหลด FactSales
        conn.execute(text("DROP TABLE IF EXISTS fact_sales CASCADE;"))
        conn.execute(text("""
            CREATE TABLE fact_sales (
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
        # ยอดขาย Chinook
        conn.execute(text("""
            INSERT INTO fact_sales (date_key, product_key, customer_key, quantity, unit_price, sales_amount, source_system)
            SELECT 
                CAST(TO_CHAR(CAST(i.invoicedate AS TIMESTAMP), 'YYYYMMDD') AS INT),
                dp.product_key, dc.customer_key, il.quantity, il.unitprice,
                (il.quantity * il.unitprice), 'Chinook'
            FROM chinook_raw_invoiceline il
            JOIN chinook_raw_invoice i ON il.invoiceid = i.invoiceid
            LEFT JOIN dim_product dp ON dp.integration_id = 'CHN_' || il.trackid
            LEFT JOIN dim_customer dc ON dc.integration_id = 'CHN_' || i.customerid;
        """))
        # ยอดขาย Northwind
        conn.execute(text("""
            INSERT INTO fact_sales (date_key, product_key, customer_key, quantity, unit_price, sales_amount, source_system)
            SELECT 
                CAST(TO_CHAR(CAST(o.orderdate AS TIMESTAMP), 'YYYYMMDD') AS INT),
                dp.product_key, dc.customer_key, od.quantity, od.unitprice,
                (od.quantity * od.unitprice), 'Northwind'
            FROM nw_raw_order_details od
            JOIN nw_raw_orders o ON od.orderid = o.orderid
            LEFT JOIN dim_product dp ON dp.integration_id = 'NWD_' || od.productid
            LEFT JOIN dim_customer dc ON dc.integration_id = 'NWD_' || o.customerid;
        """))
        print("✅ fact_sales created")

# ==========================================
#  Orchestration Flow
# ==========================================
@flow(name="One-Time OmniCorp Initial Load (Docker Optimized)")
def main_flow():
    # 1. ดูดข้อมูลลงตาราง _raw_ ใน Postgres (ผ่าน Temp file เพื่อไม่ให้ Docker รก)
    load_raw_to_postgres()
    
    # 2. ให้ Postgres จัดการ Join และสร้าง Star Schema ให้เอง (ประหยัด RAM)
    transform_star_schema_in_db()

if __name__ == "__main__":
    main_flow()