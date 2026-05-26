#!/bin/bash
set -e

# ==============================================================================
# PROJECT: OmniCorp ELT Data Stack (Full Kimball Star Schema Edition)
# DESCRIPTION: Production-Ready Setup (Cross-Platform / Windows & Mac Compatible)
# ==============================================================================

PROJECT_DIR="omnicorp_data_stack"

echo "🚀 Initializing Project: $PROJECT_DIR"
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

# ==========================================
# 0. Auto-Cleanup
# ==========================================
if [ -f "docker-compose.yml" ]; then
    echo "🧹 Cleaning up existing containers..."
    docker compose down 2>/dev/null || true
fi

# ==========================================
# 1. Generate requirements.txt
# ==========================================
echo "📦 Generating requirements.txt..."
cat << 'EOF' > requirements.txt
prefect>=3.0.0
pandas
sqlalchemy
psycopg2-binary
requests
EOF

# ==========================================
# 2. Generate ELT Pipeline Script
# ==========================================
echo "🐍 Generating elt_pipeline.py..."
cat << 'EOF' > elt_pipeline.py
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

# ==========================================
# 🥉 Task 1: Extract & Load to Bronze
# ==========================================
@task(log_prints=True, retries=2)
def load_raw_to_postgres():
    sources = {
        "chinook": "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite",
        "nw": "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db"
    }
    
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
                    if 'date' in col.lower() or 'time' in col.lower():
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                target_name = f"{prefix}_raw_{table.lower().replace(' ', '_')}"
                df.to_sql(target_name, target_engine, if_exists="replace", index=False)
                print(f"✅ {target_name} loaded: {len(df)} rows")
        finally:
            # Clean up temp file (สำคัญสำหรับ Windows)
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

# ==========================================
# 🥇 Task 2: In-Database Transform
# ==========================================
@task(log_prints=True)
def transform_star_schema_in_db():
    print("⏳ Executing In-Database Transformation for Full Star Schema...")
    target_engine = create_engine(PG_URI)
    
    with target_engine.begin() as conn:
        # 1. DimDate
        conn.execute(text("DROP TABLE IF EXISTS dimdate CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dimdate AS
            SELECT
                TO_CHAR(datum, 'YYYYMMDD')::INT AS datekey,
                datum::DATE AS date,
                EXTRACT(YEAR FROM datum)::INT AS year,
                EXTRACT(QUARTER FROM datum)::INT AS quarter,
                EXTRACT(MONTH FROM datum)::INT AS month,
                EXTRACT(WEEK FROM datum)::INT AS week,
                EXTRACT(DAY FROM datum)::INT AS day,
                EXTRACT(ISODOW FROM datum)::INT AS dayofweek
            FROM (SELECT generate_series('2000-01-01'::DATE, '2030-12-31'::DATE, '1 day') AS datum) d;
            ALTER TABLE dimdate ADD PRIMARY KEY (datekey);
        """))
        
        # 2. DimSource_System
        conn.execute(text("DROP TABLE IF EXISTS dimsource_system CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dimsource_system (
                sourcesystemkey VARCHAR PRIMARY KEY,
                source_system_name VARCHAR,
                description VARCHAR
            );
            INSERT INTO dimsource_system VALUES ('CHN', 'Chinook', 'Digital Music Store'), ('NWD', 'Northwind', 'Physical Goods Store');
        """))

        # 3. DimCustomer
        conn.execute(text("DROP TABLE IF EXISTS dimcustomer CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dimcustomer (
                customerkey INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                customer_id VARCHAR(100),
                company VARCHAR(255),
                address VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(100),
                postalcode VARCHAR(50),
                country VARCHAR(100),
                phone VARCHAR(100),
                email VARCHAR(255),
                fax VARCHAR(100)
            );
        """))
        conn.execute(text("""
            INSERT INTO dimcustomer (customer_id, company, address, city, state, postalcode, country, phone, email, fax)
            SELECT 'CHN_' || customerid, company, address, city, state, postalcode, country, phone, email, fax FROM chinook_raw_customer
            UNION ALL
            SELECT 'NWD_' || customerid, companyname, address, city, region, postalcode, country, phone, NULL, fax FROM nw_raw_customers;
        """))

        # 4. DimEmployee
        conn.execute(text("DROP TABLE IF EXISTS dimemployee CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dimemployee (
                employeekey INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                employee_id VARCHAR(100),
                firstname VARCHAR(255),
                lastname VARCHAR(255),
                title VARCHAR(255),
                hiredate TIMESTAMP,
                country VARCHAR(100)
            );
        """))
        conn.execute(text("""
            INSERT INTO dimemployee (employee_id, firstname, lastname, title, hiredate, country)
            SELECT 'CHN_' || employeeid, firstname, lastname, title, hiredate, country FROM chinook_raw_employee
            UNION ALL
            SELECT 'NWD_' || employeeid, firstname, lastname, title, hiredate, country FROM nw_raw_employees;
        """))

        # 5. DimProduct
        conn.execute(text("DROP TABLE IF EXISTS dimproduct CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dimproduct (
                productkey INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                product_id VARCHAR(100),
                productname VARCHAR(255),
                genrekey INT,
                genrename VARCHAR(255),
                categorykey INT,
                categoryname VARCHAR(255),
                composer VARCHAR(255)
            );
        """))
        conn.execute(text("""
            INSERT INTO dimproduct (product_id, productname, genrekey, genrename, categorykey, categoryname, composer)
            SELECT 'CHN_' || t.trackid, t.name, t.genreid, g.name, NULL, NULL, t.composer 
            FROM chinook_raw_track t LEFT JOIN chinook_raw_genre g ON t.genreid = g.genreid
            UNION ALL
            SELECT 'NWD_' || p.productid, p.productname, NULL, NULL, p.categoryid, c.categoryname, NULL 
            FROM nw_raw_products p LEFT JOIN nw_raw_categories c ON p.categoryid = c.categoryid;
        """))

        # 6. FactSales
        conn.execute(text("DROP TABLE IF EXISTS factsales CASCADE;"))
        conn.execute(text("""
            CREATE TABLE factsales (
                factsalekey INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                datekey INT,
                customerkey INT,
                employeekey INT,
                productkey INT,
                sourcesystemkey VARCHAR(50),
                salesquantity INT,
                unitprice NUMERIC,
                totalamount NUMERIC
            );
        """))
        conn.execute(text("""
            INSERT INTO factsales (datekey, customerkey, employeekey, productkey, sourcesystemkey, salesquantity, unitprice, totalamount)
            SELECT 
                CAST(TO_CHAR(CAST(i.invoicedate AS TIMESTAMP), 'YYYYMMDD') AS INT),
                dc.customerkey, de.employeekey, dp.productkey, 'CHN',
                il.quantity, il.unitprice, (il.quantity * il.unitprice)
            FROM chinook_raw_invoiceline il 
            JOIN chinook_raw_invoice i ON il.invoiceid = i.invoiceid 
            LEFT JOIN dimproduct dp ON dp.product_id = 'CHN_' || il.trackid 
            LEFT JOIN dimcustomer dc ON dc.customer_id = 'CHN_' || i.customerid
            LEFT JOIN chinook_raw_customer crc ON i.customerid = crc.customerid
            LEFT JOIN dimemployee de ON de.employee_id = 'CHN_' || crc.supportrepid;
        """))
        conn.execute(text("""
            INSERT INTO factsales (datekey, customerkey, employeekey, productkey, sourcesystemkey, salesquantity, unitprice, totalamount)
            SELECT 
                CAST(TO_CHAR(CAST(o.orderdate AS TIMESTAMP), 'YYYYMMDD') AS INT),
                dc.customerkey, de.employeekey, dp.productkey, 'NWD',
                od.quantity, od.unitprice, (od.quantity * od.unitprice)
            FROM nw_raw_order_details od 
            JOIN nw_raw_orders o ON od.orderid = o.orderid 
            LEFT JOIN dimproduct dp ON dp.product_id = 'NWD_' || od.productid 
            LEFT JOIN dimcustomer dc ON dc.customer_id = 'NWD_' || o.customerid
            LEFT JOIN dimemployee de ON de.employee_id = 'NWD_' || o.employeeid;
        """))
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
    # หน่วงเวลาเล็กน้อยให้มั่นใจว่า Prefect API บูตเสร็จสมบูรณ์
    time.sleep(10) 
    
    try:
        main_flow()
    except Exception as e:
        print(f"⚠️ Initial run failed (maybe Prefect isn't fully up yet): {e}")
        print("Re-throwing to trigger container restart...")
        raise
    
    print("📡 Deploying to Prefect Server and waiting for Quick Runs...")
    main_flow.serve(name="OmniCorp-Manual-Trigger", tags=["ELT", "DataWarehouse"])
EOF

# ==========================================
# 3. Generate Dockerfile
# ==========================================
echo "🐳 Generating Dockerfile..."
cat << 'EOF' > Dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# 📌 The Ultimate Windows Fix: ลบ Carriage Returns (\r) ออกจากไฟล์ Python
RUN sed -i 's/\r$//' elt_pipeline.py
CMD ["python", "elt_pipeline.py"]
EOF

# ==========================================
# 4. Generate docker-compose.yml
# ==========================================
echo "🐙 Generating docker-compose.yml..."
cat << 'EOF' > docker-compose.yml
version: '3.8'

services:
  omni_postgres:
    image: postgres:15-alpine
    container_name: omni_postgres
    ports:
      - "5440:5432"
    environment:
      - POSTGRES_USER=engineer
      - POSTGRES_PASSWORD=password123
      - POSTGRES_DB=data_warehouse
    volumes:
      - omni_dw_data:/var/lib/postgresql/data
    networks:
      - omni_network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U engineer -d data_warehouse"]
      interval: 10s
      timeout: 5s
      retries: 5

  omni_metabase:
    image: metabase/metabase:latest
    container_name: omni_metabase
    user: "root"
    ports:
      - "3050:3000"
    depends_on:
      omni_postgres:
        condition: service_healthy
    environment:                           
      - MB_DB_FILE=/metabase-data/metabase.db 
    volumes:                               
      - omni_metabase_data:/metabase-data
    networks:
      - omni_network

  omni_prefect:
    image: prefecthq/prefect:3-latest
    container_name: omni_prefect
    command: prefect server start --host 0.0.0.0
    environment:
      - PREFECT_UI_API_URL=http://localhost:4205/api
    ports:
      - "4205:4200"
    networks:
      - omni_network
    healthcheck:
      # เช็กว่า API ของ Prefect ตอบสนองแล้วหรือยัง
      test: ["CMD", "curl", "-f", "http://127.0.0.1:4200/api/health"]
      interval: 10s
      timeout: 5s
      retries: 10

  omni_jupyter:
    image: jupyter/scipy-notebook:latest
    container_name: omni_jupyter
    user: root
    environment:
      - JUPYTER_TOKEN=prefect
      - IN_DOCKER=true
    volumes:
      - .:/home/jovyan/work
    ports:
      - "8890:8888"
    networks:
      - omni_network

  omni_elt_runner:
    build: .
    container_name: omni_elt_runner
    # 📌 ถ้าพังจาก Network ให้พยายามรันใหม่เรื่อยๆ จนกว่าจะสำเร็จ
    restart: on-failure
    depends_on:
      omni_postgres:
        condition: service_healthy
    environment:
      - PREFECT_API_URL=http://omni_prefect:4200/api
      - IN_DOCKER=true
      - POSTGRES_USER=engineer
      - POSTGRES_PASSWORD=password123
      - POSTGRES_DB=data_warehouse
    networks:
      - omni_network

volumes:
  omni_dw_data:
  omni_metabase_data: 

networks:
  omni_network:
    driver: bridge
EOF

# ==========================================
# 5. Execute Docker Compose
# ==========================================
echo "-------------------------------------------------------"
echo "✨ All files generated successfully! ✨"
echo "Spinning up the infrastructure..."
echo "-------------------------------------------------------"

docker compose up -d

echo ""
echo "🎉 DEPLOYMENT COMPLETE!"
echo "-------------------------------------------------------"
echo "💡 The ELT pipeline is currently running its initial load."
echo "If it fails on startup (e.g. waiting for Prefect), it will AUTO-RESTART until successful."
echo "📊 Metabase UI:        http://localhost:3050"
echo "🌊 Prefect UI:         http://localhost:4205"
echo "📓 Jupyter Notebook:  http://localhost:8890 (Token: prefect)"
echo "-------------------------------------------------------"