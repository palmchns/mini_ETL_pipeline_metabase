import pandas as pd
from sqlalchemy import create_engine
import os
import requests
from prefect import task, flow

# ตั้งค่า Database Connection (ดึงจาก Environment ที่ฝังใน Docker Compose)
DB_HOST = "postgres_dw" if os.getenv("IN_DOCKER") else "localhost"
DB_USER = os.getenv("POSTGRES_USER", "engineer")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password123")
DB_DB = os.getenv("POSTGRES_DB", "data_warehouse")
DB_PORT = "5432" if os.getenv("IN_DOCKER") else "5435"

PG_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_DB}"

@task(log_prints=True, retries=3)
def download_db(force_download=True):
    sources = {
        "chinook.db": "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite",
        "northwind.db": "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db"
    }
    
    for name, url in sources.items():
        if force_download and os.path.exists(name):
            os.remove(name)
            print(f"🗑️ Removed old {name} to force a fresh download.")
            
        if not os.path.exists(name):
            print(f"📥 Downloading fresh {name}...")
            r = requests.get(url)
            with open(name, "wb") as f:
                f.write(r.content)
            print(f"✅ Downloaded {name} successfully.")
        else:
            print(f"✅ {name} already exists and is ready to use.")
            
    return list(sources.keys())

@task(log_prints=True)
def load_raw_tables(db_file, tables, prefix):
    db_path = os.path.abspath(db_file)
    src_engine = create_engine(f"sqlite:///{db_path}")
    target_engine = create_engine(PG_URI)
    
    for table in tables:
        print(f"⏳ Extracting raw {table}...")
        df = pd.read_sql(f'SELECT * FROM "{table}"', src_engine)
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]
        for col in df.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        target_name = f"{prefix}_raw_{table.lower().replace(' ', '_')}"
        df.to_sql(target_name, target_engine, if_exists="replace", index=False)
        print(f"✅ {target_name} loaded: {len(df)} rows")

@task(log_prints=True)
def transform_omnicorp_star_schema():
    print("⏳ Integrating Chinook and Northwind into Unified Star Schema...")
    cnk_engine = create_engine(f"sqlite:///{os.path.abspath('chinook.db')}")
    nwd_engine = create_engine(f"sqlite:///{os.path.abspath('northwind.db')}")
    target_engine = create_engine(PG_URI)

    # ==========================================
    # 1. สร้าง DimCustomer (รวมลูกค้า 2 บริษัท)
    # ==========================================
    c_cnk = pd.read_sql('SELECT CustomerId, FirstName, LastName, Company, City, Country FROM Customer', cnk_engine)
    c_nwd = pd.read_sql('SELECT CustomerID, ContactName, CompanyName, City, Country FROM Customers', nwd_engine)

    c_cnk['CustomerName'] = c_cnk['FirstName'] + ' ' + c_cnk['LastName']
    c_cnk['IntegrationID'] = 'CHN_' + c_cnk['CustomerId'].astype(str)
    
    c_nwd['IntegrationID'] = 'NWD_' + c_nwd['CustomerID'].astype(str)
    c_nwd.rename(columns={'ContactName': 'CustomerName', 'CompanyName': 'Company'}, inplace=True)

    dim_customer = pd.concat([
        c_cnk[['IntegrationID', 'CustomerName', 'Company', 'City', 'Country']],
        c_nwd[['IntegrationID', 'CustomerName', 'Company', 'City', 'Country']]
    ], ignore_index=True)
    # สร้าง Surrogate Key (CustomerKey)
    dim_customer.insert(0, 'CustomerKey', range(1, 1 + len(dim_customer)))
    dim_customer.to_sql('dim_customer', target_engine, if_exists='replace', index=False)
    print("✅ dim_customer created")

    # ==========================================
    # 2. สร้าง DimProduct (รวมเพลงและอาหาร)
    # ==========================================
    p_cnk = pd.read_sql('SELECT t.TrackId, t.Name as TrackName, g.Name as Genre FROM Track t LEFT JOIN Genre g ON t.GenreId = g.GenreId', cnk_engine)
    p_nwd = pd.read_sql('SELECT p.ProductID, p.ProductName, c.CategoryName FROM Products p LEFT JOIN Categories c ON p.CategoryID = c.CategoryID', nwd_engine)

    p_cnk['IntegrationID'] = 'CHN_' + p_cnk['TrackId'].astype(str)
    p_cnk.rename(columns={'TrackName': 'ProductName', 'Genre': 'CategoryName'}, inplace=True)
    
    p_nwd['IntegrationID'] = 'NWD_' + p_nwd['ProductID'].astype(str)

    dim_product = pd.concat([
        p_cnk[['IntegrationID', 'ProductName', 'CategoryName']],
        p_nwd[['IntegrationID', 'ProductName', 'CategoryName']]
    ], ignore_index=True)
    # สร้าง Surrogate Key (ProductKey)
    dim_product.insert(0, 'ProductKey', range(1, 1 + len(dim_product)))
    dim_product.to_sql('dim_product', target_engine, if_exists='replace', index=False)
    print("✅ dim_product created")

    # ==========================================
    # 3. สร้าง DimDate (ดึงวันที่จากทั้ง 2 ระบบ)
    # ==========================================
    inv = pd.read_sql('SELECT InvoiceDate FROM Invoice', cnk_engine)
    ord = pd.read_sql('SELECT OrderDate FROM Orders', nwd_engine)
    
    all_dates = pd.concat([inv['InvoiceDate'], ord['OrderDate']]).dropna()
    all_dates = pd.to_datetime(all_dates)
    
    dim_date = pd.DataFrame({'FullDate': all_dates.dt.date.unique()})
    dim_date['DateKey'] = dim_date['FullDate'].astype(str).str.replace('-', '').astype(int)
    dim_date['Year'] = pd.to_datetime(dim_date['FullDate']).dt.year
    dim_date['Quarter'] = pd.to_datetime(dim_date['FullDate']).dt.quarter
    dim_date['Month'] = pd.to_datetime(dim_date['FullDate']).dt.month
    dim_date['MonthName'] = pd.to_datetime(dim_date['FullDate']).dt.strftime('%B')
    
    dim_date.to_sql('dim_date', target_engine, if_exists='replace', index=False)
    print("✅ dim_date created")

    # ==========================================
    # 4. สร้าง FactSales
    # ==========================================
    # ฝั่ง Chinook
    inv_line = pd.read_sql('SELECT InvoiceId, TrackId, UnitPrice, Quantity FROM InvoiceLine', cnk_engine)
    inv_df = pd.read_sql('SELECT InvoiceId, CustomerId, InvoiceDate FROM Invoice', cnk_engine)
    sales_cnk = inv_line.merge(inv_df, on='InvoiceId')
    sales_cnk['Cust_IntegrationID'] = 'CHN_' + sales_cnk['CustomerId'].astype(str)
    sales_cnk['Prod_IntegrationID'] = 'CHN_' + sales_cnk['TrackId'].astype(str)
    sales_cnk['OrderDate'] = pd.to_datetime(sales_cnk['InvoiceDate'])
    sales_cnk['SourceSystem'] = 'Chinook'

    # ฝั่ง Northwind
    ord_det = pd.read_sql('SELECT OrderID, ProductID, UnitPrice, Quantity FROM "Order Details"', nwd_engine)
    ord_df = pd.read_sql('SELECT OrderID, CustomerID, OrderDate FROM Orders', nwd_engine)
    sales_nwd = ord_det.merge(ord_df, on='OrderID')
    sales_nwd['Cust_IntegrationID'] = 'NWD_' + sales_nwd['CustomerID'].astype(str)
    sales_nwd['Prod_IntegrationID'] = 'NWD_' + sales_nwd['ProductID'].astype(str)
    sales_nwd['OrderDate'] = pd.to_datetime(sales_nwd['OrderDate'])
    sales_nwd['SourceSystem'] = 'Northwind'

    # นำ Fact มารวมกัน
    fact_all = pd.concat([
        sales_cnk[['OrderDate', 'Cust_IntegrationID', 'Prod_IntegrationID', 'Quantity', 'UnitPrice', 'SourceSystem']],
        sales_nwd[['OrderDate', 'Cust_IntegrationID', 'Prod_IntegrationID', 'Quantity', 'UnitPrice', 'SourceSystem']]
    ])
    
    # คำนวณยอดขาย และสร้าง DateKey
    fact_all['SalesAmount'] = fact_all['Quantity'] * fact_all['UnitPrice']
    fact_all['DateKey'] = fact_all['OrderDate'].dt.strftime('%Y%m%d').astype(int)

    # Map กลับไปหา Surrogate Key ของ DimCustomer และ DimProduct
    fact_all = fact_all.merge(dim_customer[['IntegrationID', 'CustomerKey']], left_on='Cust_IntegrationID', right_on='IntegrationID', how='left')
    fact_all = fact_all.merge(dim_product[['IntegrationID', 'ProductKey']], left_on='Prod_IntegrationID', right_on='IntegrationID', how='left')

    # เลือกเฉพาะคอลัมน์ที่ใช้งานจริงลง Fact Table
    fact_sales = fact_all[['DateKey', 'ProductKey', 'CustomerKey', 'Quantity', 'UnitPrice', 'SalesAmount', 'SourceSystem']]
    fact_sales.insert(0, 'SalesKey', range(1, 1 + len(fact_sales))) # สร้าง Running Number ให้บรรทัดยอดขาย
    
    fact_sales.to_sql('fact_sales', target_engine, if_exists='replace', index=False)
    print("✅ fact_sales created")

@flow(name="End-to-End OmniCorp Pipeline")
def main_flow():
    # 1. โหลดฐานข้อมูล SQLite
    download_db()
    
    # 2. โหลดข้อมูลแบบ Raw ลง Data Lake Layer (เก็บไว้เผื่ออ้างอิง)
    cnk_tables = ["Album", "Artist", "Customer", "Employee", "Genre", "Invoice", "InvoiceLine", "MediaType", "Playlist", "PlaylistTrack", "Track"]
    nw_tables = ["Categories", "Customers", "Employees", "Order Details", "Orders", "Products"]
    
    load_raw_tables("chinook.db", cnk_tables, "cnk")
    load_raw_tables("northwind.db", nw_tables, "nw")
    
    # 3. สร้าง Data Warehouse (Star Schema) แบบรวมศูนย์ OmniCorp
    transform_omnicorp_star_schema()

if __name__ == "__main__":
    main_flow()