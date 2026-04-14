FROM python:3.10-slim

WORKDIR /app

# ติดตั้ง dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ก๊อปปี้โค้ดทั้งหมดเข้า Container
COPY . .

# กำหนดให้รัน Pipeline อัตโนมัติเมื่อ Start (หรือคอมเมนต์ไว้ถ้าจะสั่งรันเอง)
# CMD ["python", "etl_pipeline.py"]