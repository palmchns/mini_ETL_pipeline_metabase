FROM python:3.10-slim

WORKDIR /app

# ติดตั้ง dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ก๊อปปี้โค้ดทั้งหมดเข้า Container
COPY . .

# กำหนดให้รัน Pipeline อัตโนมัติเมื่อ Start (แก้ชื่อไฟล์ให้ตรง)
CMD ["python", "etl_pipeline.py"]