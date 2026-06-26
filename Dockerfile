FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# The Ultimate Windows Fix: ลบ Carriage Returns (\r) ออกจากไฟล์ Python เพื่อป้องกันปัญหาการรันใน Linux
RUN sed -i 's/\r$//' elt_pipeline.py
CMD ["python", "elt_pipeline.py"]
