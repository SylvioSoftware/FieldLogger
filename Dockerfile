FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000
EXPOSE 9001
EXPOSE 9002

CMD ["python", "main.py"]
