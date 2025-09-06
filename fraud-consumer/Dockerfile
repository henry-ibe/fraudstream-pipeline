FROM python:3.11-slim
RUN pip install fastapi uvicorn boto3 psycopg2-binary
WORKDIR /app
COPY app/ /app/
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8080"]

