FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir fastapi uvicorn[standard]
RUN pip install --no-cache-dir ollama psycopg2-binary python-dotenv pydantic requests
RUN pip install --no-cache-dir sentence-transformers
RUN pip install --no-cache-dir chromadb

COPY . .

RUN mkdir -p /app/data/chroma

EXPOSE 5000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]