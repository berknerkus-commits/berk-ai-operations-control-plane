FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY scripts ./scripts
EXPOSE 8000
CMD ["uvicorn","app:api","--host","0.0.0.0","--port","8000"]
