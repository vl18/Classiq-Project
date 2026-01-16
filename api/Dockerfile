FROM python:3.11-slim

WORKDIR /app

# System deps for qiskit-aer (uses prebuilt wheels for manylinux; keep minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY common /app/common
COPY api /app/api

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
