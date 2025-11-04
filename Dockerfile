FROM python:3.10-slim

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends git gcc

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . /app

ENTRYPOINT ["python", "-m"]
