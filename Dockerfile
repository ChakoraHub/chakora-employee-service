FROM python:3.10-slim

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for container security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Copy requirements file for layer tracking
COPY requirements.txt /app/requirements.txt

# Install dependencies using only verified binary packages with locked versions
RUN pip install --no-cache-dir --only-binary :all: --upgrade pip==25.0.1 && \
    pip install --no-cache-dir --only-binary :all: \
        fastapi==0.115.8 \
        uvicorn==0.34.0 \
        pydantic==2.10.6 \
        boto3==1.36.19 \
        oracledb==2.5.1 \
        python-dotenv==1.0.1 \
        python-multipart==0.0.20

# Copy application code
COPY employee_service.py /app/employee_service.py

# Switch to non-root user
USER appuser

# Expose service port
EXPOSE 8002

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:8002/health || exit 1

# Command to run employee service
CMD ["uvicorn", "employee_service:app", "--host", "0.0.0.0", "--port", "8002"]
