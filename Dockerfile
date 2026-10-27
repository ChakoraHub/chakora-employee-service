FROM python:3.9-slim
LABEL org.opencontainers.image.source=https://github.com/ChakoraHub/chakora-employee-service
WORKDIR /app
COPY requirements-docker.txt .
RUN pip install --no-cache-dir -r requirements-docker.txt
COPY . .
EXPOSE 8002
CMD ["python", "-m", "uvicorn", "employee_service:app", "--host", "0.0.0.0", "--port", "8002"]
