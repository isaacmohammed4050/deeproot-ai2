FROM python:3.12-slim

LABEL maintainer="Isaac <isaacmohammed4050@gmail.com>"
LABEL description="DeepRoot AI - AI-powered RCA over OpenTelemetry data"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
