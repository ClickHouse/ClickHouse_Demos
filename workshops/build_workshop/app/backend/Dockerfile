FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install OpenTelemetry auto-instrumentation for the libraries present in the
# image (FastAPI, logging, etc.). Safe no-op if OTel is not used at runtime;
# actual instrumentation only activates when launched via opentelemetry-instrument
# (see entrypoint.sh, gated on OTEL_ENABLED).
RUN opentelemetry-bootstrap -a install

COPY app /app/app
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

# entrypoint.sh chooses plain uvicorn vs. opentelemetry-instrument based on OTEL_ENABLED.
CMD ["/app/entrypoint.sh"]

