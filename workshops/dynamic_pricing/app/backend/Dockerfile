FROM python:3.12.13-slim@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36
WORKDIR /opt/pricing
ENV PRICING_LAB_ROOT=/opt/pricing
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY versions.lock /tmp/versions.lock
RUN pip install --no-cache-dir -r /tmp/versions.lock
COPY app/backend app/backend
COPY generator generator
COPY pricing pricing
COPY infra infra
RUN pip install --no-cache-dir --no-deps app/backend && useradd --uid 10001 --create-home workshop
COPY sql sql
COPY fixtures fixtures
COPY agent agent
COPY reports reports
RUN mkdir -p .run/retail-spool reports/exports && chown -R workshop:workshop .run reports/exports
USER workshop
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health',timeout=2)"
CMD ["uvicorn", "pricing_lab.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
