FROM python:3.12.13-slim@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36
WORKDIR /opt/pricing
ENV PRICING_LAB_ROOT=/opt/pricing
COPY versions.lock /tmp/versions.lock
RUN pip install --no-cache-dir -r /tmp/versions.lock
COPY app/backend app/backend
COPY generator generator
COPY pricing pricing
COPY infra infra
RUN pip install --no-cache-dir --no-deps app/backend && useradd --uid 10001 --create-home workshop
COPY app/loadgen/writer.py writer.py
USER workshop
CMD ["python", "writer.py"]
