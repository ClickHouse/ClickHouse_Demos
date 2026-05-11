FROM python:3.12-slim
WORKDIR /app
COPY generate.py .
RUN mkdir -p /var/log/generators
CMD ["python", "-u", "generate.py"]
