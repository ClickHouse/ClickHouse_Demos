FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collector ./collector

RUN useradd --create-home --uid 10001 workshop
USER workshop

EXPOSE 8090

CMD ["python", "-m", "collector"]
