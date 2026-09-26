# Runs the silver-to-gold KPI aggregation as its own long-lived service.
FROM python:3.11-slim

WORKDIR /app

COPY build_gold.py .

RUN useradd --create-home --shell /bin/bash golduser \
    && mkdir -p /app/silver /app/gold \
    && chown -R golduser:golduser /app
USER golduser

ENTRYPOINT ["python", "build_gold.py"]
CMD ["--input-root", "silver", "--output-root", "gold", "--interval", "60"]