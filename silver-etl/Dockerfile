# Runs the raw-to-silver cleaning step as its own long-lived service.
FROM python:3.11-slim

WORKDIR /app

COPY clean_to_silver.py .

RUN useradd --create-home --shell /bin/bash siluser \
    && mkdir -p /app/raw /app/silver \
    && chown -R siluser:siluser /app
USER siluser

ENTRYPOINT ["python", "clean_to_silver.py"]
# Default: check for new bronze data every 20s.
CMD ["--input-root", "raw", "--output-root", "silver", "--interval", "20"]