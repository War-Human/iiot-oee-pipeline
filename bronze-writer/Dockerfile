FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt
COPY bronze_writer.py .
RUN useradd --create-home --shell /bin/bash bwuser && mkdir -p /data/raw && chown -R bwuser:bwuser /app /data
USER bwuser
ENTRYPOINT ["python", "bronze_writer.py"]
CMD ["--mqtt-host", "mosquitto", "--root", "/data/raw"]
