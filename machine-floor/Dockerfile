FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests paho-mqtt
COPY machine_telemetry_simulator.py .
RUN useradd --create-home --shell /bin/bash simuser && chown -R simuser:simuser /app
USER simuser
ENTRYPOINT ["python", "machine_telemetry_simulator.py"]
CMD ["--interval", "5", "--output", "mqtt"]
