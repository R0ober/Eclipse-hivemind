FROM python:3.12-slim AS logger
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir "fastapi==0.142.2" "pydantic==2.13.5" "uvicorn>=0.30,<1" "PyYAML==6.0.3"
COPY src/eclipse_hivemind/__init__.py /app/eclipse_hivemind/__init__.py
COPY src/eclipse_hivemind/logger /app/eclipse_hivemind/logger
COPY src/eclipse_hivemind/attack_coordinator /app/eclipse_hivemind/attack_coordinator
CMD ["python", "-m", "eclipse_hivemind.logger"]

FROM logger AS coordinator
CMD ["python", "-m", "eclipse_hivemind.attack_coordinator"]
