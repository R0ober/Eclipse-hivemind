FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
	"fastapi>=0.115.0" \
	"uvicorn>=0.30.0" \
	"PyYAML>=6.0"

COPY src/eclipse_hivemind /app/eclipse_hivemind

CMD ["python", "-m", "eclipse_hivemind.aggregator"]
