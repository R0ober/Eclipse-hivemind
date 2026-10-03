FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir "hivemind==1.1.12"
COPY src/eclipse_hivemind/nodes/shared /app/shared
COPY src/eclipse_hivemind/nodes/observer/runner.py /app/runner.py
CMD ["python", "/app/runner.py"]
