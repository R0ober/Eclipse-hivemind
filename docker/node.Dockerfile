FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    PYTHONWARNINGS="ignore::FutureWarning:torch.jit._script"

WORKDIR /app

RUN pip install --no-cache-dir "torch>=2.0" "hivemind>=1.1.0"

COPY src/eclipse_hivemind/node/runner.py /app/runner.py
COPY src/eclipse_hivemind/node/client.py /app/client.py

CMD ["python", "/app/runner.py"]
