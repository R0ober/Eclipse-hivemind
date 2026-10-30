FROM python:3.12-slim AS dependencies
ENV PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    PYTHONWARNINGS="ignore::FutureWarning:torch.jit._script"
WORKDIR /app
COPY docker/node-constraints.txt /tmp/node-constraints.txt
RUN pip install --no-cache-dir -c /tmp/node-constraints.txt \
    "torch==2.14.1" "hivemind==1.1.12"

FROM dependencies AS sc1
COPY src/eclipse_hivemind/nodes/shared /app/shared
COPY src/eclipse_hivemind/nodes/honest/runner.py /app/honest_runner.py
COPY src/eclipse_hivemind/nodes/coordinated_attacker /app/coordinated_attacker
COPY src/eclipse_hivemind/nodes/factorial /app/factorial
COPY src/eclipse_hivemind/nodes/factorial/runner.py /app/runner.py
CMD ["python", "/app/runner.py"]

FROM sc1 AS sc2
COPY src/eclipse_hivemind/nodes/factorial2/runtime.py /app/factorial/runtime.py
COPY src/eclipse_hivemind/nodes/factorial2_compact/trace_full.py /app/factorial/trace_full.py
COPY src/eclipse_hivemind/nodes/factorial2_compact/compact.py /app/factorial/compact.py
COPY src/eclipse_hivemind/nodes/factorial2_compact/trace.py /app/factorial/trace.py
