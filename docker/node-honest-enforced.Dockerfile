FROM eclipse-hivemind-node-honest:dev

# Reuse the exact installed dependencies of the ordinary-matchmaking image.
COPY src/eclipse_hivemind/nodes/honest-enforced/runner.py /app/runner.py
