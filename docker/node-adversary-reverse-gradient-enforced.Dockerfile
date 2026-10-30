FROM eclipse-adversary-reverse-gradient:dev

# Reuse the exact installed dependencies of the ordinary-matchmaking image.
COPY src/eclipse_hivemind/nodes/adversary-reverse-gradient-enforced/runner.py /app/runner.py
