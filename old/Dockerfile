FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
WORKDIR /app
RUN pip install --no-cache-dir torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir hivemind==1.1.12
COPY bootstrap.py /app/bootstrap.py
COPY train.py /app/train.py
CMD ["python", "train.py"]
