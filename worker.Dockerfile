FROM python:3.14-slim
WORKDIR /app

COPY src/worker.py src/main.py
COPY src/storage.py src/storage.py

RUN touch src/__init__.py

COPY worker.requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

RUN useradd worker
USER worker

CMD ["python", "/app/src/main.py"]