FROM python:3.14-slim

WORKDIR /app

COPY UI/main.py main.py
COPY UI/GUI GUI/.

COPY Auth .

COPY UI/GUI/gui.requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD ["sh", "-c", "python import_realm.py && uvicorn main:app --host 0.0.0.0 --port 8080"]