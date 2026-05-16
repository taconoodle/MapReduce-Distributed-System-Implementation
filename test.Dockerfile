FROM python:3.14-slim

COPY manager.requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt