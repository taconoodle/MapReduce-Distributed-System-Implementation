#FROM python:3.14-slim
FROM vlassisioa/distributed-mapreduce:test
WORKDIR /app

COPY src/manager.py src/main.py
COPY src/storage.py src/storage.py
COPY src/database.py src/database.py

RUN touch __init__.py

#COPY manager.requirements.txt requirements.txt
#RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

RUN mkdir /app/logs/

RUN useradd app
#USER app

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]