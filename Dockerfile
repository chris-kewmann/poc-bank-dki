FROM python:3.8.16-slim-buster
WORKDIR /app
COPY src /app/src
COPY requirements.txt /app/

RUN pip install --upgrade pip
RUN pip install --default-timeout=120 --no-cache --no-cache-dir -r ./requirements.txt
RUN mkdir /data

CMD ["streamlit", "run", "./src/main.py"]

EXPOSE 8501