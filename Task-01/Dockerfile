FROM python:latest
WORKDIR /app
COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt
COPY main.py test_main.py /app/
