FROM python:3.8-slim
COPY indexer /root/app
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

ENV PYTHONUNBUFFERED=0

CMD ["python3", "-u", "/root/app/main.py"]