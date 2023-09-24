FROM python:3.8-slim
COPY . /root/app
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

CMD ["python3", "/root/app/runner.py"]