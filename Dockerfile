FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

ADD mydata/ mydata/
ADD requirements.txt .
ADD schema_standard.ttl .

RUN pip3 install -r /app/requirements.txt

CMD flask --app mydata.viewer:app run -h viewer -p 4999