FROM python:3.9-slim

EXPOSE 8501

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY *.py *.csv *.txt .
COPY mydata mydata
COPY pages pages

RUN pip3 install -r requirements.txt

ENTRYPOINT ["streamlit", "run", "My_Data.py", "--server.port=8501", "--server.address=0.0.0.0"]