FROM python:3.13.0a4-alpine3.19

#python:3.8

WORKDIR /app

COPY requirements.txt /app/requirements.txt

#RUN apt-get update && apt-get upgrade

RUN apk update

RUN pip install --upgrade setuptools
RUN apk add --no-cache build-base
#RUN apt-get install -y gdal-bin libgdal-dev g++

RUN apk add --no-cache \
    --repository http://dl-cdn.alpinelinux.org/alpine/edge/testing \
    --repository http://dl-cdn.alpinelinux.org/alpine/edge/main \
    libmaxminddb postgresql-dev gcc musl-dev gdal-dev linux-headers g++

#apt-get install -y build-essential libssl-dev libffi-dev python3-dev

RUN pip install --no-cache-dir  --upgrade -r /app/requirements.txt

COPY . /app

CMD python3 /app/telegram_bot_safe.py