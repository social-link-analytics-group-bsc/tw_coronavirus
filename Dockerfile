# pull official base image
FROM python:3.7-alpine

# set work directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install SO dependencies
RUN apk --no-cache add --virtual build-dependencies \
    build-base \
    gcc \
    libc-dev \
    libffi-dev \
    icu \
    icu-dev \
    lapack \
    lapack-dev \
    openblas-dev \
    gfortran

# install app dependencies
RUN pip install --upgrade pip
COPY requirements.container.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt

# copy project
COPY ./ /usr/src/app/