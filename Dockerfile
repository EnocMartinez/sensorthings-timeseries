FROM python:3-alpine3.18
WORKDIR /app
COPY ./app.py ./common.py ./requirements.txt /app
RUN apk --upgrade add curl build-base
RUN pip3 install -r requirements.txt
EXPOSE 3000
CMD python3 ./app.py


# Base image
#FROM tiangolo/uwsgi-nginx-flask:python3.8-alpine
## Update and install
#RUN apk --update add bash nano vim
#ENV STATIC_URL /static
#ENV STATIC_PATH /var/www/app/static
#COPY ./requirements.txt /var/www/requirements.txt
#RUN pip install -r /var/www/requirements.txt
