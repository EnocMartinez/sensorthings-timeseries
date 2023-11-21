FROM ubuntu:22.04
WORKDIR /app
COPY ./app.py ./common.py ./requirements.txt /app
RUN apt update
RUN apt install python3 -y
RUN apt install python3-pip -y
RUN pip3 install -r requirements.txt
CMD python3 ./app.py

