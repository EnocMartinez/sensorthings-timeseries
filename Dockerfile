FROM ubuntu:22.04
WORKDIR /app
COPY ./app.py ./common.py ./requirements.txt /app
RUN apt update && \
    apt install python3 -y && \
    apt install python3-pip -y && \
    pip3 install -r requirements.txt
CMD python3 ./app.py

