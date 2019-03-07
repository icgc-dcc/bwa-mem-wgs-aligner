FROM java

RUN apt-get update
RUN apt-get install -y python3-pip python3-dev
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git
RUN cd /usr/local/bin
RUN ln -s /usr/bin/python3 python
RUN pip3 install --upgrade pip
RUN apt-get install -y software-properties-common wget


VOLUME ["/app"]
COPY . /app
WORKDIR /app

ADD cgc-uploader.tgz /app/

RUN pip3 install -r requirements.txt

ENV PATH="/app:${PATH}"
ENV PATH="/app/cgc-uploader/bin:${PATH}"