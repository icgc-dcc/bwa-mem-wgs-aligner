FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y python3-pip python3-dev
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git
RUN cd /usr/local/bin
RUN ln -s /usr/bin/python3 python
RUN pip3 install --upgrade pip

VOLUME ["/app"]
COPY . /app
WORKDIR /app

RUN pip3 install -e git+https://github.com/baminou/overture-song-payload.git@0.0.4#egg=overture_song_payload
RUN pip3 install pyyaml>=4.2b1

ENV PATH="/app:${PATH}"