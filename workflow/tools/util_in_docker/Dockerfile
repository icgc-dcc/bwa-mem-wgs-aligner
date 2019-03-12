FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y python3-pip python3-dev
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git
RUN cd /usr/local/bin
RUN ln -s /usr/bin/python3 python
RUN pip3 install --upgrade pip
RUN apt-get install -y software-properties-common wget

RUN wget https://cgc.sbgenomics.com/downloads/cli/cgc-uploader.tgz -O cgc-uploader.tgz
RUN wget https://github.com/baminou/overture-song-payload/archive/0.0.5.tar.gz -O overture-song-payload.tar.gz
RUN tar -xvzf cgc-uploader.tgz
RUN tar -xvzf overture-song-payload.tar.gz

# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

COPY . /app

RUN pip3 install /overture-song-payload-0.0.5/
RUN pip3 install 'pyyaml>=4.2b1'

ENV PATH="/app:/app/cgc-uploader/bin:${PATH}"

RUN mv cgc-uploader /app

WORKDIR /app

