FROM ubuntu:20.04
RUN apt-get update && apt-get install -y \
      software-properties-common \
      sudo \
      unzip \
      wget

RUN wget https://jataware-world-modelers.s3.amazonaws.com/gadm/gadm36_2.feather.zip && \
      wget https://jataware-world-modelers.s3.amazonaws.com/gadm/gadm36_3.feather.zip && \
      mkdir ~/mixmasta_data && \
      unzip gadm36_2.feather.zip -d ~/mixmasta_data/ && \
      unzip gadm36_3.feather.zip -d ~/mixmasta_data/ && \
      rm gadm36_?.feather.zip

RUN add-apt-repository ppa:ubuntugis/ubuntugis-unstable && apt-get update && \
      apt-get install -y \
      gdal-bin \
      libgdal-dev \
      python3-pip \
      python3-rtree \
      python3.8-dev && \
      apt-get -y autoremove && apt-get clean autoclean

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

COPY . /mixmasta

WORKDIR /mixmasta

RUN pip3 install numpy==1.22 && \
      pip3 install -r requirements.txt && \
      python3 setup.py install
