FROM ubuntu:20.04
RUN apt-get update && \
      apt-get -y install sudo
RUN apt-get install -y software-properties-common
RUN apt-get update

RUN apt-get install wget unzip
RUN wget https://jataware-world-modelers.s3.amazonaws.com/gadm/gadm36_2.feather.zip 
RUN wget https://jataware-world-modelers.s3.amazonaws.com/gadm/gadm36_3.feather.zip
RUN mkdir ~/mixmasta_data && \
      unzip gadm36_2.feather.zip -d ~/mixmasta_data/ && \
      unzip gadm36_3.feather.zip -d ~/mixmasta_data/ && \
      rm gadm36_?.feather.zip

RUN add-apt-repository ppa:ubuntugis/ubuntugis-unstable
RUN apt-get update
RUN apt-get install -y python3.8-dev
RUN apt-get install -y gdal-bin
RUN apt-get install -y libgdal-dev
RUN apt-get install -y python3-pip
RUN apt install -y python3-rtree

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN pip3 install numpy==1.22

WORKDIR /
COPY requirements.txt /requirements.txt
RUN pip3 install -r requirements.txt
COPY . /
RUN python3 setup.py install
#RUN mixmasta download

# ENTRYPOINT ["mixmasta"]
