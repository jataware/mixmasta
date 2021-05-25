FROM ubuntu:20.04
RUN apt-get update && \
      apt-get -y install sudo
RUN apt-get install -y software-properties-common
RUN apt-get update
RUN add-apt-repository ppa:ubuntugis/ubuntugis-unstable
RUN apt-get update
RUN apt-get install -y python3.8-dev
RUN apt-get install -y gdal-bin
RUN apt-get install -y libgdal-dev
RUN apt-get install -y python3-pip
RUN apt install -y python3-rtree
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN pip3 install numpy==1.20.1

WORKDIR /
COPY . /
RUN pip3 install -r requirements.txt
RUN python3 setup.py install
RUN mixmasta download

ENTRYPOINT ["mixmasta"]