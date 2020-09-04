FROM jupyter/pyspark-notebook

# RUN mkdir /usr/src

# WORKDIR /usr/src

COPY ./. /usr/src

RUN pip install -r /usr/src/requirements.txt

RUN pwd

EXPOSE 8888
