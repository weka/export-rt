FROM alpine:latest

RUN apk add --no-cache bash curl python3 py3-pip

RUN pip3 install prometheus_client pyyaml python-dateutil wekalib==1.0.4

ARG BASEDIR="/weka"
ARG ID="472"
ARG USER="weka"

RUN mkdir -p $BASEDIR

WORKDIR $BASEDIR

COPY export $BASEDIR
COPY export-rt.yml $BASEDIR
COPY collector.py $BASEDIR
COPY lokilogs.py $BASEDIR

RUN addgroup -S -g $ID $USER &&\
    adduser -S -h $BASEDIR -u $ID -G $USER $USER && \
    chown -R $USER:$USER $BASEDIR

RUN chmod +x $BASEDIR/export

EXPOSE 8001

USER $USER
ENTRYPOINT ["./export"]
