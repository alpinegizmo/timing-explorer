FROM alpine:3.9

ADD dashboard.json /tmp
ADD influxdb_datasource.json /tmp
ADD entrypoint.sh /tmp
ADD add_dashboard.sh /tmp
ADD add_datasource.sh /tmp

RUN apk add --no-cache curl bash

RUN chmod +x /tmp/*.sh

ENTRYPOINT '/tmp/entrypoint.sh'
