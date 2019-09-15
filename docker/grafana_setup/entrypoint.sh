#!/bin/sh

# wait for grafana to start up
sleep 60

./tmp/add_datasource.sh
./tmp/add_dashboard.sh
