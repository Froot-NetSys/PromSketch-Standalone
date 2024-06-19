#!/bin/bash

python3 fake_norm_exporter.py --port=10000 --instancestart=0 --batchsize=100 --valuescale=10 & 
./victoria-metrics --promscrape.config="vmconfig.yml" &
./vmalert -rule="rules.yml" -datasource.url="http://localhost:8428" -remoteWrite.url="http://localhost:8428" -evaluationInterval=20s &

echo "created apps"