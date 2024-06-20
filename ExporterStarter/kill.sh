#!/bin/bash

kill -9  $(ps aux | grep './vmalert' | awk '{print $2}')
kill -9 $(ps aux | grep './victoria-metrics' | awk '{print $2}')
kill -9 $(ps aux | grep  -i '[p]ython fake_norm_exporter.py' | awk '{print $2}')
kill -9 $(ps aux | grep -i '[p]ython ../EvaluationTools/EvalData.py' | awk '{print $2}')

rm -r victoria-metrics-data/
