pkill -9 prometheus
kill $(ps aux | grep '[p]ython fake_norm_exporter.py' | awk '{print $2}')
