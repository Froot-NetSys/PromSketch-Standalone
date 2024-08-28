echo "--windowsize=10 --querytype=avg"
python ExportManager.py --config=num_samples_config.yml --targets=200 --timeseries=10000 --max_windowsize=100000 --querytype=avg --waiteval=300
