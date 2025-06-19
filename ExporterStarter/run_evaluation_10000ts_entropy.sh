echo "--windowsize=10 --querytype=entropy"
python3 ExportManager.py --config=num_samples_config.yml --targets=200 --timeseries=10000 --max_windowsize=1000000 --querytype=entropy --waiteval=300

./kill.sh
