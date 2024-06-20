python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=1 --windowsize=10 --querytype=sum --waiteval=1
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=10 --windowsize=10 --querytype=sum --waiteval=5
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=100 --windowsize=10 --querytype=sum --waiteval=5
python ExportManager.py --config=num_samples_config.yml --targets=2 --timeseries=1000 --windowsize=10 --querytype=sum --waiteval=10
python ExportManager.py --config=num_samples_config.yml --targets=20 --timeseries=10000 --windowsize=10 --querytype=sum --waiteval=10
python ExportManager.py --config=num_samples_config.yml --targets=200 --timeseries=100000 --windowsize=10 --querytype=sum --waiteval=10
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=1 --windowsize=100 --querytype=sum --waiteval=10
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=10 --windowsize=100 --querytype=sum --waiteval=10
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=100 --windowsize=100 --querytype=sum --waiteval=20
python ExportManager.py --config=num_samples_config.yml --targets=2 --timeseries=1000 --windowsize=100 --querytype=sum --waiteval=20
python ExportManager.py --config=num_samples_config.yml --targets=20 --timeseries=10000 --windowsize=100 --querytype=sum --waiteval=30
python ExportManager.py --config=num_samples_config.yml --targets=200 --timeseries=100000 --windowsize=100 --querytype=sum --waiteval=40

/bin/bash ./kill.sh
