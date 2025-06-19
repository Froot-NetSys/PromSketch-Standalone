How to run?
1. Start Your Fake Exporters
First, navigate to your ExporterStarter/ directory and run the ExportManager.py script. This will launch all your fake_norm_exporter.py instances on their respective ports, ready to be scraped.
python3 ExportManager.py   --config=num_samples_config.yml   --targets=10   --timeseries=10  --max_windowsize=100  --querytype=quantile  --waiteval=30

3. Launch the PromSketch Go Server
Next, go into your PromsketchServer/ directory and start your Go server. This is the core component that will receive data from the ingester and serve queries. You can optionally set NUM_TIMESERIES_INIT if you need to initialize with a specific number of time series.
go run . 

3. Run the Custom Data Ingester 
Now, head back to ExporterStarter/custom_data_ingester.py and launch your custom Python data ingester. This script will begin sending data from your running fake exporters directly to your PromSketch Go server. 
