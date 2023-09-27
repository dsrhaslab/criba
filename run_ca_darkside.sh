#!/bin/bash

ES_URL="cloud124:31111"

echo -e "\n\n-------GET TRACES--------\n"
cd traces/darkside
tar -xvf runs.tar.gz

echo -e "\n\n-------DataParser--------\n"
cd ~/ransomware-analysis/scripts/correlations
python3.10 correlation_algorithms/dio_importer.py -u "http://$ES_URL" --size 10000 -d --session darkside traces/darkside/runs/run1/dio-trace.json
python3.10 correlation_algorithms/metricbeat_importer.py -u "http://$ES_URL" --size 10000 --session darkside traces/darkside/runs/run1/metricbeat

echo -e "\n\n----------FPCA------------\n"
bash correlate_fp.sh correlate_index "$ES_URL" 0 1 darkside

echo -e "\n\n----------UNExt-----------\n"
python3.10 correlation_algorithms/unext.py -u "http://$ES_URL" -sz 10000 -d -t 300 darkside

echo -e "\n\n----------DsetU-----------\n"
python3.10 correlation_algorithms/dataset_usage.py -u "http://$ES_URL" --file correlation_algorithms/dataset10G.csv  -d -sz 10000 darkside

echo -e "\n\n-------Transversals-------\n"
python3.10 correlation_algorithms/transversals.py -u "http://$ES_URL"  -sz 10000 --nProc 4 -d darkside

echo -e "\n\n---------FnGram-----------\n"
python3.10 correlation_algorithms/fngram.py -u "http://$ES_URL" -d -sz 10000 darkside

echo -e "\n\n--------FSysSeq-----------\n"
python3.10 correlation_algorithms/fsysseq.py -u "http://$ES_URL" -d -sz 10000 -p 4 darkside


