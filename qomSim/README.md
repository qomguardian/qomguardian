# Requirements

* `pip install ns.py`
* `pip install simpy`
* The simulator is mainly based ns.py, which is a discrete network event simulator.

# Usage

* Tasks:
Flow size estimation, Heavy hitter detection, Heavy change detection, Cardinality estimation, Entropy estimation, Maximum inter-arrival time estimation
* How to Run: `python FatTree_DHT_Tasks.py --k 8 --limit_memory --n_flows 2500000 --memory 10 20 30  --memory_limit_ratio 0.1 --log_file "test.log"`
* Refer to the `FatTree_DHT_Tasks.py` for more information about the arguments.