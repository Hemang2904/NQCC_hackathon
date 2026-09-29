# UK National Quantum Computing Hackathon 2024 (NQCC)

Team entry with CGI UK, July 2024. The challenge: forecasting supply, demand and price for the UK energy system with quantum and quantum-classical hybrid methods, at the consumer-level granularity where classical forecasting starts to struggle.

This repository holds the data preparation for that work.

## Contents

- `preprocessing.py`: reads the raw half-hourly settlement datasets (cost, generation by fuel type, demand), keeps the relevant columns, removes duplicates, restricts generation to January 2016 to April 2024, and sums generation per settlement period.
- `Original_Cost.csv`, `Original_Demand.csv`: the raw inputs.
- `preprocessed_cost.csv`, `preprocessed_demand.csv`, `preprocessed_generation.csv`: the cleaned outputs.
- `merged_generation_demand.csv`: generation and demand joined on settlement date and period, the table the forecasting experiments were built on.

## Run

```bash
pip install pandas
python preprocessing.py
```

Related: input encodings and loss definitions for quantum circuit simulation, and the resource analysis for the hybrid approach, were explored during the hackathon with Qiskit and Classiq.
