# Fraud model metrics (FraudBench, time-ordered held-out test split)

Split: claims sorted by timestamp; first 353 train, last 236 test (126 fraud, 110 legit). Test AUC 0.7766.
Fraud = DeepFake damage claims, legit = genuine Negative damage claims (see train.py docstring; FraudBench is a claim-fraud proxy, not card data).

Recall at a fixed false-decline rate (threshold chosen on test legit scores, so the false-decline rate is at most the stated rate; with few legit rows, 0.5% means zero false declines).

| false-decline rate | recall | threshold | achieved false-decline rate |
|---|---|---|---|
| 0.5% | 0.1270 | 0.9722 | 0.0000 |
| 1% | 0.1349 | 0.9685 | 0.0091 |
| 2% | 0.1746 | 0.9619 | 0.0182 |
