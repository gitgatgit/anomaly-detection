## Anomaly detection

Dataset missing features, large 'unassigned' cluster.

## Access and credentials

**GCP impersonation credentials** are used to access BigQuery. Authentication is done via [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation): the code runs on **AWS** (e.g. EC2), obtains AWS credentials from the instance metadata (IMDSv2), and exchanges them for short-lived GCP credentials by impersonating a GCP service account. **This restricts BigQuery access to a single AWS account** — only workloads running in that account can obtain the GCP credentials needed to query BigQuery, improving security and auditability.

You need:

- A GCP project with Workload Identity Federation configured for your AWS account (and optionally a service account impersonation URL).
- A `credentials.json` file at a path of your choice (default in code: `/path/credentials.json`) containing at least: `audience`, `subject_token_type`, `token_url`, and `service_account_impersonation_url`.

Update the path in `project.py` and set your BigQuery project and dataset (`project_number_here`, `my_dataset_uc1`) before running.

## Pipeline overview

1. **Sample & feature tables** — Stratified sample from `bigquery-public-data.crypto_solana_mainnet_us` (Transactions, Token Transfers, Instructions), then build a feature table (`solana_drain_features`) with wallet/tx metrics (e.g. `drain_sol_ratio`, token fanout, CPI ratio).
2. **Sequence prep** — Per-wallet transaction sequences (fixed length), with DeFi context and time deltas.
3. **Anomaly scoring** — Isolation Forest on sequence-derived stats + Transformer autoencoder reconstruction error; scores are fused and calibrated with conformal prediction.
4. **Output** — Wallet-level risk scores and flags, projected back to transaction rows for alerting or export.

## Requirements

- Python with `google-auth`, `google-cloud-bigquery`, `requests`, `pandas`, `numpy`, `scikit-learn`, `torch`.
- Execution on AWS (or an environment that can supply AWS credentials) when using the included IMDSv2-based credential supplier.
