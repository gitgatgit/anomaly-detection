# Converted from project.ipynb - Solana drain anomaly detection pipeline

import os
import json
import requests
from google.auth import aws
from google.cloud import bigquery

IMDS = "http://169.254.169.254/latest"

def _imds_token():
    return requests.put(
        f"{IMDS}/api/token",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
        timeout=2,
    ).text

class IMDSv2Supplier(aws.AwsSecurityCredentialsSupplier):
    def get_aws_region(self, context, request):
        az = requests.get(
            f"{IMDS}/meta-data/placement/availability-zone",
            headers={"X-aws-ec2-metadata-token": _imds_token()},
            timeout=2,
        ).text
        return az[:-1]

    def get_aws_security_credentials(self, context, request):
        tok = _imds_token()
        role = requests.get(
            f"{IMDS}/meta-data/iam/security-credentials/",
            headers={"X-aws-ec2-metadata-token": tok},
            timeout=2,
        ).text.strip()
        creds = json.loads(requests.get(
            f"{IMDS}/meta-data/iam/security-credentials/{role}",
            headers={"X-aws-ec2-metadata-token": tok},
            timeout=2,
        ).text)
        return aws.AwsSecurityCredentials(
            access_key_id=creds["AccessKeyId"],
            secret_access_key=creds["SecretAccessKey"],
            session_token=creds.get("Token"),
        )

with open("/home/ubuntu/things/folder/credentials.json") as f:
    info = json.load(f)

credentials = aws.Credentials(
    audience=info["audience"],
    subject_token_type=info["subject_token_type"],
    token_url=info["token_url"],
    service_account_impersonation_url=info["service_account_impersonation_url"],
    aws_security_credentials_supplier=IMDSv2Supplier(),
)

client = bigquery.Client(project="372534660131", credentials=credentials)

query = "SELECT 'Handshake Successful!' as status"
for row in client.query(query):
    print(row.status)

DATASET_ID = 'my_dataset_uc1'

names = ['sep', 'oct', 'nov']
chunks = [
    ('2024-09-01', '2024-09-30'),
    ('2024-10-01', '2024-10-31'),
    ('2024-11-01', '2024-11-30'),
]

EXPECTED_FEATURE_COLUMNS = [
    'signature','block_timestamp','tx_date','wallet','fee_sol','compute_units_consumed',
    'success_flag','num_accounts','num_signers','num_writable','log_count','num_balance_changes',
    'net_sol_flow','wallet_sol_delta','max_balance_change','num_pre_token_balances',
    'num_post_token_balances','token_accounts_closed','hour_of_day','day_of_week',
    'num_token_transfers','unique_mints','unique_destinations','unique_sources','self_transfers',
    'total_token_value','max_token_value','num_instructions','unique_programs',
    'unique_instruction_types','inner_instructions','token_program_calls','has_compute_budget',
    'program_list','wallet_tx_count','active_days','wallet_age_minutes','wallet_out_degree',
    'wallet_transfer_count','fanout_ratio','cpi_ratio','wallet_activity_rate',
    'avg_token_transfer','drain_sol_ratio'
]

def build_query_one(start_date, end_date, daily_cap=10_000, presample_pct=2):
    return f"""
CREATE OR REPLACE TABLE {DATASET_ID}.sampled_transactions
PARTITION BY DATE(block_timestamp)
CLUSTER BY signature
AS

WITH pre_sample AS (
  SELECT
    signature,
    block_timestamp,
    fee,
    compute_units_consumed,
    status,
    accounts,
    log_messages,
    balance_changes,
    pre_token_balances,
    post_token_balances
  FROM `bigquery-public-data.crypto_solana_mainnet_us.Transactions`
  TABLESAMPLE SYSTEM ({presample_pct} PERCENT)
  WHERE DATE(block_timestamp) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    AND compute_units_consumed IS NOT NULL
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY DATE(block_timestamp)
      ORDER BY RAND()
    ) AS _rn
  FROM pre_sample
)
SELECT * EXCEPT(_rn)
FROM ranked
WHERE _rn <= {daily_cap}
"""

def build_query_two(start_date, end_date):
    return f"""
CREATE OR REPLACE TABLE {DATASET_ID}.solana_drain_features
PARTITION BY tx_date
CLUSTER BY wallet
AS

WITH tx_base AS (
SELECT
  signature,
  block_timestamp,
  DATE(block_timestamp) AS tx_date,

  fee / 1e9 AS fee_sol,
  compute_units_consumed,
  IF(status='Success',1,0) AS success_flag,

  ARRAY_LENGTH(IFNULL(accounts,[])) AS num_accounts,
  ARRAY_LENGTH(IFNULL(log_messages,[])) AS log_count,
  ARRAY_LENGTH(IFNULL(balance_changes,[])) AS num_balance_changes,

  (SELECT COUNTIF(a.signer) FROM UNNEST(accounts) a) AS num_signers,
  (SELECT COUNTIF(a.writable) FROM UNNEST(accounts) a) AS num_writable,

  COALESCE(
    (SELECT SUM(bc.after - bc.before)
     FROM UNNEST(balance_changes) bc),0)/1e9 AS net_sol_flow,

  COALESCE(
    (SELECT bc.after - bc.before
     FROM UNNEST(balance_changes) bc
     WHERE bc.account = (
       SELECT a.pubkey FROM UNNEST(accounts) a WHERE a.signer LIMIT 1
     )
     LIMIT 1), 0) / 1e9 AS wallet_sol_delta,

  COALESCE(
    (SELECT MAX(ABS(bc.after - bc.before))
     FROM UNNEST(balance_changes) bc),0)/1e9 AS max_balance_change,

  ARRAY(
    SELECT a.pubkey
    FROM UNNEST(accounts) a
    WHERE a.signer
  )[SAFE_OFFSET(0)] AS wallet,

  ARRAY_LENGTH(IFNULL(pre_token_balances,[])) AS num_pre_token_balances,
  ARRAY_LENGTH(IFNULL(post_token_balances,[])) AS num_post_token_balances,

  (ARRAY_LENGTH(IFNULL(pre_token_balances,[]))
   - ARRAY_LENGTH(IFNULL(post_token_balances,[]))) AS token_accounts_closed,

  EXTRACT(HOUR FROM block_timestamp) AS hour_of_day,
  EXTRACT(DAYOFWEEK FROM block_timestamp) AS day_of_week

FROM {DATASET_ID}.sampled_transactions
WHERE DATE(block_timestamp) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
),

token_features AS (
SELECT
  tt.tx_signature,
  COUNT(*) AS num_token_transfers,
  COUNT(DISTINCT mint) AS unique_mints,
  COUNT(DISTINCT destination) AS unique_destinations,
  COUNT(DISTINCT source) AS unique_sources,
  COUNTIF(source = destination) AS self_transfers,
  SUM(value) AS total_token_value,
  MAX(value) AS max_token_value
FROM `bigquery-public-data.crypto_solana_mainnet_us.Token Transfers` tt
JOIN tx_base s
  ON tt.tx_signature = s.signature
WHERE DATE(tt.block_timestamp) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
GROUP BY tt.tx_signature
),

instruction_features AS (
SELECT
  i.tx_signature,
  COUNT(*) AS num_instructions,
  COUNT(DISTINCT program_id) AS unique_programs,
  COUNT(DISTINCT instruction_type) AS unique_instruction_types,
  COUNTIF(parent_index IS NOT NULL) AS inner_instructions,
  COUNTIF(program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA') AS token_program_calls,
  COUNTIF(program_id = 'ComputeBudget111111111111111111111111111111') AS has_compute_budget,
  ARRAY_AGG(DISTINCT program_id) AS program_list
FROM `bigquery-public-data.crypto_solana_mainnet_us.Instructions` i
JOIN tx_base s
  ON i.tx_signature = s.signature
WHERE DATE(i.block_timestamp) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
GROUP BY i.tx_signature
),

wallet_velocity AS (
SELECT
  wallet,
  COUNT(*) AS wallet_tx_count,
  COUNT(DISTINCT DATE(block_timestamp)) AS active_days,
  MIN(block_timestamp) AS first_seen,
  MAX(block_timestamp) AS last_seen
FROM tx_base
GROUP BY wallet
),

wallet_graph AS (
SELECT
  tt.source AS wallet,
  COUNT(DISTINCT tt.destination) AS wallet_out_degree,
  COUNT(*) AS wallet_transfer_count
FROM `bigquery-public-data.crypto_solana_mainnet_us.Token Transfers` tt
JOIN tx_base s
  ON tt.source = s.wallet
WHERE DATE(tt.block_timestamp) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
GROUP BY wallet
)

SELECT
  s.signature,
  s.block_timestamp,
  s.tx_date,
  s.wallet,
  s.fee_sol,
  s.compute_units_consumed,
  s.success_flag,
  s.num_accounts,
  s.num_signers,
  s.num_writable,
  s.log_count,
  s.num_balance_changes,
  s.net_sol_flow,
  s.wallet_sol_delta,
  s.max_balance_change,
  s.num_pre_token_balances,
  s.num_post_token_balances,
  s.token_accounts_closed,
  s.hour_of_day,
  s.day_of_week,
  COALESCE(t.num_token_transfers,0) AS num_token_transfers,
  COALESCE(t.unique_mints,0) AS unique_mints,
  COALESCE(t.unique_destinations,0) AS unique_destinations,
  COALESCE(t.unique_sources,0) AS unique_sources,
  COALESCE(t.self_transfers,0) AS self_transfers,
  COALESCE(t.total_token_value,0) AS total_token_value,
  COALESCE(t.max_token_value,0) AS max_token_value,
  COALESCE(i.num_instructions,0) AS num_instructions,
  COALESCE(i.unique_programs,0) AS unique_programs,
  COALESCE(i.unique_instruction_types,0) AS unique_instruction_types,
  COALESCE(i.inner_instructions,0) AS inner_instructions,
  COALESCE(i.token_program_calls,0) AS token_program_calls,
  COALESCE(i.has_compute_budget,0) AS has_compute_budget,
  i.program_list,
  COALESCE(v.wallet_tx_count,0) AS wallet_tx_count,
  COALESCE(v.active_days,0) AS active_days,
  TIMESTAMP_DIFF(s.block_timestamp, v.first_seen, MINUTE) AS wallet_age_minutes,
  COALESCE(g.wallet_out_degree,0) AS wallet_out_degree,
  COALESCE(g.wallet_transfer_count,0) AS wallet_transfer_count,
  SAFE_DIVIDE(t.unique_destinations, NULLIF(t.num_token_transfers,0)) AS fanout_ratio,
  SAFE_DIVIDE(i.inner_instructions, NULLIF(i.num_instructions,0)) AS cpi_ratio,
  SAFE_DIVIDE(v.wallet_tx_count, NULLIF(v.active_days,0)) AS wallet_activity_rate,
  SAFE_DIVIDE(t.total_token_value, NULLIF(t.num_token_transfers,0)) AS avg_token_transfer,
  SAFE_DIVIDE(ABS(s.wallet_sol_delta), NULLIF(s.max_balance_change,0)) AS drain_sol_ratio
FROM tx_base s
LEFT JOIN token_features t
  ON s.signature = t.tx_signature
LEFT JOIN instruction_features i
  ON s.signature = i.tx_signature
LEFT JOIN wallet_velocity v
  ON s.wallet = v.wallet
LEFT JOIN wallet_graph g
  ON s.wallet = g.wallet
WHERE s.wallet IS NOT NULL
"""

import re

def dry_run(client, sql, label, start_date='', end_date=''):
    """
    Attempt BQ dry_run. Returns (gb, cost) if successful.
    WIF credentials are blocked from dry_run / metadata on bigquery-public-data,
    so this will usually raise — caller handles the fallback.
    """
    match      = re.search(r'\bAS\s*\n\s*\n(WITH\b.*)', sql, re.DOTALL | re.IGNORECASE)
    select_sql = match.group(1) if match else sql
    cfg  = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job  = client.query(select_sql, job_config=cfg, location='US')
    return job.total_bytes_processed / 1e9, (job.total_bytes_processed / 1e12) * 5


print("=" * 65)
print("Cost estimate — 2 queries × 3 chunks")
print("=" * 65)

total_gb, total_cost = 0.0, 0.0
dry_run_ok = False

for name, (start, end) in zip(names, chunks):
    print(f"\n  {name.upper()}  ({start}  →  {end})")
    for label, build_fn in [
        ("query_one  sample+stratify", build_query_one),
        ("query_two  feature eng.",    build_query_two),
    ]:
        try:
            gb, cost = dry_run(client, build_fn(start, end), label, start, end)
            print(f"  [{label:<30}]  {gb:>8.1f} GB   ${cost:.4f}  (dry_run)")
            total_gb   += gb
            total_cost += cost
            dry_run_ok = True
        except Exception:
            pass  # handled below

if not dry_run_ok:
    print("""
⚠  BQ dry_run is blocked for this WIF credential on bigquery-public-data.
   dry_run, get_table, and INFORMATION_SCHEMA all return NotFound/Forbidden.
   Real queries succeed — see job history below for reference costs.

─────────────────────────────────────────────────────────────────
 Reference from job history (list_jobs cell):
   Old query_two  (no JOIN, full table scan)  ~14,344 GB  $71.72 / run
   Recent query_one (TABLESAMPLE, ~30 days)   ~35– 55 GB  $0.18–0.28 / run

 New queries — rough estimates:
   query_one  TABLESAMPLE 2%, 30 days         ~45 GB      ~$0.23 / chunk
   query_two  post-JOIN (sampled 300K rows)   ~50–300 GB  ~$0.25–1.50 / chunk
              (down from 14 TB — JOIN prunes token_transfers + instructions)

   3 chunks total:                            ~$1.50–6.00  (rough upper bound)

→ Run the queries, then re-run the list_jobs cell to see exact costs.
─────────────────────────────────────────────────────────────────
""")
else:
    print(f"\n{'─'*65}")
    print(f"  Total scan : {total_gb:.1f} GB")
    print(f"  Est. cost  : ${total_cost:.4f}  (on-demand @ $5/TB)")

import time
from google.api_core import exceptions as gcp_exc


# ---- Pre-live validation + optional live run ----

# Step 0: ensure dataset exists in us-central1
dataset_ref = bigquery.DatasetReference(client.project, DATASET_ID)
dataset = bigquery.Dataset(dataset_ref)
dataset.location = 'us-central1'
try:
    client.create_dataset(dataset, exists_ok=True)
    print('Dataset ready.')
except gcp_exc.Forbidden:
    try:
        client.get_dataset(dataset_ref)
        print('Dataset already exists - continuing.')
    except Exception as exc:
        raise RuntimeError(
            f"Dataset '{DATASET_ID}' missing and service account cannot create it. "
            "Create it manually with location us-central1."
        ) from exc

# Step 1: probe source table access
print("\nProbing access to public Solana transactions table...")
probe_sql = """
    SELECT COUNT(*) AS n
    FROM `bigquery-public-data.crypto_solana_mainnet_us.Transactions`
    WHERE block_timestamp BETWEEN '2024-09-01' AND '2024-09-01 00:01:00'
"""
probe = client.query(probe_sql, location='us-central1').result()
count = next(iter(probe))['n']
print(f"  Access confirmed - {count:,} rows in first minute of 2024-09-01\n")

# Step 1.5: feature-level sample validation
print('Running feature-level sample check before live run...')
sample_start = '2024-09-01'
sample_end = '2024-09-01'

attempts = [
    {'daily_cap': 20, 'presample_pct': 0.02},
    {'daily_cap': 100, 'presample_pct': 0.10},
    {'daily_cap': 300, 'presample_pct': 0.50},
]

sample_q = f"""
    SELECT *
    FROM {DATASET_ID}.solana_drain_features
    WHERE tx_date BETWEEN DATE('{sample_start}') AND DATE('{sample_end}')
    LIMIT 10
"""

sample_df = None
for i, cfg in enumerate(attempts, start=1):
    print(
        f"  Sample attempt {i}/{len(attempts)} "
        f"(daily_cap={cfg['daily_cap']}, presample_pct={cfg['presample_pct']})..."
    )
    client.query(
        build_query_one(
            sample_start,
            sample_end,
            daily_cap=cfg['daily_cap'],
            presample_pct=cfg['presample_pct'],
        ),
        location='us-central1',
    ).result()
    client.query(build_query_two(sample_start, sample_end), location='us-central1').result()
    sample_df = client.query(sample_q, location='us-central1').to_dataframe()
    if not sample_df.empty:
        break

if sample_df is None or sample_df.empty:
    raise RuntimeError(
        'Feature sample query returned 0 rows after multiple low-cost attempts. '
        'Try widening sample dates or increasing daily_cap/presample_pct.'
    )

print(f"Feature sample returned {len(sample_df)} rows.")
print(f"Feature column count: {len(sample_df.columns)}")
print('Columns:')
print(', '.join(sample_df.columns))

missing = [c for c in EXPECTED_FEATURE_COLUMNS if c not in sample_df.columns]
if missing:
    raise RuntimeError('Feature schema mismatch. Missing expected columns: ' + ', '.join(missing))

print('Schema check passed: all expected feature columns are present.')
print('\nPreview row:')
print(sample_df.head(1).to_string(index=False))
print('\n' + '=' * 65 + '\n')

RUN_LIVE = True  # Set to True when you want chunk export.

# Step 2: optional live chunk run
if RUN_LIVE:
    save_dir = '/home/ubuntu/data/chunks'
    os.makedirs(save_dir, exist_ok=True)

    for name, (start, end) in zip(names, chunks):
        out_path = os.path.join(save_dir, f'{name}.parquet')
        if os.path.exists(out_path):
            print(f"[{name}] Skip - already saved at {out_path}")
            continue

        print(f"\n=== {name.upper()} ({start} -> {end}) ===")
        print('  [1/3] Building stratified sample...')
        client.query(build_query_one(start, end), location='us-central1').result()
        print('  [2/3] Engineering features...')
        client.query(build_query_two(start, end), location='us-central1').result()
        print('  [3/3] Exporting to parquet...')

        q = f"""
            SELECT *
            FROM {DATASET_ID}.solana_drain_features
            WHERE tx_date BETWEEN '{start}' AND '{end}'
        """
        df = client.query(q, location='us-central1').to_dataframe(progress_bar_type='tqdm')
        df.to_parquet(out_path, index=False)
        print(f"      {len(df):,} rows -> {out_path}")
        time.sleep(1)

    print('\nAll chunks complete.')
else:
    print('RUN_LIVE=False, so only pre-live validation was executed.')

# ---- Standalone re-export (if feature table already built, skip re-running queries) ----
import time
import os

save_dir = '/home/ubuntu/data/chunks'
os.makedirs(save_dir, exist_ok=True)

for name, (start, end) in zip(names, chunks):
    out_path = os.path.join(save_dir, f'{name}.parquet')

    if os.path.exists(out_path):
        print(f"Skip {name} (already saved)")
        continue

    print(f"Exporting {name} ({start} -> {end})...")
    q = f"""
        SELECT *
        FROM {DATASET_ID}.solana_drain_features
        WHERE tx_date BETWEEN '{start}' AND '{end}'
    """
    df = client.query(q, location='us-central1').to_dataframe(progress_bar_type='tqdm')
    df.to_parquet(out_path, index=False)
    print(f"  {name}: {len(df):,} rows saved")
    time.sleep(1)

import pandas as pd

# ---- build flat dataset from monthly parquet chunks ----
chunk_dir = '/home/ubuntu/data/chunks'
chunk_names = ['sep', 'oct', 'nov']
chunk_paths = [os.path.join(chunk_dir, f'{m}.parquet') for m in chunk_names]

missing = [p for p in chunk_paths if not os.path.exists(p)]
if missing:
    raise FileNotFoundError('Missing parquet chunks: ' + ', '.join(missing))

df_parts = [pd.read_parquet(p) for p in chunk_paths]
df_flat = pd.concat(df_parts, ignore_index=True)

print('Loaded chunks:', ', '.join(chunk_names))
print('df_flat shape:', df_flat.shape)
print('Columns:', len(df_flat.columns))

import numpy as np
from pandas.api.types import is_datetime64_any_dtype

# ---- keep DeFi rows and add explicit context feature ----
DEX_PROGRAMS = {
    'JUP6LkbZbjS1jKKccwgws78vMR7maRciM379Gbu8ZQP',  # Jupiter
    '675k1q2AY9GesSSTNBbuGu2KmC69TdtTkiB1C6XMRSE',  # Raydium
    'whirLbMiqSk27Lmc2rrB8meSZu8D9S2rDJSmMJCvA6',   # Orca
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',  # Pump.fun
}
def has_defi_program(program_list):
    if program_list is None or (isinstance(program_list, float) and np.isnan(program_list)):
        return 0
    if isinstance(program_list, (list, tuple, set)):
        return int(any(p in DEX_PROGRAMS for p in program_list))
    if isinstance(program_list, str):
        return int(any(p in program_list for p in DEX_PROGRAMS))
    return 0

# robust datetime handling (works with tz-aware and tz-naive)
if not is_datetime64_any_dtype(df_flat['block_timestamp']):
    df_flat['block_timestamp'] = pd.to_datetime(df_flat['block_timestamp'], utc=True, errors='coerce')
elif getattr(df_flat['block_timestamp'].dt, 'tz', None) is None:
    df_flat['block_timestamp'] = df_flat['block_timestamp'].dt.tz_localize('UTC')
df_flat = df_flat.sort_values(['wallet', 'block_timestamp']).copy()
df_flat['delta_time'] = (
    df_flat.groupby('wallet')['block_timestamp']
      .diff()
      .dt.total_seconds()
      .fillna(0.0)
)
df_flat['is_defi'] = df_flat['program_list'].apply(has_defi_program).astype('int8')
feature_cols = [
    'fee_sol', 'compute_units_consumed', 'num_accounts', 'num_signers', 'num_writable',
    'num_token_transfers', 'unique_mints', 'unique_destinations', 'fanout_ratio',
    'cpi_ratio', 'wallet_activity_rate', 'avg_token_transfer', 'inner_instructions',
    'num_instructions', 'delta_time', 'is_defi'
]

for col in feature_cols:
    df_flat[col] = pd.to_numeric(df_flat[col], errors='coerce').fillna(0.0)
max_seq_len = 50
seq_list = []
wallet_ids = []
for wallet, group in df_flat.groupby('wallet', sort=False):
    seq = group[feature_cols].to_numpy(dtype=np.float32)[-max_seq_len:]
    padded = np.zeros((max_seq_len, len(feature_cols)), dtype=np.float32)
    padded[-len(seq):] = seq
    seq_list.append(padded)
    wallet_ids.append(wallet)

import torch

X_np = np.stack(seq_list) if seq_list else np.empty((0, max_seq_len, len(feature_cols)), dtype=np.float32)
X = torch.from_numpy(X_np)
print('Sequence tensor shape:', tuple(X.shape))
print('Feature count:', len(feature_cols))

# jobs = list(client.list_jobs(max_results=50, all_users=True))
# total_cost = 0
# for job in sorted(jobs, key=lambda j: j.created, reverse=True):
#     if hasattr(job, 'total_bytes_processed') and job.total_bytes_processed:
#         gb    = job.total_bytes_processed / 1e9
#         cost  = (job.total_bytes_processed / 1e12) * 5
#         total_cost += cost
#         print(f"{str(job.created)[:19]}  {job.state:<9}  {gb:>10.1f} GB  ${cost:>8.2f}  {job.job_id[:30]}")
# print(f"\nTotal estimated cost: ${total_cost:.2f}")

from sklearn.ensemble import IsolationForest

# ---- IF anomaly detection on sequence-derived features ----
if X_np.shape[0] == 0:
    raise RuntimeError('No sequences found for IF anomaly detection.')

# Build compact wallet-level representation from each sequence.
seq_last = X_np[:, -1, :]
seq_mean = X_np.mean(axis=1)
seq_std = X_np.std(axis=1)
X_if = np.concatenate([seq_last, seq_mean, seq_std], axis=1)

if_model = IsolationForest(
    n_estimators=300,
    contamination='auto',
    random_state=42,
    n_jobs=-1,
)
if_model.fit(X_if)

# decision_function: higher is more normal. We invert for anomaly score.
if_raw = if_model.decision_function(X_if)
if_score = -if_raw
if_pred = if_model.predict(X_if)  # -1 anomaly, 1 inlier

df_if = pd.DataFrame({
    'wallet': wallet_ids,
    'if_score': if_score,
    'if_is_anomaly': (if_pred == -1).astype('int8'),
})

print('IF input shape:', X_if.shape)
print('Detected anomalies:', int(df_if['if_is_anomaly'].sum()))
print('Top 20 wallets by IF score:')
print(df_if.sort_values('if_score', ascending=False).head(20).to_string(index=False))

# ---- Project wallet IF risk back to all transaction rows (910k) ----
df_wallet_scores = df_if.copy()

wallet_score_quantile = 0.99
wallet_cutoff = float(df_wallet_scores['if_score'].quantile(wallet_score_quantile))
df_wallet_scores['if_is_anomaly_strict'] = (
    df_wallet_scores['if_score'] >= wallet_cutoff
).astype('int8')

df_tx_scored = df_flat.merge(
    df_wallet_scores,
    on='wallet',
    how='left',
    validate='many_to_one',
)

df_tx_scored['if_score'] = pd.to_numeric(df_tx_scored['if_score'], errors='coerce').fillna(0.0)
for col in ['if_is_anomaly', 'if_is_anomaly_strict']:
    df_tx_scored[col] = pd.to_numeric(df_tx_scored[col], errors='coerce').fillna(0).astype('int8')

print('Wallet quantile threshold:', wallet_score_quantile)
print('Wallet score cutoff:', round(wallet_cutoff, 6))
print('Scored transaction table shape:', df_tx_scored.shape)
print('Rows with default IF flag:', int(df_tx_scored['if_is_anomaly'].sum()))
print('Rows with strict wallet flag:', int(df_tx_scored['if_is_anomaly_strict'].sum()))
print('Unique strict anomalous wallets:', int(df_tx_scored.loc[df_tx_scored['if_is_anomaly_strict'] == 1, 'wallet'].nunique()))

cols_show = [
    'wallet', 'block_timestamp', 'signature', 'if_score', 'if_is_anomaly', 'if_is_anomaly_strict',
    'drain_sol_ratio', 'wallet_sol_delta', 'max_balance_change',
]
cols_show = [c for c in cols_show if c in df_tx_scored.columns]
print('\nTop 20 rows by wallet anomaly score:')
print(df_tx_scored.sort_values('if_score', ascending=False)[cols_show].head(20).to_string(index=False))

# Unified ML (Chollet-style) anomaly pipeline
# Component flow (wallet-level):
#   BigQuery Features -> Sequence Prep -> IF (sklearn) + Transformer (PyTorch)
#                     -> Score Fusion -> Conformal Calibration -> Wallet Alerts
#                     -> Project back to transactions (910k)

from dataclasses import dataclass
from typing import Dict, List, Tuple

def robust_z(s: pd.Series) -> pd.Series:
    med = s.median()
    iqr = s.quantile(0.75) - s.quantile(0.25)
    scale = float(iqr) if float(iqr) > 1e-12 else 1.0
    return (s - med) / scale


def conformal_threshold(cal_scores: np.ndarray, alpha: float) -> float:
    n = len(cal_scores)
    if n < 30:
        raise RuntimeError(f'Calibration set too small: n={n}')
    k = int(np.ceil((n + 1) * (1 - alpha))) - 1
    k = min(max(k, 0), n - 1)
    return float(np.sort(cal_scores)[k])


def conformal_p_values(cal_scores: np.ndarray, test_scores: np.ndarray) -> np.ndarray:
    n = len(cal_scores)
    return (1 + (cal_scores.reshape(1, -1) >= test_scores.reshape(-1, 1)).sum(axis=1)) / (n + 1)


# Build wallet timestamp index for temporal split.
wallet_time = (
    df_flat.groupby('wallet', as_index=False)['block_timestamp']
    .min()
    .rename(columns={'block_timestamp': 'wallet_first_ts'})
)

wallet_index = pd.DataFrame({'wallet': wallet_ids}).merge(wallet_time, on='wallet', how='left', validate='one_to_one')
wallet_index = wallet_index.sort_values('wallet_first_ts').reset_index(drop=True)

n_wallets = len(wallet_index)
train_end = int(0.60 * n_wallets)
cal_end = int(0.80 * n_wallets)

train_wallets = set(wallet_index.loc[:train_end - 1, 'wallet'])
cal_wallets = set(wallet_index.loc[train_end:cal_end - 1, 'wallet'])
live_wallets = set(wallet_index.loc[cal_end:, 'wallet'])

wallet_to_pos = {w: i for i, w in enumerate(wallet_ids)}
train_idx = np.array([wallet_to_pos[w] for w in wallet_ids if w in train_wallets], dtype=np.int64)
cal_idx = np.array([wallet_to_pos[w] for w in wallet_ids if w in cal_wallets], dtype=np.int64)
live_idx = np.array([wallet_to_pos[w] for w in wallet_ids if w in live_wallets], dtype=np.int64)

print('Wallet split sizes (train/cal/live):', len(train_idx), len(cal_idx), len(live_idx))

from torch import nn

class SeqTransformerAutoencoder(nn.Module):
    def __init__(self, in_dim: int, d_model: int = 64, nhead: int = 4, num_layers: int = 2, ff_dim: int = 128, dropout: float = 0.1):
        super().__init__()
        self.in_proj = nn.Linear(in_dim, d_model)
        enc_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=ff_dim,
            dropout=dropout,
            batch_first=True,
            activation='gelu',
        )
        self.encoder = nn.TransformerEncoder(enc_layer, num_layers=num_layers)
        self.out_proj = nn.Linear(d_model, in_dim)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        z = self.in_proj(x)
        z = self.encoder(z)
        return self.out_proj(z)


def train_transformer_ae(X_train: np.ndarray, cfg: Dict, device: str = 'cpu') -> nn.Module:
    model = SeqTransformerAutoencoder(
        in_dim=X_train.shape[-1],
        d_model=cfg['d_model'],
        nhead=cfg['nhead'],
        num_layers=cfg['num_layers'],
        ff_dim=cfg['ff_dim'],
        dropout=cfg['dropout'],
    ).to(device)

    opt = torch.optim.AdamW(model.parameters(), lr=cfg['lr'], weight_decay=cfg['weight_decay'])
    loss_fn = nn.MSELoss()

    train_tensor = torch.from_numpy(X_train).float()
    loader = torch.utils.data.DataLoader(train_tensor, batch_size=cfg['batch_size'], shuffle=True)

    model.train()
    for _ in range(cfg['epochs']):
        for xb in loader:
            xb = xb.to(device)
            pred = model(xb)
            loss = loss_fn(pred, xb)
            opt.zero_grad(set_to_none=True)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()

    return model


def transformer_recon_score(model: nn.Module, X_all: np.ndarray, device: str = 'cpu') -> np.ndarray:
    model.eval()
    xs = torch.from_numpy(X_all).float()
    loader = torch.utils.data.DataLoader(xs, batch_size=512, shuffle=False)
    scores: List[np.ndarray] = []
    with torch.no_grad():
        for xb in loader:
            xb = xb.to(device)
            pred = model(xb)
            err = torch.mean((pred - xb) ** 2, dim=(1, 2))
            scores.append(err.cpu().numpy())
    return np.concatenate(scores)


def run_experiment(cfg: Dict, X_np: np.ndarray, wallet_ids: List[str], train_idx: np.ndarray, cal_idx: np.ndarray, live_idx: np.ndarray) -> Tuple[pd.DataFrame, Dict]:
    seq_last = X_np[:, -1, :]
    seq_mean = X_np.mean(axis=1)
    seq_std = X_np.std(axis=1)
    X_if = np.concatenate([seq_last, seq_mean, seq_std], axis=1)

    if_model = IsolationForest(
        n_estimators=cfg['if_n_estimators'],
        max_samples=cfg['if_max_samples'],
        contamination='auto',
        random_state=cfg['seed'],
        n_jobs=-1,
    )
    if_model.fit(X_if[train_idx])
    if_score = -if_model.decision_function(X_if)

    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    torch.manual_seed(cfg['seed'])
    np.random.seed(cfg['seed'])

    tr_model = train_transformer_ae(X_np[train_idx], cfg, device=device)
    tr_score = transformer_recon_score(tr_model, X_np, device=device)

    df = pd.DataFrame({
        'wallet': wallet_ids,
        'if_score': if_score,
        'tr_score': tr_score,
    })
    df['if_z'] = robust_z(df['if_score'])
    df['tr_z'] = robust_z(df['tr_score'])
    df['raw_score'] = cfg['w_if'] * df['if_z'] + cfg['w_tr'] * df['tr_z']

    cal_scores = df.iloc[cal_idx]['raw_score'].to_numpy()
    q_hat = conformal_threshold(cal_scores, cfg['alpha'])

    live_scores = df.iloc[live_idx]['raw_score'].to_numpy()
    live_p = conformal_p_values(cal_scores, live_scores)

    df['conformal_p'] = np.nan
    df['conformal_flag'] = 0
    df.loc[df.index[live_idx], 'conformal_p'] = live_p
    df.loc[df.index[live_idx], 'conformal_flag'] = (live_scores > q_hat).astype('int8')

    live_flag_rate = float(df.iloc[live_idx]['conformal_flag'].mean())
    target_gap = abs(live_flag_rate - cfg['alpha'])

    metrics = {
        'name': cfg['name'],
        'alpha': cfg['alpha'],
        'q_hat': q_hat,
        'live_flag_rate': live_flag_rate,
        'live_flag_wallets': int(df.iloc[live_idx]['conformal_flag'].sum()),
        'target_gap': target_gap,
    }
    return df, metrics


experiment_grid = [
    {
        'name': 'baseline_balanced',
        'seed': 42,
        'if_n_estimators': 300,
        'if_max_samples': 'auto',
        'd_model': 64,
        'nhead': 4,
        'num_layers': 2,
        'ff_dim': 128,
        'dropout': 0.10,
        'lr': 1e-3,
        'weight_decay': 1e-4,
        'epochs': 4,
        'batch_size': 256,
        'w_if': 0.50,
        'w_tr': 0.50,
        'alpha': 0.01,
    },
    {
        'name': 'if_heavy',
        'seed': 42,
        'if_n_estimators': 500,
        'if_max_samples': 2048,
        'd_model': 64,
        'nhead': 4,
        'num_layers': 2,
        'ff_dim': 128,
        'dropout': 0.10,
        'lr': 8e-4,
        'weight_decay': 1e-4,
        'epochs': 4,
        'batch_size': 256,
        'w_if': 0.70,
        'w_tr': 0.30,
        'alpha': 0.01,
    },
    {
        'name': 'transformer_heavy',
        'seed': 42,
        'if_n_estimators': 300,
        'if_max_samples': 'auto',
        'd_model': 96,
        'nhead': 4,
        'num_layers': 3,
        'ff_dim': 192,
        'dropout': 0.15,
        'lr': 8e-4,
        'weight_decay': 1e-4,
        'epochs': 5,
        'batch_size': 256,
        'w_if': 0.35,
        'w_tr': 0.65,
        'alpha': 0.01,
    },
]

all_runs = []
run_outputs = {}
for cfg in experiment_grid:
    print(f"Running experiment: {cfg['name']}")
    df_run, metrics = run_experiment(cfg, X_np, wallet_ids, train_idx, cal_idx, live_idx)
    all_runs.append(metrics)
    run_outputs[cfg['name']] = df_run

exp_results = pd.DataFrame(all_runs).sort_values(['target_gap', 'live_flag_wallets']).reset_index(drop=True)
print('\nExperiment leaderboard:')
print(exp_results.to_string(index=False))

best_name = exp_results.iloc[0]['name']
df_wallet_final = run_outputs[best_name].copy()
print(f"\nSelected best run: {best_name}")

# ---- Project final calibrated wallet score back to transaction rows ----

wallet_cols = ['wallet', 'if_score', 'tr_score', 'raw_score', 'conformal_p', 'conformal_flag']
df_wallet_serving = df_wallet_final[wallet_cols].copy()
df_wallet_serving['risk_score'] = -np.log10(np.clip(df_wallet_serving['conformal_p'].fillna(1.0), 1e-12, 1.0))
df_wallet_serving['risk_tier'] = pd.cut(
    df_wallet_serving['risk_score'],
    bins=[-np.inf, 1, 2, 3, np.inf],
    labels=['low', 'medium', 'high', 'critical'],
)

df_tx_final = df_flat.merge(
    df_wallet_serving,
    on='wallet',
    how='left',
    validate='many_to_one',
)

df_tx_final['conformal_flag'] = pd.to_numeric(df_tx_final['conformal_flag'], errors='coerce').fillna(0).astype('int8')
df_tx_final['conformal_p'] = pd.to_numeric(df_tx_final['conformal_p'], errors='coerce').fillna(1.0)
df_tx_final['risk_score'] = pd.to_numeric(df_tx_final['risk_score'], errors='coerce').fillna(0.0)

print('Final transaction table shape:', df_tx_final.shape)
print('Flagged tx rows (conformal):', int(df_tx_final['conformal_flag'].sum()))
print('Flagged wallets (conformal):', int(df_tx_final.loc[df_tx_final['conformal_flag'] == 1, 'wallet'].nunique()))

show_cols = [
    'wallet', 'block_timestamp', 'signature',
    'if_score', 'tr_score', 'raw_score', 'conformal_p', 'conformal_flag', 'risk_tier',
    'drain_sol_ratio', 'wallet_sol_delta', 'max_balance_change',
]
show_cols = [c for c in show_cols if c in df_tx_final.columns]
print('\nTop 20 rows by fused raw_score:')
print(df_tx_final.sort_values('raw_score', ascending=False)[show_cols].head(20).to_string(index=False))

# Merge wallet-level model outputs (df_tr from unified pipeline)
df_tr = df_wallet_final[['wallet', 'tr_score']].copy()
df_scores = df_if[['wallet', 'if_score']].merge(
    df_tr[['wallet', 'tr_score']],
    on='wallet',
    how='inner',
    validate='one_to_one'
).copy()

def robust_z(s: pd.Series) -> pd.Series:
    med = s.median()
    iqr = s.quantile(0.75) - s.quantile(0.25)
    scale = iqr if iqr > 1e-12 else 1.0
    return (s - med) / scale

df_scores['if_z'] = robust_z(df_scores['if_score'])
df_scores['tr_z'] = robust_z(df_scores['tr_score'])
w_if, w_tr = 0.5, 0.5
df_scores['raw_score'] = w_if * df_scores['if_z'] + w_tr * df_scores['tr_z']
