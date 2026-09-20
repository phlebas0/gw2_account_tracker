# GW2 Cloud Account + Flip Tracker

One cloud-hosted project that:

- collects a broad set of authenticated Guild Wars 2 account data;
- retains daily historical account, wallet, mastery and character metrics;
- captures unlocks and PvP matches without duplicating them every day;
- keeps current JSON state for the long tail of account endpoints and records only distinct state changes;
- imports Trading Post current/history transactions and retains the FIFO flip-profit calculation;
- estimates liquid account value using current Trading Post prices;
- writes directly to Supabase PostgreSQL;
- runs automatically through GitHub Actions; and
- exposes the history through Streamlit Community Cloud.

## Files

- `collector.py` — scheduled entry point.
- `tracker_core.py` — GW2 API, storage, valuation and FIFO logic.
- `streamlit_app.py` — dashboard.
- `.github/workflows/collect.yml` — daily cloud schedule.

## GitHub secrets

Repository → Settings → Secrets and variables → Actions:

- `GW2_API_KEY`
- `DATABASE_URL`

The database schema is created automatically by the first collector run.

## First run

After committing the files to the default branch:

1. Open **Actions**.
2. Select **Collect GW2 data**.
3. Choose **Run workflow**.
4. Open the completed run and inspect its log.

A successful run ends with output similar to:

```text
GW2 cloud snapshot complete
Account: ...
Liquid gold: ...
Liquid net worth estimate: ...
Characters: ...
Playtime: ...
Endpoints: ... ok / ... failed
```

Some endpoints can legitimately fail if an API permission is absent or if ArenaNet temporarily returns an endpoint-specific error. These are logged to `endpoint_errors` rather than aborting the entire run.

## Streamlit

Create the Community Cloud app from the same GitHub repository and use:

```text
streamlit_app.py
```

as the entry point.

Streamlit secrets are separate from GitHub Actions secrets. In the Streamlit app settings add:

```toml
DATABASE_URL = "your Supabase connection string"
```

The dashboard does **not** need the GW2 API key because collection is handled by GitHub Actions.

## Collection schedule

The supplied workflow runs at 03:17 Europe/London each day and can also be run manually. Change the cron expression in `.github/workflows/collect.yml` if you later want two snapshots per day.

## Storage model

### Historical snapshots

Small numerical histories are retained every run:

- account summary and wealth;
- all wallet currencies;
- mastery points by region;
- character playtime/deaths;
- PvP/account headline metrics.

### Event/first-seen data

Data is inserted once where possible:

- unlock IDs;
- PvP matches;
- Trading Post transactions.

### Hashed state changes

Large or irregular endpoint responses use `current_state` plus `state_changes`. The full current response is always available, while another historical JSON blob is only stored when its hash changes.

### Character raw state

Full character data changes constantly because playtime and deaths are counters. Those volatile fields are removed before hashing character state, while their numeric history is retained separately in `character_snapshots`.

## Wealth definition

`liquid_net_worth` is intentionally a transparent, liquid-value estimate rather than a claim to reproduce GW2Efficiency's proprietary valuation exactly. It currently includes:

- wallet coin;
- delivery-box coin;
- buy-order coin already committed to the Trading Post;
- current sell orders after the 15% Trading Post fee/tax assumption;
- bank, material storage, shared inventory, character bag inventory and delivery items valued at the current highest buy price after the same 15% assumption.

The component market values are also stored separately, so this definition can be changed later without losing the underlying historical metrics.

## Achievement points

ArenaNet does not currently expose one authoritative total-AP field. The collector therefore caches public achievement metadata and derives a best-effort total from completed achievement tiers, adding the account-level historic daily/monthly AP counters. The underlying account achievement JSON is retained so the calculation can be refined without losing source data.

## Runtime-optimised daily collection

The collector is scheduled **once per day**. There are no separate weekly or monthly GitHub Actions.

To keep that single daily run fast:

- Dynamic account, wallet, wealth, AP, mastery, character summary, PvP/WvW, Wizard's Vault and reset-based data are collected daily.
- Completed Trading Post history is incremental: after the first import, pagination stops as soon as an already-stored transaction is reached.
- FIFO flips are rebuilt only when new completed TP transactions were found.
- Independent GW2 API requests run concurrently (up to 8 workers).
- Trading Post price batches run concurrently.
- Unlock collections are refreshed every 7 days *inside the daily job*; their existing counts remain available on intervening days.
- Bulky per-character hero point / quest / SAB / dungeon state is refreshed every 30 days *inside the daily job*.
- Currency metadata is refreshed every 30 days; achievement metadata remains cached and only missing achievement definitions are fetched.
- Each run records `runtime_seconds` in `job_runs` and prints per-stage timings in the GitHub Actions log.

The schedule remains one daily run at 03:17 Europe/London.
