import os
from datetime import timezone

import pandas as pd
import streamlit as st
from sqlalchemy import text

from tracker_core import copper_to_str, ensure_schema, make_engine

st.set_page_config(page_title="GW2 Account Tracker", layout="wide")

# Streamlit Cloud secrets are separate from GitHub Actions secrets.
if not os.getenv("DATABASE_URL") and "DATABASE_URL" in st.secrets:
    os.environ["DATABASE_URL"] = st.secrets["DATABASE_URL"]

if not os.getenv("DATABASE_URL"):
    st.error("DATABASE_URL is not configured in Streamlit secrets.")
    st.stop()

engine = make_engine()
ensure_schema(engine)


def query_df(sql: str, params=None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def latest_snapshot():
    df = query_df("SELECT * FROM account_snapshots ORDER BY snapshot_at DESC LIMIT 1")
    return None if df.empty else df.iloc[0]


def delta_since(column: str, days: int):
    df = query_df(f"""
        WITH latest AS (
            SELECT snapshot_at, {column} AS value FROM account_snapshots
            ORDER BY snapshot_at DESC LIMIT 1
        ), prior AS (
            SELECT snapshot_at, {column} AS value FROM account_snapshots
            WHERE snapshot_at <= (SELECT snapshot_at FROM latest) - INTERVAL '{days} days'
            ORDER BY snapshot_at DESC LIMIT 1
        )
        SELECT (SELECT value FROM latest) AS latest_value,
               (SELECT value FROM prior) AS prior_value
    """)
    if df.empty or pd.isna(df.iloc[0]["prior_value"]):
        return None
    return int(df.iloc[0]["latest_value"] - df.iloc[0]["prior_value"])


latest = latest_snapshot()

st.title("GW2 Account Tracker")
if latest is None:
    st.info("No snapshot has been collected yet. Run the GitHub Actions workflow once, then refresh this page.")
    st.stop()

snapshot_time = pd.to_datetime(latest["snapshot_at"], utc=True)
st.caption(f"Last collected: {snapshot_time.strftime('%d %b %Y %H:%M UTC')} · {int(latest['endpoints_ok'] or 0)} endpoints OK / {int(latest['endpoints_failed'] or 0)} failed")

net_delta = delta_since("liquid_net_worth", 30)
gold_delta = delta_since("liquid_gold", 30)
play_delta = delta_since("total_playtime_seconds", 30)
skin_delta = delta_since("skins_count", 30)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Liquid net worth", copper_to_str(int(latest["liquid_net_worth"] or 0)), copper_to_str(net_delta) if net_delta is not None else None)
c2.metric("Liquid gold", copper_to_str(int(latest["liquid_gold"] or 0)), copper_to_str(gold_delta) if gold_delta is not None else None)
c3.metric("Achievement points", f"{int(latest['achievement_points_estimate'] or 0):,}")
c4.metric("Playtime", f"{(latest['total_playtime_seconds'] or 0) / 3600:,.0f} h", f"{play_delta/3600:+,.1f} h" if play_delta is not None else None)
c5.metric("Skins", f"{int(latest['skins_count'] or 0):,}", f"{skin_delta:+,}" if skin_delta is not None else None)

st.subheader("Account history")
history = query_df("""
    SELECT snapshot_at, liquid_net_worth, liquid_gold,
           total_playtime_seconds, skins_count, mastery_earned, pvp_rank, wvw_rank
    FROM account_snapshots
    ORDER BY snapshot_at
""")
if not history.empty:
    history["snapshot_at"] = pd.to_datetime(history["snapshot_at"], utc=True)
    history["net_worth_gold"] = history["liquid_net_worth"] / 10000
    st.line_chart(history.set_index("snapshot_at")[["net_worth_gold"]], y_label="Gold")

left, right = st.columns(2)
with left:
    st.subheader("Progression")
    prog = pd.DataFrame([
        ["Mastery earned", int(latest["mastery_earned"] or 0)],
        ["Mastery unspent", int(latest["mastery_unspent"] or 0)],
        ["Fractal level", latest["fractal_level"]],
        ["WvW rank", latest["wvw_rank"]],
        ["PvP rank", latest["pvp_rank"]],
        ["PvP wins", int(latest["pvp_wins"] or 0)],
        ["PvP losses", int(latest["pvp_losses"] or 0)],
        ["Legendary items", int(latest["legendary_count"] or 0)],
    ], columns=["Metric", "Value"])
    st.dataframe(prog, hide_index=True, use_container_width=True)

with right:
    st.subheader("Wealth breakdown")
    wealth = pd.DataFrame([
        ["Bank", int(latest["bank_market_value"] or 0)],
        ["Materials", int(latest["materials_market_value"] or 0)],
        ["Shared inventory", int(latest["shared_inventory_market_value"] or 0)],
        ["Character inventories", int(latest["character_inventory_market_value"] or 0)],
        ["Delivery box items", int(latest["delivery_market_value"] or 0)],
        ["Buy orders", int(latest["buy_order_committed"] or 0)],
        ["Sell orders (net)", int(latest["sell_order_net_value"] or 0)],
    ], columns=["Component", "Copper"])
    wealth["Value"] = wealth["Copper"].map(copper_to_str)
    st.dataframe(wealth[["Component", "Value"]], hide_index=True, use_container_width=True)

st.subheader("Wallet")
wallet = query_df("""
    WITH last_time AS (SELECT MAX(snapshot_at) AS t FROM wallet_snapshots)
    SELECT COALESCE(cm.name, 'Currency ' || ws.currency_id::text) AS currency,
           ws.value,
           cm.display_order
    FROM wallet_snapshots ws
    LEFT JOIN currency_metadata cm ON cm.currency_id = ws.currency_id
    WHERE ws.snapshot_at = (SELECT t FROM last_time)
    ORDER BY COALESCE(cm.display_order, 999999), currency
""")
st.dataframe(wallet[["currency", "value"]], hide_index=True, use_container_width=True, height=420)

st.subheader("Characters")
chars = query_df("""
    WITH last_time AS (SELECT MAX(snapshot_at) AS t FROM character_snapshots)
    SELECT character_name AS name, profession, race, level,
           ROUND(age_seconds / 3600.0, 1) AS playtime_hours, deaths
    FROM character_snapshots
    WHERE snapshot_at = (SELECT t FROM last_time)
    ORDER BY age_seconds DESC
""")
st.dataframe(chars, hide_index=True, use_container_width=True)

st.subheader("Trading")
t1, t2, t3 = st.columns(3)
t1.metric("Realised FIFO profit", copper_to_str(int(latest["realised_flip_profit"] or 0)))
t2.metric("Matched flip rows", f"{int(latest['matched_flip_rows'] or 0):,}")
match_count = query_df("SELECT COUNT(*) AS n FROM pvp_matches").iloc[0]["n"]
t3.metric("PvP matches retained", f"{int(match_count):,}")

with st.expander("Collector health"):
    runs = query_df("SELECT started_at, finished_at, status, endpoints_ok, endpoints_failed, message FROM job_runs ORDER BY id DESC LIMIT 20")
    st.dataframe(runs, hide_index=True, use_container_width=True)
    errors = query_df("SELECT snapshot_at, endpoint, status_code, error FROM endpoint_errors ORDER BY id DESC LIMIT 50")
    if not errors.empty:
        st.write("Recent endpoint errors")
        st.dataframe(errors, hide_index=True, use_container_width=True)
