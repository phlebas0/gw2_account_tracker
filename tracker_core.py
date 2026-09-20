from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib.parse import quote_plus

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

GW2_API = "https://api.guildwars2.com/v2"
TP_TAX = 0.15
BATCH_SIZE = 200


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def state_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def copper_to_str(copper: int | None) -> str:
    if copper is None:
        return "—"
    sign = "-" if copper < 0 else ""
    copper = abs(int(copper))
    gold, rem = divmod(copper, 10000)
    silver, copper = divmod(rem, 100)
    parts = []
    if gold:
        parts.append(f"{gold:,}g")
    if silver:
        parts.append(f"{silver}s")
    if copper or not parts:
        parts.append(f"{copper}c")
    return sign + " ".join(parts)


def normalise_database_url(url: str) -> str:
    """Make Supabase/Postgres URLs work with SQLAlchemy + psycopg 3."""
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url[len("postgresql://"):]
    return url


def make_engine(database_url: str | None = None) -> Engine:
    url = normalise_database_url(database_url or os.environ["DATABASE_URL"])
    return create_engine(url, pool_pre_ping=True, future=True)


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS job_runs (
        id BIGSERIAL PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        status TEXT NOT NULL,
        endpoints_ok INTEGER NOT NULL DEFAULT 0,
        endpoints_failed INTEGER NOT NULL DEFAULT 0,
        message TEXT
    )
    """,
    "ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS runtime_seconds DOUBLE PRECISION",
    """
    CREATE TABLE IF NOT EXISTS account_snapshots (
        snapshot_at TIMESTAMPTZ PRIMARY KEY,
        account_id TEXT,
        account_name TEXT,
        account_age_seconds BIGINT,
        world_id INTEGER,
        commander BOOLEAN,
        fractal_level INTEGER,
        wvw_rank INTEGER,
        daily_ap INTEGER,
        monthly_ap INTEGER,
        achievement_points_estimate INTEGER,
        liquid_gold BIGINT,
        account_item_market_value BIGINT,
        account_item_liquidation_value BIGINT,
        bank_market_value BIGINT,
        materials_market_value BIGINT,
        shared_inventory_market_value BIGINT,
        character_inventory_market_value BIGINT,
        delivery_market_value BIGINT,
        delivery_coins BIGINT,
        buy_order_committed BIGINT,
        sell_order_net_value BIGINT,
        liquid_net_worth BIGINT,
        total_playtime_seconds BIGINT,
        total_deaths BIGINT,
        character_count INTEGER,
        mastery_earned INTEGER,
        mastery_spent INTEGER,
        mastery_unspent INTEGER,
        pvp_rank INTEGER,
        pvp_rank_points BIGINT,
        pvp_wins INTEGER,
        pvp_losses INTEGER,
        luck BIGINT,
        skins_count INTEGER,
        dyes_count INTEGER,
        titles_count INTEGER,
        minis_count INTEGER,
        outfits_count INTEGER,
        gliders_count INTEGER,
        mount_skins_count INTEGER,
        recipes_count INTEGER,
        legendary_unique INTEGER,
        legendary_count INTEGER,
        realised_flip_profit BIGINT,
        matched_flip_rows INTEGER,
        endpoints_ok INTEGER,
        endpoints_failed INTEGER
    )
    """,
    "ALTER TABLE account_snapshots ADD COLUMN IF NOT EXISTS achievement_points_estimate INTEGER",
    """
    CREATE TABLE IF NOT EXISTS wallet_snapshots (
        snapshot_at TIMESTAMPTZ NOT NULL,
        currency_id INTEGER NOT NULL,
        value BIGINT NOT NULL,
        PRIMARY KEY (snapshot_at, currency_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS achievement_metadata (
        achievement_id INTEGER PRIMARY KEY,
        name TEXT,
        flags JSONB,
        tiers JSONB,
        point_cap INTEGER,
        raw_json JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS currency_metadata (
        currency_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        icon TEXT,
        display_order INTEGER,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mastery_snapshots (
        snapshot_at TIMESTAMPTZ NOT NULL,
        region TEXT NOT NULL,
        earned INTEGER NOT NULL,
        spent INTEGER NOT NULL,
        PRIMARY KEY (snapshot_at, region)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS character_snapshots (
        snapshot_at TIMESTAMPTZ NOT NULL,
        character_name TEXT NOT NULL,
        race TEXT,
        profession TEXT,
        level INTEGER,
        age_seconds BIGINT,
        deaths BIGINT,
        created_at TIMESTAMPTZ,
        guild_id TEXT,
        PRIMARY KEY (snapshot_at, character_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS unlocks (
        unlock_type TEXT NOT NULL,
        unlock_id TEXT NOT NULL,
        first_seen TIMESTAMPTZ NOT NULL,
        last_seen TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (unlock_type, unlock_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS current_state (
        state_key TEXT PRIMARY KEY,
        state_hash TEXT NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        raw_json JSONB NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS state_changes (
        id BIGSERIAL PRIMARY KEY,
        state_key TEXT NOT NULL,
        state_hash TEXT NOT NULL,
        first_seen TIMESTAMPTZ NOT NULL,
        raw_json JSONB NOT NULL,
        UNIQUE (state_key, state_hash)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS endpoint_errors (
        id BIGSERIAL PRIMARY KEY,
        snapshot_at TIMESTAMPTZ NOT NULL,
        endpoint TEXT NOT NULL,
        status_code INTEGER,
        error TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pvp_matches (
        match_id TEXT PRIMARY KEY,
        first_seen TIMESTAMPTZ NOT NULL,
        started_at TIMESTAMPTZ,
        ended_at TIMESTAMPTZ,
        map_id INTEGER,
        result TEXT,
        team TEXT,
        rating_type TEXT,
        raw_json JSONB NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tp_transactions (
        transaction_id BIGINT NOT NULL,
        side TEXT NOT NULL,
        transaction_set TEXT NOT NULL,
        item_id INTEGER NOT NULL,
        price BIGINT NOT NULL,
        quantity INTEGER NOT NULL,
        created TIMESTAMPTZ,
        purchased TIMESTAMPTZ,
        raw_json JSONB NOT NULL,
        PRIMARY KEY (transaction_id, side, transaction_set)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS matched_flips (
        id BIGSERIAL PRIMARY KEY,
        item_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        buy_price BIGINT NOT NULL,
        sell_price BIGINT NOT NULL,
        profit BIGINT NOT NULL,
        buy_date TIMESTAMPTZ,
        sell_date TIMESTAMPTZ
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_wallet_currency_time ON wallet_snapshots(currency_id, snapshot_at)",
    "CREATE INDEX IF NOT EXISTS idx_character_name_time ON character_snapshots(character_name, snapshot_at)",
    "CREATE INDEX IF NOT EXISTS idx_state_changes_key_time ON state_changes(state_key, first_seen)",
    "CREATE INDEX IF NOT EXISTS idx_tp_item ON tp_transactions(item_id)",
]


def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        for stmt in SCHEMA_STATEMENTS:
            conn.execute(text(stmt))


@dataclass
class FetchResult:
    endpoint: str
    ok: bool
    data: Any = None
    status_code: int | None = None
    error: str | None = None


class GW2Api:
    """Thread-safe GW2 API client with connection reuse per worker thread."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ["GW2_API_KEY"]
        self._local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": "gw2-cloud-tracker/1.1",
            })
            self._local.session = session
        return session

    def get(self, path: str, **params: Any) -> Any:
        r = self._session().get(
            f"{GW2_API}{path}",
            params=params or None,
            timeout=(5, 15),
        )
        r.raise_for_status()
        return r.json()

    def try_get(self, path: str, **params: Any) -> FetchResult:
        try:
            return FetchResult(path, True, self.get(path, **params))
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            return FetchResult(path, False, status_code=status, error=str(exc))
        except Exception as exc:
            return FetchResult(path, False, error=str(exc))

    def paginated(self, path: str, **params: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 0
        while True:
            p = dict(params)
            p["page"] = page
            p["page_size"] = 200
            r = self._session().get(
                f"{GW2_API}{path}", params=p, timeout=(5, 15)
            )
            if r.status_code == 404:
                break
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            rows.extend(data)
            total_pages = int(r.headers.get("X-Page-Total", 1))
            if page >= total_pages - 1:
                break
            page += 1
        return rows

    def paginated_until_known(
        self, path: str, known_ids: set[int], **params: Any
    ) -> list[dict[str, Any]]:
        """Fetch newest-first pages and stop once an already-stored transaction is seen."""
        if not known_ids:
            return self.paginated(path, **params)

        rows: list[dict[str, Any]] = []
        page = 0
        while True:
            p = dict(params)
            p["page"] = page
            p["page_size"] = 200
            r = self._session().get(
                f"{GW2_API}{path}", params=p, timeout=(5, 15)
            )
            if r.status_code == 404:
                break
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            rows.extend(data)
            if any(
                isinstance(row, dict)
                and row.get("id") is not None
                and int(row["id"]) in known_ids
                for row in data
            ):
                break
            total_pages = int(r.headers.get("X-Page-Total", 1))
            if page >= total_pages - 1:
                break
            page += 1
        return rows

    def prices(self, item_ids: Iterable[int]) -> dict[int, dict[str, Any]]:
        unique = sorted({int(i) for i in item_ids if i})
        chunks = [unique[i:i + BATCH_SIZE] for i in range(0, len(unique), BATCH_SIZE)]
        if not chunks:
            return {}

        def fetch_chunk(chunk: list[int]) -> list[dict[str, Any]]:
            try:
                return self.get("/commerce/prices", ids=",".join(map(str, chunk)))
            except Exception:
                return []

        out: dict[int, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=min(8, len(chunks))) as pool:
            futures = [pool.submit(fetch_chunk, chunk) for chunk in chunks]
            for future in as_completed(futures):
                for row in future.result():
                    out[int(row["id"])] = row
        return out


# The GitHub Action runs once per day. Slow-changing endpoints are refreshed
# conditionally inside that same daily run; there are no extra weekly/monthly jobs.
MAX_API_WORKERS = 8
UNLOCK_REFRESH_DAYS = 7
CHARACTER_DETAIL_REFRESH_DAYS = 30
CURRENCY_METADATA_REFRESH_DAYS = 30
TOKENINFO_REFRESH_DAYS = 3650

DAILY_STATE_ENDPOINTS = [
    "/account/dailycrafting",
    "/account/dungeons",
    "/account/mapchests",
    "/account/progression",
    "/account/raids",
    "/account/wizardsvault/daily",
    "/account/wizardsvault/special",
    "/account/wizardsvault/weekly",
    "/account/worldbosses",
    "/account/wvw",
    "/pvp/standings",
]

WEEKLY_STATE_ENDPOINTS = [
    "/account/buildstorage",
    "/account/homestead/decorations",
    "/account/homestead/glyphs",
    "/account/masteries",
    "/account/wizardsvault/listings",
]


UNLOCK_ENDPOINTS = {
    "dyes": "/account/dyes",
    "skins": "/account/skins",
    "titles": "/account/titles",
    "minis": "/account/minis",
    "outfits": "/account/outfits",
    "gliders": "/account/gliders",
    "mount_skins": "/account/mounts/skins",
    "mount_types": "/account/mounts/types",
    "recipes": "/account/recipes",
    "emotes": "/account/emotes",
    "finishers": "/account/finishers",
    "jadebots": "/account/jadebots",
    "mailcarriers": "/account/mailcarriers",
    "novelties": "/account/novelties",
    "skiffs": "/account/skiffs",
    "home_cats": "/account/home/cats",
    "home_nodes": "/account/home/nodes",
    "pvp_heroes": "/account/pvp/heroes",
}


def store_state(engine: Engine, key: str, value: Any, observed_at: datetime, keep_history: bool = True) -> bool:
    """Update current JSON state; insert one historical row only for a new distinct state."""
    raw = canonical_json(value)
    digest = state_hash(value)
    with engine.begin() as conn:
        existing = conn.execute(text("SELECT state_hash FROM current_state WHERE state_key=:k"), {"k": key}).scalar()
        changed = existing != digest
        conn.execute(text("""
            INSERT INTO current_state(state_key, state_hash, observed_at, raw_json)
            VALUES (:k, :h, :t, CAST(:raw AS JSONB))
            ON CONFLICT (state_key) DO UPDATE SET
                state_hash=EXCLUDED.state_hash,
                observed_at=EXCLUDED.observed_at,
                raw_json=EXCLUDED.raw_json
        """), {"k": key, "h": digest, "t": observed_at, "raw": raw})
        if keep_history and changed:
            conn.execute(text("""
                INSERT INTO state_changes(state_key, state_hash, first_seen, raw_json)
                VALUES (:k, :h, :t, CAST(:raw AS JSONB))
                ON CONFLICT (state_key, state_hash) DO NOTHING
            """), {"k": key, "h": digest, "t": observed_at, "raw": raw})
    return changed


def record_error(engine: Engine, snapshot_at: datetime, result: FetchResult) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO endpoint_errors(snapshot_at, endpoint, status_code, error)
            VALUES (:t, :e, :s, :m)
        """), {"t": snapshot_at, "e": result.endpoint, "s": result.status_code, "m": result.error or "unknown error"})


def load_state_observed_at(engine: Engine) -> dict[str, datetime]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT state_key, observed_at FROM current_state"))
        return {str(r.state_key): r.observed_at for r in rows}


def refresh_due(
    state_seen: dict[str, datetime], key: str, now: datetime, every_days: int
) -> bool:
    seen = state_seen.get(key)
    if seen is None:
        return True
    if seen.tzinfo is None:
        seen = seen.replace(tzinfo=timezone.utc)
    return now - seen >= timedelta(days=every_days)


def load_unlock_counts(engine: Engine) -> dict[str, int]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT unlock_type, COUNT(*) AS n
            FROM unlocks
            GROUP BY unlock_type
        """))
        return {str(r.unlock_type): int(r.n) for r in rows}


def currency_metadata_due(engine: Engine, now: datetime, every_days: int) -> bool:
    with engine.connect() as conn:
        last = conn.execute(text("SELECT MAX(updated_at) FROM currency_metadata")).scalar()
    if last is None:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return now - last >= timedelta(days=every_days)


def unlock_id(value: Any) -> str | None:
    if isinstance(value, (int, str)):
        return str(value)
    if isinstance(value, dict):
        for k in ("id", "name"):
            if k in value:
                return str(value[k])
    return None


def upsert_unlocks(engine: Engine, unlock_type: str, values: Any, observed_at: datetime) -> int:
    if not isinstance(values, list):
        return 0
    rows = []
    for value in values:
        uid = unlock_id(value)
        if uid is not None:
            rows.append({"ut": unlock_type, "uid": uid, "t": observed_at})
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO unlocks(unlock_type, unlock_id, first_seen, last_seen)
            VALUES (:ut, :uid, :t, :t)
            ON CONFLICT (unlock_type, unlock_id) DO UPDATE SET last_seen=EXCLUDED.last_seen
        """), rows)
    return len(rows)


def extract_bag_items(characters: list[dict[str, Any]]) -> list[dict[str, int]]:
    items: list[dict[str, int]] = []
    for char in characters:
        for bag in char.get("bags") or []:
            if not isinstance(bag, dict):
                continue
            for slot in bag.get("inventory") or []:
                if isinstance(slot, dict) and slot.get("id") and slot.get("count"):
                    items.append({"id": int(slot["id"]), "count": int(slot["count"])})
    return items


def simple_item_stacks(value: Any) -> list[dict[str, int]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, int]] = []
    for row in value:
        if isinstance(row, dict) and row.get("id") and row.get("count"):
            items.append({"id": int(row["id"]), "count": int(row["count"])})
    return items


def value_items(items: list[dict[str, int]], prices: dict[int, dict[str, Any]]) -> tuple[int, int]:
    market = 0
    liquidation = 0
    for row in items:
        p = prices.get(int(row["id"]))
        if not p:
            continue
        qty = int(row["count"])
        sell_price = int((p.get("sells") or {}).get("unit_price") or 0)
        buy_price = int((p.get("buys") or {}).get("unit_price") or 0)
        market += sell_price * qty
        liquidation += int(buy_price * qty * (1 - TP_TAX))
    return market, liquidation


def strip_character_volatile(character: dict[str, Any]) -> dict[str, Any]:
    out = dict(character)
    for key in ("age", "deaths"):
        out.pop(key, None)
    return out


def iso_or_none(value: Any) -> Any:
    return value or None


def upsert_currency_metadata(engine: Engine, api: GW2Api, observed_at: datetime) -> None:
    try:
        rows = api.get("/currencies", ids="all")
    except Exception:
        return
    payload = []
    for row in rows:
        payload.append({
            "id": int(row["id"]),
            "name": row.get("name") or f"Currency {row['id']}",
            "description": row.get("description"),
            "icon": row.get("icon"),
            "order": row.get("order"),
            "t": observed_at,
        })
    if payload:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO currency_metadata(currency_id, name, description, icon, display_order, updated_at)
                VALUES (:id, :name, :description, :icon, :order, :t)
                ON CONFLICT (currency_id) DO UPDATE SET
                    name=EXCLUDED.name,
                    description=EXCLUDED.description,
                    icon=EXCLUDED.icon,
                    display_order=EXCLUDED.display_order,
                    updated_at=EXCLUDED.updated_at
            """), payload)


def store_wallet(engine: Engine, observed_at: datetime, wallet: Any) -> int:
    if not isinstance(wallet, list):
        return 0
    rows = [{"t": observed_at, "id": int(r["id"]), "value": int(r.get("value") or 0)} for r in wallet if isinstance(r, dict) and r.get("id") is not None]
    if rows:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO wallet_snapshots(snapshot_at, currency_id, value)
                VALUES (:t, :id, :value)
                ON CONFLICT (snapshot_at, currency_id) DO UPDATE SET value=EXCLUDED.value
            """), rows)
    return len(rows)


def store_masteries(engine: Engine, observed_at: datetime, data: Any) -> tuple[int, int]:
    earned_total = spent_total = 0
    rows = []
    totals = data.get("totals", []) if isinstance(data, dict) else []
    for values in totals:
        if not isinstance(values, dict) or not values.get("region"):
            continue
        region = str(values["region"])
        earned = int(values.get("earned") or 0)
        spent = int(values.get("spent") or 0)
        earned_total += earned
        spent_total += spent
        rows.append({"t": observed_at, "region": region, "earned": earned, "spent": spent})
    if rows:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO mastery_snapshots(snapshot_at, region, earned, spent)
                VALUES (:t, :region, :earned, :spent)
                ON CONFLICT (snapshot_at, region) DO UPDATE SET earned=EXCLUDED.earned, spent=EXCLUDED.spent
            """), rows)
    return earned_total, spent_total


def get_achievement_metadata(engine: Engine, api: GW2Api, achievement_ids: Iterable[int], observed_at: datetime) -> dict[int, dict[str, Any]]:
    ids = sorted({int(i) for i in achievement_ids if i is not None})
    if not ids:
        return {}
    existing: dict[int, dict[str, Any]] = {}
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT achievement_id, raw_json FROM achievement_metadata"))
        wanted = set(ids)
        for r in rows:
            if int(r.achievement_id) not in wanted:
                continue
            raw = r.raw_json
            existing[int(r.achievement_id)] = raw if isinstance(raw, dict) else json.loads(raw)
    missing = [i for i in ids if i not in existing]
    fetched: dict[int, dict[str, Any]] = {}
    for i in range(0, len(missing), BATCH_SIZE):
        chunk = missing[i:i + BATCH_SIZE]
        try:
            rows = api.get("/achievements", ids=",".join(map(str, chunk)))
        except Exception:
            continue
        payload = []
        for row in rows:
            aid = int(row["id"])
            fetched[aid] = row
            payload.append({
                "id": aid,
                "name": row.get("name"),
                "flags": canonical_json(row.get("flags") or []),
                "tiers": canonical_json(row.get("tiers") or []),
                "cap": row.get("point_cap"),
                "raw": canonical_json(row),
                "t": observed_at,
            })
        if payload:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO achievement_metadata(achievement_id, name, flags, tiers, point_cap, raw_json, updated_at)
                    VALUES (:id, :name, CAST(:flags AS JSONB), CAST(:tiers AS JSONB), :cap, CAST(:raw AS JSONB), :t)
                    ON CONFLICT (achievement_id) DO UPDATE SET
                        name=EXCLUDED.name,
                        flags=EXCLUDED.flags,
                        tiers=EXCLUDED.tiers,
                        point_cap=EXCLUDED.point_cap,
                        raw_json=EXCLUDED.raw_json,
                        updated_at=EXCLUDED.updated_at
                """), payload)
    existing.update(fetched)
    return existing


def calculate_achievement_points(account_progress: Any, metadata: dict[int, dict[str, Any]], daily_ap: int | None, monthly_ap: int | None) -> int:
    """Best-effort total AP using achievement tiers plus legacy daily/monthly AP counters."""
    total = int(daily_ap or 0) + int(monthly_ap or 0)
    if not isinstance(account_progress, list):
        return total
    for progress in account_progress:
        if not isinstance(progress, dict) or progress.get("id") is None:
            continue
        meta = metadata.get(int(progress["id"]))
        if not meta:
            continue
        flags = set(meta.get("flags") or [])
        if flags.intersection({"Daily", "Weekly", "Monthly"}):
            continue
        tiers = meta.get("tiers") or []
        if not tiers:
            continue
        current = int(progress.get("current") or 0)
        if progress.get("done") and progress.get("max") is not None:
            current = max(current, int(progress.get("max") or 0))
        partial = sum(int(t.get("points") or 0) for t in tiers if current >= int(t.get("count") or 0))
        if "Repeatable" in flags:
            cycle_points = sum(int(t.get("points") or 0) for t in tiers)
            earned = int(progress.get("repeated") or 0) * cycle_points + partial
            if meta.get("point_cap") is not None:
                earned = min(earned, int(meta["point_cap"]))
            total += earned
        else:
            total += partial
    return total


def store_characters(engine: Engine, observed_at: datetime, characters: Any) -> tuple[int, int, int]:
    if not isinstance(characters, list):
        return 0, 0, 0
    rows = []
    total_age = 0
    total_deaths = 0
    for c in characters:
        if not isinstance(c, dict) or not c.get("name"):
            continue
        age = int(c.get("age") or 0)
        deaths = int(c.get("deaths") or 0)
        total_age += age
        total_deaths += deaths
        rows.append({
            "t": observed_at,
            "name": c["name"],
            "race": c.get("race"),
            "profession": c.get("profession"),
            "level": c.get("level"),
            "age": age,
            "deaths": deaths,
            "created": iso_or_none(c.get("created")),
            "guild": c.get("guild"),
        })
        store_state(engine, f"character:{c['name']}:state", strip_character_volatile(c), observed_at, keep_history=True)
    if rows:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO character_snapshots(snapshot_at, character_name, race, profession, level, age_seconds, deaths, created_at, guild_id)
                VALUES (:t, :name, :race, :profession, :level, :age, :deaths, :created, :guild)
                ON CONFLICT (snapshot_at, character_name) DO UPDATE SET
                    race=EXCLUDED.race,
                    profession=EXCLUDED.profession,
                    level=EXCLUDED.level,
                    age_seconds=EXCLUDED.age_seconds,
                    deaths=EXCLUDED.deaths,
                    created_at=EXCLUDED.created_at,
                    guild_id=EXCLUDED.guild_id
            """), rows)
    return len(rows), total_age, total_deaths


def store_pvp_matches(engine: Engine, api: GW2Api, observed_at: datetime) -> int:
    try:
        ids = api.get("/pvp/games")
    except Exception:
        return 0
    if not ids:
        return 0
    try:
        matches = api.get("/pvp/games", ids=",".join(map(str, ids)))
    except Exception:
        return 0
    rows = []
    for m in matches:
        rows.append({
            "id": str(m.get("id")),
            "seen": observed_at,
            "started": m.get("started"),
            "ended": m.get("ended"),
            "map_id": m.get("map_id"),
            "result": m.get("result"),
            "team": m.get("team"),
            "rating_type": m.get("rating_type"),
            "raw": canonical_json(m),
        })
    if rows:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO pvp_matches(match_id, first_seen, started_at, ended_at, map_id, result, team, rating_type, raw_json)
                VALUES (:id, :seen, :started, :ended, :map_id, :result, :team, :rating_type, CAST(:raw AS JSONB))
                ON CONFLICT (match_id) DO NOTHING
            """), rows)
    return len(rows)


def store_tp_transactions(
    engine: Engine, api: GW2Api, observed_at: datetime
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    """Fetch current TP state and only the new portion of completed history."""
    known: dict[str, set[int]] = {}
    with engine.connect() as conn:
        for side in ("buys", "sells"):
            rows = conn.execute(text("""
                SELECT transaction_id FROM tp_transactions
                WHERE transaction_set='history' AND side=:side
            """), {"side": side})
            known[side] = {int(r.transaction_id) for r in rows}

    jobs: dict[tuple[str, str], Any] = {}
    result: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        for side in ("buys", "sells"):
            current_path = f"/commerce/transactions/current/{side}"
            history_path = f"/commerce/transactions/history/{side}"
            jobs[("current", side)] = pool.submit(api.paginated, current_path)
            jobs[("history", side)] = pool.submit(
                api.paginated_until_known, history_path, known[side]
            )

        for (tx_set, side), future in jobs.items():
            try:
                result[f"{tx_set}_{side}"] = future.result()
            except Exception:
                result[f"{tx_set}_{side}"] = []

    new_history = 0
    for tx_set in ("current", "history"):
        for side in ("buys", "sells"):
            data = result[f"{tx_set}_{side}"]
            if tx_set == "history":
                new_history += sum(
                    1 for r in data
                    if r.get("id") is not None and int(r["id"]) not in known[side]
                )

            rows = [{
                "id": int(r["id"]),
                "side": side,
                "set": tx_set,
                "item": int(r["item_id"]),
                "price": int(r["price"]),
                "qty": int(r["quantity"]),
                "created": r.get("created"),
                "purchased": r.get("purchased"),
                "raw": canonical_json(r),
            } for r in data]

            with engine.begin() as conn:
                if tx_set == "current":
                    conn.execute(text("""
                        DELETE FROM tp_transactions
                        WHERE transaction_set='current' AND side=:side
                    """), {"side": side})
                if rows:
                    conn.execute(text("""
                        INSERT INTO tp_transactions(
                            transaction_id, side, transaction_set, item_id, price, quantity,
                            created, purchased, raw_json
                        ) VALUES (
                            :id, :side, :set, :item, :price, :qty, :created, :purchased,
                            CAST(:raw AS JSONB)
                        )
                        ON CONFLICT (transaction_id, side, transaction_set) DO UPDATE SET
                            price=EXCLUDED.price,
                            quantity=EXCLUDED.quantity,
                            created=EXCLUDED.created,
                            purchased=EXCLUDED.purchased,
                            raw_json=EXCLUDED.raw_json
                    """), rows)

    return result, new_history


def get_flip_summary(engine: Engine) -> tuple[int, int]:
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT COUNT(*) AS n, COALESCE(SUM(profit), 0) AS profit
            FROM matched_flips
        """)).one()
    return int(row.n), int(row.profit)


def fifo_match(buys: list[dict[str, Any]], sells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buy_map: dict[int, list[dict[str, Any]]] = defaultdict(list)
    sell_map: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for b in buys:
        buy_map[int(b["item_id"])].append(b)
    for s in sells:
        sell_map[int(s["item_id"])].append(s)

    flips: list[dict[str, Any]] = []
    for item_id, item_sells in sell_map.items():
        queue = deque({
            "remaining": int(b["quantity"]),
            "price": int(b["price"]),
            "date": b.get("purchased"),
        } for b in sorted(buy_map.get(item_id, []), key=lambda x: x.get("purchased") or ""))

        for sell in sorted(item_sells, key=lambda x: x.get("purchased") or ""):
            remaining = int(sell["quantity"])
            sell_price = int(sell["price"])
            while remaining > 0 and queue:
                buy = queue[0]
                qty = min(remaining, buy["remaining"])
                profit = int(sell_price * qty * (1 - TP_TAX)) - buy["price"] * qty
                flips.append({
                    "item": item_id,
                    "qty": qty,
                    "buy": buy["price"],
                    "sell": sell_price,
                    "profit": profit,
                    "buy_date": buy["date"],
                    "sell_date": sell.get("purchased"),
                })
                buy["remaining"] -= qty
                remaining -= qty
                if buy["remaining"] <= 0:
                    queue.popleft()
    return flips


def refresh_matched_flips(engine: Engine) -> tuple[int, int]:
    with engine.begin() as conn:
        buys = [dict(r._mapping) for r in conn.execute(text("""
            SELECT item_id, price, quantity, purchased FROM tp_transactions
            WHERE transaction_set='history' AND side='buys'
        """))]
        sells = [dict(r._mapping) for r in conn.execute(text("""
            SELECT item_id, price, quantity, purchased FROM tp_transactions
            WHERE transaction_set='history' AND side='sells'
        """))]
    flips = fifo_match(buys, sells)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM matched_flips"))
        if flips:
            conn.execute(text("""
                INSERT INTO matched_flips(item_id, quantity, buy_price, sell_price, profit, buy_date, sell_date)
                VALUES (:item, :qty, :buy, :sell, :profit, :buy_date, :sell_date)
            """), flips)
    return len(flips), sum(int(f["profit"]) for f in flips)


def pvp_summary(data: Any) -> tuple[int | None, int | None, int, int]:
    if not isinstance(data, dict):
        return None, None, 0, 0
    agg = data.get("aggregate") or {}
    return (
        data.get("pvp_rank"),
        data.get("pvp_rank_points"),
        int(agg.get("wins") or 0),
        int(agg.get("losses") or 0),
    )


def wvw_rank_from_account(account: Any, wvw_state: Any) -> int | None:
    if isinstance(account, dict):
        if account.get("wvw_rank") is not None:
            return int(account["wvw_rank"])
        if isinstance(account.get("wvw"), dict) and account["wvw"].get("rank") is not None:
            return int(account["wvw"]["rank"])
    if isinstance(wvw_state, dict) and wvw_state.get("rank") is not None:
        return int(wvw_state["rank"])
    return None


def collect_all(engine: Engine, api: GW2Api) -> dict[str, Any]:
    ensure_schema(engine)
    started = observed_at = utcnow()
    perf_started = time.perf_counter()
    timings: dict[str, float] = {}

    with engine.begin() as conn:
        run_id = conn.execute(text("""
            INSERT INTO job_runs(started_at, status) VALUES (:t, 'running') RETURNING id
        """), {"t": started}).scalar_one()

    ok = 0
    failed = 0
    cache: dict[str, Any] = {}
    state_seen = load_state_observed_at(engine)

    def handle_result(
        path: str,
        res: FetchResult,
        *,
        store: bool,
        keep_history: bool,
        state_key: str | None = None,
    ) -> Any:
        nonlocal ok, failed
        if res.ok:
            ok += 1
            cache[path] = res.data
            if store:
                store_state(
                    engine, state_key or path, res.data, observed_at, keep_history=keep_history
                )
            return res.data
        failed += 1
        record_error(engine, observed_at, res)
        return None

    def fetch(
        path: str,
        *,
        store: bool = False,
        keep_history: bool = True,
        **params: Any,
    ) -> Any:
        return handle_result(
            path,
            api.try_get(path, **params),
            store=store,
            keep_history=keep_history,
            state_key=path,
        )

    def fetch_many(specs: list[dict[str, Any]]) -> None:
        if not specs:
            return
        with ThreadPoolExecutor(max_workers=min(MAX_API_WORKERS, len(specs))) as pool:
            futures = {
                pool.submit(api.try_get, spec["path"], **spec.get("params", {})): spec
                for spec in specs
            }
            for future in as_completed(futures):
                spec = futures[future]
                try:
                    res = future.result()
                except Exception as exc:
                    res = FetchResult(spec["path"], False, error=str(exc))
                handle_result(
                    spec["path"],
                    res,
                    store=spec.get("store", False),
                    keep_history=spec.get("keep_history", True),
                    state_key=spec.get("state_key") or spec["path"],
                )

    try:
        # 1) Daily dynamic account data. These requests are independent, so fetch them in parallel.
        t = time.perf_counter()
        core_specs: list[dict[str, Any]] = [
            {"path": "/account", "store": True, "keep_history": False},
            {"path": "/account/wallet", "store": True, "keep_history": False},
            {"path": "/account/mastery/points", "store": True, "keep_history": False},
            {"path": "/characters", "params": {"ids": "all"}},
            {"path": "/pvp/stats", "store": True, "keep_history": False},
            {"path": "/account/luck", "store": True, "keep_history": False},
            {"path": "/account/bank", "store": True, "keep_history": False},
            {"path": "/account/materials", "store": True, "keep_history": False},
            {"path": "/account/inventory", "store": True, "keep_history": False},
            {"path": "/account/legendaryarmory", "store": True, "keep_history": True},
            {"path": "/account/achievements", "store": True, "keep_history": False},
            {"path": "/commerce/delivery", "store": True, "keep_history": False},
        ]
        if refresh_due(state_seen, "/tokeninfo", observed_at, TOKENINFO_REFRESH_DAYS):
            core_specs.append({"path": "/tokeninfo", "store": True, "keep_history": False})

        existing_paths = {spec["path"] for spec in core_specs}
        for path in DAILY_STATE_ENDPOINTS:
            if path not in existing_paths:
                core_specs.append({"path": path, "store": True, "keep_history": True})
                existing_paths.add(path)
        for path in WEEKLY_STATE_ENDPOINTS:
            if path not in existing_paths and refresh_due(state_seen, path, observed_at, 7):
                core_specs.append({"path": path, "store": True, "keep_history": True})
                existing_paths.add(path)

        unlock_due: dict[str, bool] = {}
        for kind, path in UNLOCK_ENDPOINTS.items():
            due = refresh_due(state_seen, path, observed_at, UNLOCK_REFRESH_DAYS)
            unlock_due[kind] = due
            if due and path not in existing_paths:
                core_specs.append({"path": path, "store": True, "keep_history": False})
                existing_paths.add(path)

        fetch_many(core_specs)
        timings["parallel_account_api"] = time.perf_counter() - t

        account = cache.get("/account") or {}
        wallet = cache.get("/account/wallet") or []
        masteries = cache.get("/account/mastery/points") or {}
        characters = cache.get("/characters") or []
        pvp_stats = cache.get("/pvp/stats") or {}
        luck_data = cache.get("/account/luck")
        bank = cache.get("/account/bank") or []
        materials = cache.get("/account/materials") or []
        shared = cache.get("/account/inventory") or []
        legendary = cache.get("/account/legendaryarmory") or []
        achievements = cache.get("/account/achievements") or []
        delivery = cache.get("/commerce/delivery") or {}

        # 2) Small normalized daily snapshots.
        t = time.perf_counter()
        unlock_counts = load_unlock_counts(engine)
        for kind, path in UNLOCK_ENDPOINTS.items():
            if unlock_due.get(kind) and path in cache:
                count = upsert_unlocks(engine, kind, cache[path], observed_at)
                if count or isinstance(cache[path], list):
                    unlock_counts[kind] = count

        if currency_metadata_due(engine, observed_at, CURRENCY_METADATA_REFRESH_DAYS):
            upsert_currency_metadata(engine, api, observed_at)

        store_wallet(engine, observed_at, wallet)
        mastery_earned, mastery_spent = store_masteries(engine, observed_at, masteries)
        char_count, total_playtime, total_deaths = store_characters(
            engine, observed_at, characters
        )
        timings["normalized_snapshots"] = time.perf_counter() - t

        # 3) Bulky character sub-state is retained, but only refreshed monthly.
        t = time.perf_counter()
        detail_specs: list[dict[str, Any]] = []
        for char in characters if isinstance(characters, list) else []:
            if not isinstance(char, dict) or not char.get("name"):
                continue
            encoded_name = requests.utils.quote(str(char["name"]), safe="")
            for suffix in ("heropoints", "quests", "sab", "dungeons"):
                path = f"/characters/{encoded_name}/{suffix}"
                state_key = f"character:{char['name']}:{suffix}"
                if refresh_due(
                    state_seen, state_key, observed_at, CHARACTER_DETAIL_REFRESH_DAYS
                ):
                    detail_specs.append({
                        "path": path,
                        "state_key": state_key,
                        "store": True,
                        "keep_history": True,
                    })
        fetch_many(detail_specs)
        timings["character_detail_api"] = time.perf_counter() - t

        # 4) Event streams. PvP is tiny; TP history is incremental after the first run.
        t = time.perf_counter()
        pvp_matches_seen = store_pvp_matches(engine, api, observed_at)
        tp, new_history_transactions = store_tp_transactions(engine, api, observed_at)
        if new_history_transactions:
            flip_rows, realised_profit = refresh_matched_flips(engine)
        else:
            flip_rows, realised_profit = get_flip_summary(engine)
        timings["events_and_trading"] = time.perf_counter() - t

        # 5) Daily wealth valuation. Price batches are fetched concurrently.
        t = time.perf_counter()
        bank_items = simple_item_stacks(bank)
        material_items = simple_item_stacks(materials)
        shared_items = simple_item_stacks(shared)
        char_items = extract_bag_items(characters)
        delivery_items = simple_item_stacks(
            delivery.get("items") if isinstance(delivery, dict) else []
        )
        all_items = bank_items + material_items + shared_items + char_items + delivery_items
        prices = api.prices([r["id"] for r in all_items])

        bank_market, bank_liq = value_items(bank_items, prices)
        materials_market, materials_liq = value_items(material_items, prices)
        shared_market, shared_liq = value_items(shared_items, prices)
        char_market, char_liq = value_items(char_items, prices)
        delivery_market, delivery_liq = value_items(delivery_items, prices)
        account_item_market = (
            bank_market + materials_market + shared_market + char_market + delivery_market
        )
        account_item_liq = (
            bank_liq + materials_liq + shared_liq + char_liq + delivery_liq
        )
        timings["price_valuation"] = time.perf_counter() - t

        wallet_map = {
            int(r["id"]): int(r.get("value") or 0)
            for r in wallet
            if isinstance(r, dict) and r.get("id") is not None
        }
        liquid_gold = wallet_map.get(1, 0)
        delivery_coins = int(delivery.get("coins") or 0) if isinstance(delivery, dict) else 0
        buy_order_committed = sum(
            int(r["price"]) * int(r["quantity"])
            for r in tp.get("current_buys", [])
        )
        sell_order_net = sum(
            int(int(r["price"]) * int(r["quantity"]) * (1 - TP_TAX))
            for r in tp.get("current_sells", [])
        )
        liquid_net_worth = (
            liquid_gold
            + delivery_coins
            + buy_order_committed
            + sell_order_net
            + account_item_liq
        )

        # 6) Derived headline metrics. Achievement metadata is cached permanently.
        t = time.perf_counter()
        pvp_rank, pvp_points, pvp_wins, pvp_losses = pvp_summary(pvp_stats)
        wvw_state = cache.get("/account/wvw")
        wvw_rank = wvw_rank_from_account(account, wvw_state)
        daily_ap = account.get("daily_ap") if isinstance(account, dict) else None
        monthly_ap = account.get("monthly_ap") if isinstance(account, dict) else None
        achievement_meta = get_achievement_metadata(
            engine,
            api,
            [
                a.get("id")
                for a in achievements
                if isinstance(a, dict) and a.get("id") is not None
            ] if isinstance(achievements, list) else [],
            observed_at,
        )
        achievement_points_estimate = calculate_achievement_points(
            achievements, achievement_meta, daily_ap, monthly_ap
        )
        fractal_level = account.get("fractal_level") if isinstance(account, dict) else None
        luck = None
        if isinstance(luck_data, list) and luck_data and isinstance(luck_data[0], dict):
            luck = luck_data[0].get("value")
        elif isinstance(luck_data, dict):
            luck = luck_data.get("value") or luck_data.get("luck")
        elif isinstance(luck_data, int):
            luck = luck_data

        legendary_unique = len(legendary) if isinstance(legendary, list) else 0
        legendary_count = (
            sum(int(x.get("count") or 0) for x in legendary if isinstance(x, dict))
            if isinstance(legendary, list)
            else 0
        )
        timings["derived_metrics"] = time.perf_counter() - t

        row = {
            "snapshot_at": observed_at,
            "account_id": account.get("id") if isinstance(account, dict) else None,
            "account_name": account.get("name") if isinstance(account, dict) else None,
            "account_age_seconds": account.get("age") if isinstance(account, dict) else None,
            "world_id": account.get("world") if isinstance(account, dict) else None,
            "commander": account.get("commander") if isinstance(account, dict) else None,
            "fractal_level": fractal_level,
            "wvw_rank": wvw_rank,
            "daily_ap": daily_ap,
            "monthly_ap": monthly_ap,
            "achievement_points_estimate": achievement_points_estimate,
            "liquid_gold": liquid_gold,
            "account_item_market_value": account_item_market,
            "account_item_liquidation_value": account_item_liq,
            "bank_market_value": bank_market,
            "materials_market_value": materials_market,
            "shared_inventory_market_value": shared_market,
            "character_inventory_market_value": char_market,
            "delivery_market_value": delivery_market,
            "delivery_coins": delivery_coins,
            "buy_order_committed": buy_order_committed,
            "sell_order_net_value": sell_order_net,
            "liquid_net_worth": liquid_net_worth,
            "total_playtime_seconds": total_playtime,
            "total_deaths": total_deaths,
            "character_count": char_count,
            "mastery_earned": mastery_earned,
            "mastery_spent": mastery_spent,
            "mastery_unspent": mastery_earned - mastery_spent,
            "pvp_rank": pvp_rank,
            "pvp_rank_points": pvp_points,
            "pvp_wins": pvp_wins,
            "pvp_losses": pvp_losses,
            "luck": luck,
            "skins_count": unlock_counts.get("skins", 0),
            "dyes_count": unlock_counts.get("dyes", 0),
            "titles_count": unlock_counts.get("titles", 0),
            "minis_count": unlock_counts.get("minis", 0),
            "outfits_count": unlock_counts.get("outfits", 0),
            "gliders_count": unlock_counts.get("gliders", 0),
            "mount_skins_count": unlock_counts.get("mount_skins", 0),
            "recipes_count": unlock_counts.get("recipes", 0),
            "legendary_unique": legendary_unique,
            "legendary_count": legendary_count,
            "realised_flip_profit": realised_profit,
            "matched_flip_rows": flip_rows,
            "endpoints_ok": ok,
            "endpoints_failed": failed,
        }

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO account_snapshots(
                    snapshot_at, account_id, account_name, account_age_seconds, world_id, commander,
                    fractal_level, wvw_rank, daily_ap, monthly_ap, achievement_points_estimate, liquid_gold,
                    account_item_market_value, account_item_liquidation_value,
                    bank_market_value, materials_market_value, shared_inventory_market_value,
                    character_inventory_market_value, delivery_market_value, delivery_coins,
                    buy_order_committed, sell_order_net_value, liquid_net_worth,
                    total_playtime_seconds, total_deaths, character_count,
                    mastery_earned, mastery_spent, mastery_unspent,
                    pvp_rank, pvp_rank_points, pvp_wins, pvp_losses, luck,
                    skins_count, dyes_count, titles_count, minis_count, outfits_count, gliders_count,
                    mount_skins_count, recipes_count, legendary_unique, legendary_count,
                    realised_flip_profit, matched_flip_rows, endpoints_ok, endpoints_failed
                ) VALUES (
                    :snapshot_at, :account_id, :account_name, :account_age_seconds, :world_id, :commander,
                    :fractal_level, :wvw_rank, :daily_ap, :monthly_ap, :achievement_points_estimate, :liquid_gold,
                    :account_item_market_value, :account_item_liquidation_value,
                    :bank_market_value, :materials_market_value, :shared_inventory_market_value,
                    :character_inventory_market_value, :delivery_market_value, :delivery_coins,
                    :buy_order_committed, :sell_order_net_value, :liquid_net_worth,
                    :total_playtime_seconds, :total_deaths, :character_count,
                    :mastery_earned, :mastery_spent, :mastery_unspent,
                    :pvp_rank, :pvp_rank_points, :pvp_wins, :pvp_losses, :luck,
                    :skins_count, :dyes_count, :titles_count, :minis_count, :outfits_count, :gliders_count,
                    :mount_skins_count, :recipes_count, :legendary_unique, :legendary_count,
                    :realised_flip_profit, :matched_flip_rows, :endpoints_ok, :endpoints_failed
                )
            """), row)

        finished = utcnow()
        runtime_seconds = time.perf_counter() - perf_started
        timings["total"] = runtime_seconds
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE job_runs SET finished_at=:f, status='success', endpoints_ok=:ok,
                    endpoints_failed=:failed, runtime_seconds=:runtime, message=:m WHERE id=:id
            """), {
                "f": finished,
                "ok": ok,
                "failed": failed,
                "runtime": runtime_seconds,
                "m": (
                    f"Daily snapshot; {pvp_matches_seen} PvP matches observed; "
                    f"{new_history_transactions} new TP history transactions; "
                    f"{flip_rows} FIFO flip rows."
                ),
                "id": run_id,
            })
        row["_runtime_seconds"] = runtime_seconds
        row["_timings"] = timings
        row["_new_tp_history"] = new_history_transactions
        return row

    except Exception as exc:
        finished = utcnow()
        runtime_seconds = time.perf_counter() - perf_started
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE job_runs SET finished_at=:f, status='failed', endpoints_ok=:ok,
                    endpoints_failed=:failed, runtime_seconds=:runtime, message=:m WHERE id=:id
            """), {
                "f": finished,
                "ok": ok,
                "failed": failed,
                "runtime": runtime_seconds,
                "m": str(exc)[:4000],
                "id": run_id,
            })
        raise

