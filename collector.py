import os

from tracker_core import GW2Api, collect_all, copper_to_str, make_engine


def main() -> None:
    missing = [name for name in ("GW2_API_KEY", "DATABASE_URL") if not os.getenv(name)]
    if missing:
        raise SystemExit(f"Missing environment variable(s): {', '.join(missing)}")

    engine = make_engine()
    api = GW2Api()
    row = collect_all(engine, api)

    print("GW2 cloud snapshot complete")
    print(f"Account: {row.get('account_name') or 'unknown'}")
    print(f"Liquid gold: {copper_to_str(row.get('liquid_gold'))}")
    print(f"Liquid net worth estimate: {copper_to_str(row.get('liquid_net_worth'))}")
    print(f"Characters: {row.get('character_count')}")
    print(f"Playtime: {(row.get('total_playtime_seconds') or 0) / 3600:,.1f} h")
    print(f"Endpoints: {row.get('endpoints_ok')} ok / {row.get('endpoints_failed')} failed")


if __name__ == "__main__":
    main()
