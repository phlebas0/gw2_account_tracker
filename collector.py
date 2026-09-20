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
    print(f"Runtime: {row.get('_runtime_seconds', 0):.1f} s")
    print(f"New TP history transactions: {row.get('_new_tp_history', 0)}")
    timings = row.get("_timings") or {}
    if timings:
        print("Stage timings:")
        for name, seconds in sorted(timings.items(), key=lambda x: x[1], reverse=True):
            print(f"  {name}: {seconds:.1f} s")


if __name__ == "__main__":
    main()
