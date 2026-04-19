from datetime import datetime, timedelta, timezone
from db.db_connect import sessionlocal, Base, engine
from services.fetch_api import fetch_data_api
from services.parsing_data import (
    parse_station_data,
    parse_arg_observation,
    parse_aws_observation,
    parse_aaws_observation,
)
from services.save_data import (
    save_arg_batch,
    save_aws_batch,
    save_aaws_batch,
)


def build_time_window(minutes_back: int = 20) -> tuple[datetime, datetime]:
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=minutes_back)
    return start_time, end_time


# def build_items(payload: list[dict], tipe_station: str) -> list[dict]:
#     items: list[dict] = []
#
#     for item in payload:
#         station_data = parse_station_data(item)
#
#         if tipe_station == "arg":
#             obs_data = parse_arg_observation(item)
#         elif tipe_station == "aws":
#             obs_data = parse_aws_observation(item)
#         elif tipe_station == "aaws":
#             obs_data = parse_aaws_observation(item)
#         else:
#             continue
#
#         if not obs_data["observed_at"]:
#             continue
#
#         items.append({
#             "station": station_data,
#             "observation": obs_data,
#         })
# 
#     return items

def build_items(payload: list[dict], tipe_station: str) -> list[dict]:
    items: list[dict] = []

    for idx, item in enumerate(payload):
        try:
            station_data = parse_station_data(item)

            if tipe_station == "arg":
                obs_data = parse_arg_observation(item)
            elif tipe_station == "aws":
                obs_data = parse_aws_observation(item)
            elif tipe_station == "aaws":
                obs_data = parse_aaws_observation(item)
            else:
                print(f"[SKIP] tipe_station tidak dikenal: {tipe_station}")
                continue

            if not isinstance(station_data, dict):
                print(f"[SKIP] station_data bukan dict | idx={idx} | value={station_data}")
                continue

            if not isinstance(obs_data, dict):
                print(f"[SKIP] obs_data bukan dict | idx={idx} | value={obs_data}")
                continue

            observed_at = obs_data.get("observed_at")
            if not observed_at:
                print(
                    f"[SKIP] observed_at kosong | idx={idx} | "
                    f"id_station={station_data.get('id_station')} | "
                    f"tanggal={item.get('tanggal')}"
                )
                continue

            items.append({
                "station": station_data,
                "observation": obs_data,
            })

        except Exception as e:
            print(
                f"[ERROR build_items] tipe={tipe_station} idx={idx} "
                f"id_station={item.get('id_station')} error={repr(e)}"
            )

    return items


def run_ingest(minutes_back: int = 40):
    start_time, end_time = build_time_window(minutes_back=minutes_back)

    print(f"Mulai ingest dari {start_time} sampai {end_time}")

    db = sessionlocal()
    try:
        # ARG
        arg_payload = fetch_data_api("arg", start_time, end_time)
        arg_items = build_items(arg_payload, "arg")
        arg_result = save_arg_batch(db, arg_items)
        print(
            f"ARG -> payload={len(arg_payload)}, valid={len(arg_items)}, "
            f"inserted={arg_result['inserted']}, duplicated={arg_result['duplicated']}"
        )

        # AWS
        aws_payload = fetch_data_api("aws", start_time, end_time)
        aws_items = build_items(aws_payload, "aws")
        aws_result = save_aws_batch(db, aws_items)
        print(
            f"AWS -> payload={len(aws_payload)}, valid={len(aws_items)}, "
            f"inserted={aws_result['inserted']}, duplicated={aws_result['duplicated']}"
        )

        # AAWS
        aaws_payload = fetch_data_api("aaws", start_time, end_time)
        aaws_items = build_items(aaws_payload, "aaws")
        aaws_result = save_aaws_batch(db, aaws_items)
        print(
            f"AAWS -> payload={len(aaws_payload)}, valid={len(aaws_items)}, "
            f"inserted={aaws_result['inserted']}, duplicated={aaws_result['duplicated']}"
        )

        print("Ingest selesai.")

    finally:
        db.close()


if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    run_ingest(minutes_back=40)