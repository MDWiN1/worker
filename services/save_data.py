from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from models.stations import Station
from models.arg_table import ObservationARG
from models.aws_table import ObservationAWS
from models.aaws_table import ObservationAAWS
from models.latest_data import StationLatest


# def upsert_station(db: Session, station_data: dict) -> Station:
#     station = db.get(Station, station_data["id_station"])
#
#     if station:
#         station.tipe_station = station_data["tipe_station"]
#         station.name_station = station_data["name_station"]
#         station.latitude = station_data["latitude"]
#         station.longitude = station_data["longitude"]
#         station.elevasi = station_data["elevasi"]
#         station.nama_kota = station_data["nama_kota"]
#     else:
#         station = Station(**station_data)
#         db.add(station)
#
#     return station

def get_station_from_master(db: Session, station_id: str) -> Station | None:
    return db.get(Station, station_id)


def insert_arg_observation(db: Session, obs_data: dict) -> bool:
    try:
        db.add(ObservationARG(**obs_data))
        db.flush()
        return True
    except IntegrityError:
        db.rollback()
        return False


def insert_aws_observation(db: Session, obs_data: dict) -> bool:
    try:
        db.add(ObservationAWS(**obs_data))
        db.flush()
        return True
    except IntegrityError:
        db.rollback()
        return False


def insert_aaws_observation(db: Session, obs_data: dict) -> bool:
    try:
        db.add(ObservationAAWS(**obs_data))
        db.flush()
        return True
    except IntegrityError:
        db.rollback()
        return False


def detect_interval_label(last_observed_at, current_observed_at) -> str | None:
    if not last_observed_at or not current_observed_at:
        return None

    delta_seconds = abs((current_observed_at - last_observed_at).total_seconds())

    if 30 <= delta_seconds <= 90:
        return "1min"
    if 540 <= delta_seconds <= 660:
        return "10min"
    return "unknown"


def upsert_station_latest(db: Session, tipe_station: str, obs_data: dict) -> StationLatest:
    latest = db.get(StationLatest, obs_data["id_station"])

    if latest:
        interval_detected = detect_interval_label(latest.last_observed_at, obs_data["observed_at"])

        latest.tipe_station = tipe_station
        latest.last_observed_at = obs_data["observed_at"]
        latest.status_realtime = "ON"
        latest.interval_detected = interval_detected or latest.interval_detected

        latest.rr = obs_data.get("rr")
        latest.pp_air = obs_data.get("pp_air")
        latest.rh_avg = obs_data.get("rh_avg")
        latest.sr_avg = obs_data.get("sr_avg")
        latest.sr_max = obs_data.get("sr_max")
        latest.wd_avg = obs_data.get("wd_avg")
        latest.ws_avg = obs_data.get("ws_avg")
        latest.ws_max = obs_data.get("ws_max")
        latest.tt_air_avg = obs_data.get("tt_air_avg")
        latest.tt_air_min = obs_data.get("tt_air_min")
        latest.tt_air_max = obs_data.get("tt_air_max")
        latest.ws_50cm = obs_data.get("ws_50cm")
        latest.wl_pan = obs_data.get("wl_pan")
        latest.ev_pan = obs_data.get("ev_pan")
        latest.ws_2m = obs_data.get("ws_2m")
        #latest.latest_payload = obs_data.get("raw_json")
    else:
        latest = StationLatest(
            id_station=obs_data["id_station"],
            tipe_station=tipe_station,
            last_observed_at=obs_data["observed_at"],
            status_realtime="ON",
            rr=obs_data.get("rr"),
            pp_air=obs_data.get("pp_air"),
            rh_avg=obs_data.get("rh_avg"),
            sr_avg=obs_data.get("sr_avg"),
            sr_max=obs_data.get("sr_max"),
            wd_avg=obs_data.get("wd_avg"),
            ws_avg=obs_data.get("ws_avg"),
            ws_max=obs_data.get("ws_max"),
            tt_air_avg=obs_data.get("tt_air_avg"),
            tt_air_min=obs_data.get("tt_air_min"),
            tt_air_max=obs_data.get("tt_air_max"),
            ws_50cm=obs_data.get("ws_50cm"),
            wl_pan=obs_data.get("wl_pan"),
            ev_pan=obs_data.get("ev_pan"),
            ws_2m=obs_data.get("ws_2m"),
            #latest_payload=obs_data.get("raw_json"),
        )
        db.add(latest)

    db.flush()
    return latest


def save_arg_batch(db: Session, items: list[dict]) -> dict:
    inserted = 0
    duplicated = 0
    skipped = 0

    for item in items:
        station_data = item["station"]
        obs_data = item["observation"]

        try:
            station = get_station_from_master(db, station_data["id_station"])
            if not station:
                skipped += 1
                continue

            success = insert_arg_observation(db, obs_data)
            if success:
                inserted += 1
            else:
                duplicated += 1

            upsert_station_latest(db, "arg", obs_data)
            db.commit()

        except Exception:
            db.rollback()
            raise

    return {
        "inserted": inserted,
        "duplicated": duplicated,
        "skipped": skipped,
    }


def save_aws_batch(db: Session, items: list[dict]) -> dict:
    inserted = 0
    duplicated = 0
    skipped = 0

    for item in items:
        station_data = item["station"]
        obs_data = item["observation"]

        try:
            station = get_station_from_master(db, station_data["id_station"])
            if not station:
                skipped += 1
                continue

            success = insert_aws_observation(db, obs_data)
            if success:
                inserted += 1
            else:
                duplicated += 1

            upsert_station_latest(db, "aws", obs_data)
            db.commit()

        except Exception:
            db.rollback()
            raise

    return {
        "inserted": inserted,
        "duplicated": duplicated,
        "skipped": skipped,
    }


def save_aaws_batch(db: Session, items: list[dict]) -> dict:
    inserted = 0
    duplicated = 0
    skipped = 0

    for item in items:
        station_data = item["station"]
        obs_data = item["observation"]

        try:
            station = get_station_from_master(db, station_data["id_station"])
            if not station:
                skipped += 1
                continue

            success = insert_aaws_observation(db, obs_data)
            if success:
                inserted += 1
            else:
                duplicated += 1

            upsert_station_latest(db, "aaws", obs_data)
            db.commit()

        except Exception:
            db.rollback()
            raise

    return {
        "inserted": inserted,
        "duplicated": duplicated,
        "skipped": skipped,
    }