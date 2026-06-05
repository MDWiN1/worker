from datetime import datetime, timezone
from sqlalchemy.orm import Session
from models.latest_data import StationLatest
from models.stations import Station

SENSOR_COLS = [
    "rr",
    "pp_air",
    "rh_avg",
    "sr_avg",
    "sr_max",
    "wd_avg",
    "ws_avg",
    "ws_max",
    "tt_air_avg",
    "tt_air_min",
    "tt_air_max",
    "ws_50cm",
    "ws_2m",
]


def has_sensor_data(row: StationLatest) -> bool:
    return any(getattr(row, col, None) is not None for col in SENSOR_COLS)


def compute_status(row: StationLatest) -> str:
    # CASE 1: tidak ada waktu observasi
    if not row.last_observed_at:
        return "OFF"

    # CASE 2: ada data API, tapi semua sensor null
    # if not has_sensor_data(row):
    #     return "DElAY"

    now_utc = datetime.now(timezone.utc)
    diff_minutes = (now_utc - row.last_observed_at).total_seconds() / 60

    # New Case: Jika ada data tapi semua sensor null tidak langsung OFF apabila belum lebih 24 jam
    if not has_sensor_data(row):
        if diff_minutes <= 1440:
            return "DELAY"
        else:
            return "OFF"

    if row.interval_detected == "1min":
        if diff_minutes <= 60:
            return "ON"
        elif diff_minutes <= 1440:
            return "DELAY"
        else:
            return "OFF"

    if row.interval_detected == "10min":
        if diff_minutes <= 60:
            return "ON"
        elif diff_minutes <= 1440:
            return "DELAY"
        else:
            return "OFF"

    # fallback jika unknown
    if diff_minutes <= 60:
        return "ON"
    elif diff_minutes <= 1440:
        return "DELAY"
    else:
        return "OFF"


def ensure_station_latest_rows(db: Session):
    stations = db.query(Station).all()

    for st in stations:
        latest = db.get(StationLatest, st.id_station)
        if not latest:
            db.add(
                StationLatest(
                    id_station=st.id_station,
                    tipe_station=st.tipe_station,
                    status_realtime="OFF",
                )
            )

    db.commit()


def refresh_station_latest_statuses(db: Session):
    ensure_station_latest_rows(db)

    rows = db.query(StationLatest).all()

    for row in rows:
        row.status_realtime = compute_status(row)

    db.commit()