from datetime import datetime, timezone
from sqlalchemy.orm import Session
from models.latest_data import StationLatest
from models.stations import Station


def compute_status(last_observed_at, interval_detected: str | None) -> str:
    if not last_observed_at:
        return "NO DATA"

    now_utc = datetime.now(timezone.utc)
    diff_minutes = (now_utc - last_observed_at).total_seconds() / 60

    if interval_detected == "1min":
        if diff_minutes <= 60:
            return "ON"
        elif diff_minutes <= 120:
            return "DELAY"
        else:
            return "OFF"

    if interval_detected == "10min":
        if diff_minutes <= 60:
            return "ON"
        elif diff_minutes <= 120:
            return "DELAY"
        else:
            return "OFF"

    # fallback jika unknown
    if diff_minutes <= 60:
        return "ON"
    elif diff_minutes <= 120:
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
                    status_realtime="NO DATA",
                )
            )

    db.commit()


def refresh_station_latest_statuses(db: Session):
    ensure_station_latest_rows(db)

    rows = db.query(StationLatest).all()

    for row in rows:
        row.status_realtime = compute_status(
            last_observed_at=row.last_observed_at,
            interval_detected=row.interval_detected,
        )

    db.commit()