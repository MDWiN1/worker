from datetime import datetime
from typing import Any


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        v = float(value)
        if v <= -9e8:
            return None
        return v
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None


def parse_station_data(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id_station": str(item["id_station"]),
        "tipe_station": str(item["tipe_station"]).lower().strip(),
        "name_station": item.get("name_station"),
        "latitude": to_float(item.get("latt_station")),
        "longitude": to_float(item.get("long_station")),
        "elevasi": to_float(item.get("elv_station")),
        "nama_kota": item.get("nama_kota"),
    }


def parse_arg_observation(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id_station": str(item["id_station"]),
        "observed_at": parse_datetime(item.get("tanggal")),
        "rr": to_float(item.get("rr")),
        #"rr_flag": to_int(item.get("rr_flag")),
        #"raw_json": item,
    }


def parse_aws_observation(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id_station": str(item["id_station"]),
        "observed_at": parse_datetime(item.get("tanggal")),
        "rr": to_float(item.get("rr")),
        #"rr_flag": to_int(item.get("rr_flag")),
        "pp_air": to_float(item.get("pp_air")),
        #"pp_air_flag": to_int(item.get("pp_air_flag")),
        "rh_avg": to_float(item.get("rh_avg")),
        #"rh_avg_flag": to_int(item.get("rh_avg_flag")),
        "sr_avg": to_float(item.get("sr_avg")),
        #"sr_avg_flag": to_int(item.get("sr_avg_flag")),
        "sr_max": to_float(item.get("sr_max")),
        #"sr_max_flag": to_int(item.get("sr_max_flag")),
        "wd_avg": to_float(item.get("wd_avg")),
        #"wd_avg_flag": to_int(item.get("wd_avg_flag")),
        "ws_avg": to_float(item.get("ws_avg")),
        #"ws_avg_flag": to_int(item.get("ws_avg_flag")),
        "ws_max": to_float(item.get("ws_max")),
        #"ws_max_flag": to_int(item.get("ws_max_flag")),
        "tt_air_avg": to_float(item.get("tt_air_avg")),
        #"tt_air_avg_flag": to_int(item.get("tt_air_avg_flag")),
        "tt_air_min": to_float(item.get("tt_air_min")),
        #"tt_air_min_flag": to_int(item.get("tt_air_min_flag")),
        "tt_air_max": to_float(item.get("tt_air_max")),
        #"tt_air_max_flag": to_int(item.get("tt_air_max_flag")),
        #"raw_json": item,
    }


def parse_aaws_observation(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id_station": str(item["id_station"]),
        "observed_at": parse_datetime(item.get("tanggal")),
        "rr": to_float(item.get("rr")),
        #"rr_flag": to_int(item.get("rr_flag")),
        "pp_air": to_float(item.get("pp_air")),
        #"pp_air_flag": to_int(item.get("pp_air_flag")),
        "rh_avg": to_float(item.get("rh_avg")),
        #"rh_avg_flag": to_int(item.get("rh_avg_flag")),
        "sr_avg": to_float(item.get("sr_avg")),
        #"sr_avg_flag": to_int(item.get("sr_avg_flag")),
        "sr_max": to_float(item.get("sr_max")),
        #"sr_max_flag": to_int(item.get("sr_max_flag")),
        "wd_avg": to_float(item.get("wd_avg")),
        #"wd_avg_flag": to_int(item.get("wd_avg_flag")),
        "ws_avg": to_float(item.get("ws_avg")),
        #"ws_avg_flag": to_int(item.get("ws_avg_flag")),
        "ws_max": to_float(item.get("ws_max")),
        #"ws_max_flag": to_int(item.get("ws_max_flag")),
        "tt_air_avg": to_float(item.get("tt_air_avg")),
        #"tt_air_avg_flag": to_int(item.get("tt_air_avg_flag")),
        "tt_air_min": to_float(item.get("tt_air_min")),
        #"tt_air_min_flag": to_int(item.get("tt_air_min_flag")),
        "tt_air_max": to_float(item.get("tt_air_max")),
        #"tt_air_max_flag": to_int(item.get("tt_air_max_flag")),
        "ws_50cm": to_float(item.get("ws_50cm")),
        #"ws_50cm_flag": to_int(item.get("ws_50cm_flag")),
        "wl_pan": to_float(item.get("wl_pan")),
        #"wl_pan_flag": to_int(item.get("wl_pan_flag")),
        "ev_pan": to_float(item.get("ev_pan")),
        #"ev_pan_flag": to_int(item.get("ev_pan_flag")),
        "ws_2m": to_float(item.get("ws_2m")),
        #"ws_2m_flag": to_int(item.get("ws_2m_flag")),
        #"raw_json": item,
    }