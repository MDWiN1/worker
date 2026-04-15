import os
import time
import requests
from datetime import datetime
from typing import Any
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("url_api")
TOKEN = os.getenv("token_api")
REQUEST_DELAY = float(os.getenv("delay"))


def fetch_data_api(
    tipe_station: str,
    start_time: datetime,
    end_time: datetime,
    timeout: int = 120,
) -> list[dict[str, Any]]:
    if tipe_station not in {"arg", "aws", "aaws"}:
        raise ValueError(f"tipe_station tidak valid: {tipe_station}")

    params = {
        "token": TOKEN,
        "filter": "*",
        "tgl_mulai": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "tgl_selesai": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "tipe_station": tipe_station,
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, list):
        raise ValueError(
            f"Response API {tipe_station} bukan list. Dapat: {type(payload).__name__}"
        )

    time.sleep(REQUEST_DELAY)
    return payload