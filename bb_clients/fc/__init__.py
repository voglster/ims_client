"""Simple python client for the capspire Forecast Service"""

__version__ = "0.1.16"

from datetime import datetime
from functools import lru_cache
from os import getenv
from typing import List

import httpx
from loguru import logger
from pydantic import BaseModel
from sparkles import chunked
from tenacity import retry, stop_after_attempt, wait_fixed


class Forecast(BaseModel):
    date: datetime
    lower: float = None
    upper: float = None
    value: float


class UpdateRequest(BaseModel):
    store_number: str
    tank_id: str
    market: str = None
    product: str = None
    max_age_hours: int = None
    disabled: bool = None
    timezone: str = None
    daily_lifting_estimate: int = None
    daily_lifting_std: int = None
    storage_max: int = None
    volume_strategy: str = None
    target_volume: int = None
    island: str = None


class ForecastService:
    def __init__(self, base_url=None, system_psk=None, timeout=10):
        self.base_url = base_url or getenv("FC_URL")
        self.system_psk = system_psk or getenv("SYSTEM_PSK")
        self.timeout = timeout
        logger.info(f"connecting to forecast service {self.base_url}")

    def __repr__(self):
        return f"FS_{self.base_url}"

    def __str__(self):
        return self.__repr__()

    @property
    def params(self):
        return {"system_psk": self.system_psk}

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def near(self, store: str, tank: str, as_model=False):
        r = httpx.post(
            f"{self.base_url}/forecast/latest_near",
            params=self.params,
            json={"store": store.upper(), "tank": str(tank)},
            timeout=self.timeout,
        )
        data = r.json() if r.status_code == 200 else []
        return [Forecast(**r) for r in data] if as_model else data

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def far(self, store: str, tank: str, as_model: bool = False):
        r = httpx.post(
            f"{self.base_url}/forecast/latest_far",
            params=self.params,
            json={
                "store": store.upper(),
                "tank": str(tank),
            },
            timeout=self.timeout,
        )
        data = r.json() if r.status_code == 200 else []
        return [Forecast(**r) for r in data] if as_model else data

    def period_demand(self, store: str, tank: str, dates: list):
        body = {
            "store": store,
            "tank": tank,
            "periods": [
                {"period": p, "start": s.isoformat(), "end": e.isoformat()}
                for p, s, e in dates
            ],
        }
        r = httpx.post(
            f"{self.base_url}/forecast/period_demand",
            params=self.params,
            json=body,
            timeout=self.timeout,
        )
        if r.status_code == 200 and r.json() is not None:
            return r.json()
        else:
            logger.info(f"{store} and {tank} no data")
            return {}

    def period_demand_many(self, ranges: list, chunk_size=None):
        if chunk_size is None:
            return self._period_demand_many(ranges)

        res = {}
        for idx, chunk in enumerate(chunked(ranges, chunk_size)):
            logger.info(
                f"period demand many {chunk_size*idx}-{chunk_size*(idx+1)} of {len(ranges)}"
            )
            res.update(self._period_demand_many(chunk))
        return res

    def _period_demand_many(self, ranges: list):
        body = [
            {
                "store": store.upper(),
                "tank": str(tank),
                "periods": [
                    {"period": p, "start": s.isoformat(), "end": e.isoformat()}
                    for p, s, e in dates
                ],
            }
            for store, tank, dates in ranges
        ]
        r = httpx.post(
            f"{self.base_url}/forecast/period_demand_many",
            params=self.params,
            json=body,
            timeout=self.timeout,
        )
        if r.status_code == 200 and r.json() is not None:
            return r.json()
        else:
            logger.error(f"Error forecast many {r.text}")
            return {}

    def latest_near_version(self, store, tank):
        return self.latest(store, tank, "near").get("version", 0)

    def latest_near_date(self, store, tank):
        return self.latest(store, tank, "near").get("created_at", datetime.min)

    def latest_far_version(self, store, tank):
        return self.latest(store, tank, "far").get("version", 0)

    def latest_far(self, store, tank):
        return self.latest(store, tank, "far").get("created_at", datetime.min)

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def latest(self, store, tank, forecast_type) -> dict:
        r = httpx.post(
            f"{self.base_url}/forecast/version",
            params=self.params,
            json={
                "store": store.upper(),
                "tank": str(tank),
                "type": forecast_type,
            },
            timeout=self.timeout,
        )
        return r.json() if r.status_code == 200 else {}

    def update_tank_config_many(self, reqs: List[UpdateRequest]):
        r = httpx.post(
            f"{self.base_url}/tank_config/update_many",
            params=self.params,
            json=[req.dict() for req in reqs],
            timeout=self.timeout,
        )
        return r.json() if r.status_code == 200 else {}

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def tank_config(self, store: str = None, tank: str = None):
        params = {**self.params}
        if store:
            params["store"] = store
        if tank:
            params["tank"] = tank
        r = httpx.post(f"{self.base_url}/tank_config/list", params=params, timeout=120)
        if r.status_code != 200:
            logger.error(f"Forecaster request error {r.status_code} {r.text}")
            return []
        ret = []
        for tank in r.json():
            if dle := tank.get("daily_lifting_estimate", None):
                tank["turn_time"] = tank.get("target_volume", 0) / dle
                tank["empty_time"] = tank.get("storage_max", 0) / dle
            ret.append(tank)
        return ret

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def create_tank_config(self, store: str, tank: str, near_frequency=1):
        r = httpx.post(
            f"{self.base_url}/tank_config/create",
            params=self.params,
            json={"store": store, "tank": tank, "near_frequency": near_frequency},
            timeout=self.timeout,
        )
        return r.json()

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def disable_tank_config(self, store: str, tank: str):
        r = httpx.post(
            f"{self.base_url}/tank_config/disable_tank",
            params=self.params,
            json={"store": store, "tank": tank},
            timeout=self.timeout,
        )
        return r.json()

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def forecast_list(self, forecast_type: str = None):
        body = {"type": forecast_type} if forecast_type else {}
        r = httpx.post(
            f"{self.base_url}/forecast/list",
            params=self.params,
            json=body,
            timeout=self.timeout,
        )
        return r.json()


@lru_cache(maxsize=2)
def get_fc_service(base_url=None, system_psk=None, timeout=120):
    return ForecastService(base_url, system_psk, timeout)
