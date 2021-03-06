from asyncio import sleep
from datetime import datetime, timedelta
from functools import lru_cache
from os import getenv
from typing import Iterable, List, Union

import httpx
import pytz
from dateutil.parser import parse
from loguru import logger
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_fixed


class Tank(BaseModel):
    id: str
    monitor_type: str
    payload: dict
    product: str
    sample_rate: int
    status: str
    store_number: str
    tank_id: str
    temperature: float
    updated: datetime
    volume: float


class Reading(BaseModel):
    read_time: datetime
    run_time: datetime
    store_number: str
    tank_id: str
    volume: float
    temperature: float


class InventoryManagementSystem:
    def __init__(self, base_url=None, system_psk=None, timeout=10):
        self.base_url = base_url or getenv("IMS_URL")
        self.system_psk = system_psk or getenv("SYSTEM_PSK")
        self.timeout = timeout

    @property
    def params(self) -> dict:
        return {"system_psk": self.system_psk}

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def readings(self, store, tank, start: datetime, end: datetime = None):
        params = {
            **self.params,
            "store_number": store,
            "tank_id": str(tank),
            "start_date": start.isoformat(),
        }

        if end:
            params["end_data"] = end.isoformat()
        r = httpx.post(
            f"{self.base_url}/tank_inventory/readings",
            params=params,
            timeout=self.timeout,
        )
        data = r.json() if r.status_code == 200 else []
        for row in data:
            row["read_time"] = parse(row["read_time"])
            row["run_time"] = parse(row["run_time"])
        return data

    @logger.catch(reraise=True)
    def localize(
        self,
        zone: str,
        store,
        tank,
        start: datetime,
        end: datetime = None,
    ) -> Iterable[dict]:
        tz = pytz.timezone(zone)
        data = self.readings(store, tank, start, end)
        for r in data:
            r["read_time"] = tz.fromutc(r["read_time"]).replace(tzinfo=None)
            r["run_time"] = tz.fromutc(r["run_time"]).replace(tzinfo=None)
            yield r

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def tanks(self, store="", tank="", as_model=False) -> Union[List[dict], List[Tank]]:
        params = {
            **self.params,
            "store_number": store,
            "tank_id": str(tank),
        }
        r = httpx.post(
            f"{self.base_url}/tank/tanks",
            params=params,
            timeout=self.timeout,
        )
        if not as_model:
            return r.json()
        return [Tank(**row) for row in r.json()]

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    async def async_readings(
        self, store: str, tank: str, start: datetime, end: datetime = None
    ) -> List[Reading]:
        params = {
            **self.params,
            "store_number": store,
            "tank_id": str(tank),
            "start_date": start.isoformat(),
        }

        if end:
            params["end_data"] = end.isoformat()
        async with httpx.AsyncClient() as client:
            logger.debug(f"posting to {self.base_url}")
            r = await client.post(
                f"{self.base_url}/tank_inventory/readings",
                params=params,
                timeout=self.timeout,
            )
        data = r.json() if r.status_code == 200 else []
        return [Reading(**row) for row in data]

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    async def async_tanks(self, store="", tank="") -> List[Tank]:
        params = {
            **self.params,
            "store_number": store,
            "tank_id": str(tank),
        }
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{self.base_url}/tank/tanks", params=params, timeout=self.timeout
            )
        if r.status_code != 200:
            logger.warning(f"unable to get any tanks {r.text}")
            return []
        return [Tank(**row) for row in r.json()]

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    async def archive(self, limit: int = 500, days_back: int = 30) -> bool:
        params = {
            **self.params,
            "limit": limit,
            "days_back": days_back,
        }
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{self.base_url}/archive/archive_dangerous",
                params=params,
                timeout=self.timeout,
            )
        if r.status_code != 200:
            logger.warning(
                f"something went wrong in the archive process: r.status_code {r.status_code} r.text {r.text}"
            )
            return False
        return r.json()

    @logger.catch(reraise=True)
    async def archive_all(
        self,
        limit: int = 500,
        days_back: int = 30,
        sleep_sec: int = 10,
        log_info: bool = False,
    ):
        start = datetime.utcnow()
        delta = start
        count = 0
        avg_time: List[float] = []
        while await self.archive(limit, days_back):
            count += 1
            avg_time.append((datetime.utcnow() - delta).total_seconds())
            if log_info:
                logger.info(
                    f"since start: {datetime.utcnow()-start}, archived {limit*count}"
                )
            if log_info and not (datetime.utcnow() - start) % timedelta(minutes=1):
                logger.info(
                    f"average per {limit} -> {sum(avg_time)/len(avg_time)} seconds"
                )
                avg_time = []
            await sleep(sleep_sec)
            delta = datetime.utcnow()
        if log_info:
            logger.info(f"done in {datetime.utcnow()-start}")
        return True


@lru_cache(maxsize=2)
def get_ims_service(base_url=None, system_psk=None, timeout=120):
    return InventoryManagementSystem(base_url, system_psk, timeout)
