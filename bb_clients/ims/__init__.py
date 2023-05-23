import json
from asyncio import sleep
from datetime import datetime, timedelta
from functools import lru_cache
from http.client import HTTPException
from json import JSONDecodeError
from os import getenv
from typing import Iterable, List, Union, Optional

import httpx
import pytz
from dateutil.parser import parse
from loguru import logger
from pydantic import BaseModel
from pydantic.json import pydantic_encoder
from starlette.status import HTTP_200_OK
from tenacity import retry, stop_after_attempt, wait_fixed


class Tank(BaseModel):
    id: str
    monitor_type: str
    payload: dict
    product: Optional[str]
    sample_rate: int
    status: str
    store_number: str
    tank_id: str
    temperature: Optional[float]
    updated: datetime
    volume: float


class BaseStoreTank(BaseModel):
    store: str
    tank: str

    @property
    def identity(self):
        return f"{self.store}:{self.tank}"

    def store_tank_query(self):
        return {"store_number": self.store, "tank_id": self.tank}


class Reading(BaseModel):
    store_number: str
    tank_id: str
    run_time: datetime  # UTC
    read_time: datetime  # UTC
    volume: float
    product: str = None
    temperature: float = 0


class RegisterTankMonitorRequest(BaseModel):
    store_number: str
    host: str
    port: int
    monitor_type: str


class NearestReading(BaseModel):
    site: str
    tank_id: str
    read_time: datetime  # UTC
    volume: float
    diff: int  # Milliseconds off of target date


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
    def readings(
        self,
        store,
        tank,
        start: datetime,
        end: datetime = None,
        include_manual=True,
        limit=None,
    ):
        params = {
            **self.params,
            "store_number": store,
            "tank_id": str(tank),
            "start_date": start.isoformat(),
            "include_manual": include_manual,
        }

        if end:
            params["end_data"] = end.isoformat()
        if limit:
            params["limit"] = limit
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
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def nearest_readings(
        self, store_numbers: list[str], date: datetime
    ) -> list[NearestReading]:
        r = httpx.post(
            f"{self.base_url}/tank_inventory/nearest",
            json=store_numbers,
            params={"date": date.isoformat(), **self.params},
            timeout=self.timeout,
        )
        data = r.json() if r.status_code == 200 else []

        def fix_store_to_site(row):
            row["site"] = row["store_number"]
            return row

        return [NearestReading.parse_obj(fix_store_to_site(row)) for row in data]

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def latest_readings(self, store_tanks: Iterable[BaseStoreTank]) -> list[Reading]:
        encoded = json.dumps([d.dict() for d in store_tanks], default=pydantic_encoder)
        r = httpx.post(
            f"{self.base_url}/tank_inventory/latest/many",
            data=encoded,
            params=self.params,
            timeout=self.timeout,
        )
        data = r.json() if r.status_code == 200 else []
        return [Reading.parse_obj(row) for row in data]

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def tank_connection_information(self, store, tank):
        params = {
            **self.params,
            "store_number": store,
            "tank_id": str(tank),
        }

        r = httpx.post(
            f"{self.base_url}/tank/tank_connection_information",
            params=params,
            timeout=self.timeout,
        )
        if r.status_code == 200:
            return r.json()
        raise Exception(f"Ims error {r.status_code}", r.text)

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def store_connection_information(self, store):
        params = {
            **self.params,
            "store_number": store,
        }

        r = httpx.post(
            f"{self.base_url}/tank/store_connection_information",
            params=params,
            timeout=self.timeout,
        )
        if r.status_code == 200:
            return r.json()
        raise Exception(f"Ims error {r.status_code}", r.text)

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
                    f"since start: {datetime.utcnow() - start}, archived {limit * count}"
                )
            if log_info and not (datetime.utcnow() - start) % timedelta(minutes=1):
                logger.info(
                    f"average per {limit} -> {sum(avg_time) / len(avg_time)} seconds"
                )
                avg_time = []
            await sleep(sleep_sec)
            delta = datetime.utcnow()
        if log_info:
            logger.info(f"done in {datetime.utcnow() - start}")
        return True

    @logger.catch(reraise=True)
    def replication_data(
        self, window_start: datetime = None, limit: int = 1000, force: bool = False
    ) -> Iterable:
        if not window_start:
            window_start = datetime.utcnow() - timedelta(days=2)
        window_end = datetime.utcnow()
        count = 0
        total = self.get_replication_count(force, limit, window_end, window_start)
        while count < total:
            r = self.get_replication_data_chunk(
                count, force, limit, window_end, window_start
            )
            try:
                chunk = r.json()
                if total == limit:
                    total = chunk.get("total", limit)
                if chunk_data := chunk.get("data", []):
                    count += chunk.get("count", 0)
                    yield chunk_data
                else:
                    break
            except JSONDecodeError:
                break

    def get_replication_data_chunk(self, count, force, limit, window_end, window_start):
        r = httpx.post(
            f"{self.base_url}/logs/replication_data",
            params={
                **self.params,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "skip": count,
                "limit": limit,
                "force": force,
            },
            json={},
            timeout=self.timeout,
        )
        return r

    def get_replication_count(self, force, limit, window_end, window_start) -> int:
        r = httpx.post(
            f"{self.base_url}/logs/replication_count",
            params={
                **self.params,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "force": force,
            },
            json={},
            timeout=self.timeout,
        )
        if r.status_code != HTTP_200_OK:
            raise HTTPException(
                422, f"Bad response from target IMS service: {r.status_code}"
            )
        try:
            total = r.json().get("total", limit)
        except JSONDecodeError:
            raise HTTPException(500, f"Error reading response from target IMS service")
        return total

    def register_tank_monitor(self, req: RegisterTankMonitorRequest):
        logger.info(f"Sending to {self.base_url}/tank/register/create")
        params = {
            **self.params,
        }
        data = req.dict()
        data["ip_address"] = data.pop("host")

        r = httpx.post(
            f"{self.base_url}/tank/register/create",
            params=params,
            json=data,
            timeout=self.timeout,
        )
        return r.json()


@lru_cache(maxsize=2)
def get_ims_service(base_url=None, system_psk=None, timeout=120):
    return InventoryManagementSystem(base_url, system_psk, timeout)
