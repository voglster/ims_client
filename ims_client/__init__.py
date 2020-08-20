"""Simple python client for the capspire IMS Server"""

__version__ = "0.1.1"
from datetime import datetime
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed
from dateutil.parser import parse
import requests

from os import getenv


class InventoryManagementServer:
    def __init__(self, base_url=None, system_psk=None):
        self.base_url = base_url or getenv("IMS_URL")
        self.system_psk = system_psk or getenv("SYSTEM_PSK")

    @property
    def params(self):
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
        r = requests.post(f"{self.base_url}/tank_inventory/readings", params=params)
        data = r.json() if r.status_code == 200 else []
        for row in data:
            row["read_time"] = parse(row["read_time"])
            row["run_time"] = parse(row["run_time"])
        return data

    @logger.catch(reraise=True)
    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    def tanks(self, store="", tank=""):
        params = {
            **self.params,
            "store_number": store,
            "tank_id": str(tank),
        }
        r = requests.post(f"{self.base_url}/tank/tanks", params=params)
        return r.json()
