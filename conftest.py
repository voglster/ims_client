import asyncio
import os
from datetime import datetime, timedelta
from pathlib import Path  # Python 3.6+ only
from pprint import pprint

from dotenv import load_dotenv

from ims_client import get_ims_server

env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)


tank = os.getenv("TANK")
store = os.getenv("STORE")
system_psk = os.getenv("SYSTEM_PSK")
ims_url = os.getenv("IMS_URL")


async def test_async_readings():
    ims = get_ims_server(ims_url, system_psk)
    start = datetime.utcnow() - timedelta(days=5)
    return await ims.async_readings(store, tank, start)


async def test_async_tanks():
    ims = get_ims_server(ims_url, system_psk)
    return await ims.async_tanks()


if __name__ == "__main__":
    r = asyncio.run(test_async_readings())
    pprint(r)
