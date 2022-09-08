import asyncio
import csv
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from pydantic import BaseModel
import logging

from converters.config import AUTH_TOKEN, BASE_URL, ENV
from converters.converter_utils import write_csv_file

data_path = "./data"
output_path = "./outputs"
log_path = "./logs"
base_url = BASE_URL

headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {AUTH_TOKEN}'
}

env = ENV

item_not_exist = []
convert_failed = []


class Response(BaseModel):
    _id: str

    # validate any if need


async def call_get_request(request_id: str):
    try:
        url = f"{BASE_URL}/{request_id}/tenants"
        async with httpx.AsyncClient(headers=headers) as ac:
            timeout = httpx.Timeout(45.0, read=None)
            return await ac.get(url=url, timeout=timeout)
    except Exception as err:
        logging.warning(f"Caught an exception: {err.args}")
        return None


async def write_result(result, writer):
    async with asyncio.Lock():   # lock for gracefully write to shared file object
        writer.writerow(result)


async def generate_data(_id, writer):
    print(f"Start generating for _id: {_id}")
    resp = await call_get_request(request_id=_id)
    if not resp:
        logging.warning(f"Request failed with id: {_id}")
        convert_failed.append(_id)
        return
    if resp.status_code != 200:
        if resp.status_code == 404:
            print(f"Status code: {resp.status_code}. Message: {resp.text} for id: {_id}")
            item_not_exist.append(_id)
        elif resp.status_code >= 400:
            print(f"Status code: {resp.status_code}. Message: {resp.text} for id: {_id}")
            convert_failed.append(_id)
        return

    if resp and resp.status_code == 200:
        formatted = None
        try:
            resp = resp.json()
            result = resp | {"_id": _id}
            formatted = Response(**result)
        except Exception as err:
            print(f"Caught an exception on formatting data: {err.args}")
            convert_failed.append(_id)
        try:
            await write_result(result=formatted.dict(), writer=writer)
        except Exception as err:
            print(f"Caught an exception on writing data: {err.args}")
            convert_failed.append(_id)


async def main():
    local_time = datetime.now(tz=ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")
    start = datetime.now()

    with open(f"{data_path}/customers/", newline='') as f_read, open(
        f"{output_path}/{local_time}.csv", 'w') as f_write:
        reader = csv.reader(f_read, delimiter=',')
        temp_header = Response(_id="temp").dict()
        field_names=temp_header.keys()
        writer = csv.DictWriter(f_write, fieldnames=field_names)
        writer.writeheader()

        _ids = []
        for row in reader:
            _ids.append(row[0])

        # Create tasks
        index = 0
        batch_size = 3

        for i in range(0, len(_ids), batch_size):

            tasks = [*map(lambda id: generate_data(_id=id, writer=writer), _ids[i:i+batch_size])]
            semaphore = asyncio.Semaphore(len(tasks))

            async def semaphore_task(task):
                async with semaphore:
                    return await task

            await asyncio.gather(*(semaphore_task(task) for task in tasks))

            if i + (batch_size * 2) > len(_ids):
                index = i + batch_size
                break

        for i in range(index, len(_ids)):
            await generate_data(_id=_ids[i], writer=writer)

        # Log failed data
        write_csv_file(path=f"{log_path}/{local_time}.csv", records=item_not_exist)
        write_csv_file(path=f"{log_path}/{local_time}.csv", records=convert_failed)

        print(f"Time: {datetime.now() - start}")


if __name__ == "__main__":
    asyncio.run(main())
