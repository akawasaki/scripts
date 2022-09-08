import asyncio
import csv


def write_csv_file(path: str, records: list):
    with open(path, 'w', newline="") as f_write:
        writer = csv.writer(f_write, dialect='excel')
        item_type = type(records[0])
        if item_type == str:
            for i in records:
                writer.writerow([i])
        elif item_type == list:
            writer.writerows(records)


async def semaphore_asyncio_gather(*tasks):
    """
    General helper to run asyncio.gather with limited number of concurrent tasks
    """
    semaphore = asyncio.Semaphore(len(tasks))

    async def semaphore_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(semaphore_task(task) for task in tasks))
