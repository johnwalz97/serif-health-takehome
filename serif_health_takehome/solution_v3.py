import asyncio
import json
import multiprocessing
import os
import sys

import requests
from aiohttp import ClientSession, ClientTimeout
from gzip_stream import AsyncGZIPDecompressedStream
from tqdm import tqdm

URL = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2023-04-01_anthem_index.json.gz"
LOOKUP_URL = (
    "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/{ein}.json"
)


def process_ein(ein):
    urls = set()

    resp = requests.get(LOOKUP_URL.format(ein=ein))
    if resp.status_code != 200:
        print(resp.text)
        raise RuntimeError(f"Failed to download file: {resp.status_code}")

    if "_PPO_" not in resp.text:
        return urls

    data = resp.json()

    for f in data["Blue Cross Blue Shield Association Out-of-Area Rates Files"]:
        if f["displayname"].split("2023-04_")[1][:2]:
            urls.add(f["url"])

    return urls


def process_line(line):
    try:
        data = json.loads(line)
    except json.decoder.JSONDecodeError:
        print(f"Failed to decode line: {line}")
        return set()

    if data["reporting_plans"][0]["plan_id_type"] != "EIN":
        # skip non-EIN plans for now
        return set()

    ein = data["reporting_plans"][0]["plan_id"]

    return process_ein(ein)


def worker(input_queue, output_queue):
    while True:
        line = input_queue.get()
        if line is None:
            break
        output_queue.put(process_line(line))


async def download_file(url: str):
    ny_urls = set()

    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()

    num_workers = multiprocessing.cpu_count()
    workers = [
        multiprocessing.Process(target=worker, args=(input_queue, output_queue))
        for _ in range(num_workers)
    ]

    for w in workers:
        w.start()

    async with ClientSession(timeout=ClientTimeout(total=60 * 60 * 4)) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Failed to download file: {resp.status}")

            decompressed_stream = AsyncGZIPDecompressedStream(resp.content)

            # create progress bar that gives human-readable file size
            progress_bar = tqdm(
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                total=int(resp.headers["Content-Length"]),
                desc="Downloading",
            )

            unfinished_line = ""

            while True:
                # read in 1mb chunks
                chunk = await decompressed_stream.read(1024 * 1024 * 10)
                if not chunk:
                    break

                # if the last line was not finished, add it to the beginning of the next chunk
                chunk = unfinished_line + chunk.decode()
                # find the last newline in the chunk
                last_newline = chunk.rfind("\n")
                # if there is no newline, then the last line is not finished
                if last_newline == -1:
                    unfinished_line = chunk
                    continue
                # otherwise, save everything after the last newline for the next chunk
                unfinished_line = chunk[last_newline + 1 :]
                # only process the lines before the last newline
                chunk = chunk[:last_newline]

                # each line is a new "reporting_plans" object (except for the first few lines)
                for line in chunk.splitlines():
                    if line.startswith('{"reporting_plans"'):
                        input_queue.put(line[:-1])

                progress_bar.update(len(chunk))

            # Send sentinel values to terminate the workers
            for _ in range(num_workers):
                input_queue.put(None)

            # Wait for all workers to finish
            for w in workers:
                w.join()

            # Collect the results from the output queue
            while not output_queue.empty():
                ny_urls |= output_queue.get()

            progress_bar.close()

    with open("ny_urls.txt", "w") as f:
        f.write("\n".join(ny_urls))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_file(sys.argv[1] if len(sys.argv) > 1 else URL))
