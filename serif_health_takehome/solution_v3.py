"""This solution satisfies the requirements and uses multiprocessing to speed up the process"""

import asyncio
import json
import multiprocessing
import os
import sys

from aiohttp import ClientSession, ClientTimeout
from gzip_stream import AsyncGZIPDecompressedStream
from tqdm import tqdm

URL = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2023-04-01_anthem_index.json.gz"
LOOKUP_URL = (
    "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/{ein}.json"
)


async def process_ein(ein):
    urls = set()
    data = None

    async with ClientSession() as session:
        try:
            async with session.get(LOOKUP_URL.format(ein=ein)) as resp:
                if resp.status != 200:
                    # TODO actually handle this
                    print(f"Failed to download file: {resp.status}")
                    return urls

                text = await resp.text()

                if "_PPO_" not in text:
                    return urls

                data = json.loads(text)
        except Exception as e:
            with open("failed_eins.txt", "a") as f:
                f.write(LOOKUP_URL.format(ein=ein) + "\n")
            print(f"Failed to process EIN {ein}: {e}")
            return await process_ein(ein)

    key = "In-Network Negotiated Rates Files"
    if key in data:
        for f in data[key]:
            if "NY_PPO" in f["displayname"]:
                urls.add(f["url"])

    key = "Blue Cross Blue Shield Association Out-of-Area Rates Files"
    if key in data:
        for f in data[key]:
            if f["displayname"].split("2023-04_")[1][:2] == "NY":
                urls.add(f["url"])

    # don't think we need "Out-of-Network Allowed Amounts Files" but not sure

    return urls


async def process_line(line):
    try:
        data = json.loads(line)
    except json.decoder.JSONDecodeError:
        print(f"Failed to decode line: {line[:500]}")
        with open("failed_lines.txt", "a") as f:
            f.write(line + "\n")
        return set()

    if data["reporting_plans"][0]["plan_id_type"] != "EIN":
        # skip non-EIN plans for now
        return set()

    ein = data["reporting_plans"][0]["plan_id"]

    return await process_ein(ein)


async def process_multiple_lines(lines):
    tasks = [process_line(line) for line in lines]
    return await asyncio.gather(*tasks)


def worker(input_queue, identifier):
    with open(f"ny_urls_{identifier}.txt", "a") as f:
        while True:
            lines = []
            for _ in range(100):
                line = input_queue.get()
                if line is None:
                    continue
                lines.append(line)

            if not lines:
                break

            urls_lists = asyncio.run(process_multiple_lines(lines))

            for urls in urls_lists:
                f.write("\n".join(urls))


async def download_file(url: str):
    input_queue = multiprocessing.Queue()
    num_workers = multiprocessing.cpu_count()
    workers = [
        multiprocessing.Process(target=worker, args=(input_queue, i))
        for i in range(num_workers)
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
                total=int(resp.headers["Content-Length"]) * 2, # hacky way to guess the uncompressed size
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

                # TODO: fix this
                progress_bar.update(len(chunk))

            progress_bar.close()

    # Send sentinel values to terminate the workers
    for _ in range(num_workers):
        input_queue.put(None)

    # Wait for all workers to finish
    for w in workers:
        w.join()

    # collect urls into one file
    ny_urls = set()

    for filename in os.listdir("."):
        if filename.startswith("ny_urls_"):
            with open(filename, "r") as f:
                ny_urls.update(f.read().splitlines())
            os.remove(filename)

    with open("ny_urls.txt", "w") as f:
        f.write("\n".join(ny_urls))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_file(sys.argv[1] if len(sys.argv) > 1 else URL))
