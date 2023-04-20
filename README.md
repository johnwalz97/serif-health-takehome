# Serif Health Takehome Interview

## Pre-requisites

- [Poetry](https://python-poetry.org/docs/#installation)

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

## Setup

1. Clone the repository:

```bash
git clone https://github.com/johnwalz97/serif-health-takehome.git
```

2. Install the dependencies:

```bash
poetry install
```

3. Activate the virtual environment:

```bash
poetry shell
```

4. Run the script with:

```bash
python serif_health_takehome/__init__.py
```

## Solution Overview

First things first, this is a very MVP solution. The code is very terse and hard to read. Its brittle and has very little validation or error handling. The goal that I had going into this was creating a fast solution. This took a bit longer than I expected and I ran up against the 2 hour mark.

After getting a sample of the inputs and looking over the requirements, I decided that I would have to take a streaming approach where the data gets uncompressed and processed as it is downloaded. This is because the data is too large to fit in memory and there is no reason to wait to download the entire file before processing it. I took an asynchronous approach to this using the `aiohttp` and `gzip-stream` libraries. I also used the `asyncio` library to manage the asynchronous tasks. Alternatively, I could have used multiple threads to accomplish the same thing. If I had some more time, I think the best approach would be to use multi-processing so that one process could download the data, another could decompress it, and a third could process it. This would allow for the most efficient use of resources. In a real production setting though, the best approach would be a true streaming data pipeline using something like Kafka.

It took me a bit to get the hang of how to figure out which files were for NY and which were PPO. I figured out that the files were the same between the index and the EIN lookup and that the EIN lookup had the display name which contained the state and the plan type. I then figured out that the files had a coding system for states which would allow me to skip looking up the EIN every time and instead progressively build a crosswalk dictionary between the state code and the state name. This way I could just loop through the files, check if the state code was NY and then add it to the set of NY files.

The whole script would have taken about an hour or so on my home network connection but I ended up running it on a Digital Ocean machine and it took about 20 minutes. Overall, I am happy with the solution but would have loved to spend more time improving it.
