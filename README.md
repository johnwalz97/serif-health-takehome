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
python serif_health_takehome/solution_v3.py
```

## Solution Overview

First things first, this is a very MVP solution. The code is very terse and hard to read. Its brittle and has very little validation or error handling. The goal that I had going into this was creating a fast solution. This took a bit longer than I expected and I ran up against the 2 hour mark.

After getting a sample of the inputs and looking over the requirements, I decided that I would have to take a streaming approach where the data gets uncompressed and processed as it is downloaded. This is because the data is too large to fit in memory and there is no reason to wait to download the entire file before processing it. I took an asynchronous approach to this using the `aiohttp` and `gzip-stream` libraries. I also used the `asyncio` library to manage the asynchronous tasks. Alternatively, I could have used multiple threads to accomplish the same thing. If I had some more time, I think the best approach would be to use multi-processing so that one process could download the data, another could decompress it, and a third could process it. This would allow for the most efficient use of resources. In a real production setting though, the best approach would be a true streaming data pipeline using something like Kafka.

It took me a bit to get the hang of how to figure out which files were for NY. I figured out that the files were the same between the index and the EIN lookup and that the EIN lookup had the display name which contained the state and the plan type. I then figured out that the files had a coding system for states which would allow me to skip looking up the EIN every time and instead progressively build a crosswalk dictionary between the state code and the state name. This way I could just loop through the files, check if the state code was NY and then add it to the set of NY files. Problem was that after doing this I realized that I had no way of figuring out which plans were PPOs. So I had to scrap that approach and go back to doing the EIN lookup everytime. I didn't look too hard into an alternative.

This slowed down everything quite a bit so I decided to tackle converting it to a multi-processing solution. This sped things up quite a bit. You can see the evolution of my solutions in the different solution files. The final solution would take about 45 minutes on my home machine just because of my network speed. I ran it on a digital ocean droplet and it finished in under 15 minutes. On an AWS instance close to where the data is hosted, it would probably be even faster. I also noticed that the main process which downloads and decompresses the data is the bottleneck. An improvement would be to use separate processes for downloading and decompressing but this would require a bit more work to implement the shared buffer between the two processes.

A few changes I would make if this was a real production solution and I had more time:

- Fix the progress bar to show the correct progress (right now it uses the total bytes of the compressed file instead of the total bytes of the uncompressed file so its off by like 20-ish percent)
- Use a more resilient solution for parsing the URLs to get the state information
- Use multi-processing to improve performance (this is actually done)
- Break out the code into more modular functions
- Add more validation and error handling
- Make the approach work for other states and plan types
- Use Kafka to create a true streaming data pipeline
