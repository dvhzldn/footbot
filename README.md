# Footbot

An asynchronous Python-based automation tool that monitors football matches
across multiple global leagues via the ESPN API and delivers real-time updates,
FPL-specific insights, and Reddit-sourced goal replays to Discord.

## Engineering highlights

- Asynchronous Architecture: Built using `asyncio` and `httpx` to handle
  high-concurrency monitoring across dozen of leagues without blocking.

- Producer-Consumer Pattern: Implements an `asyncio.Queue` with a pool of worker
  tasks to decouple data fetching from Discord delivery, ensuring rate-limit
  compliance.

- Smart Replay Discovery: Automatically searches Reddit for goal replays with
  adaptive jitter and retry logic, filtering through verified video domains.

- Robust Config Management: 100% decoupled logic. All endpoints, thresholds, and
  secrets are managed via `config.json` and `.env` files.

- Defensive Programming: Includes automatic request jitter, rotating
  User-Agents, and graceful shutdown handling.

## Stack

- Language: Python 3.10+

- Network: `httpx` (Async HTTP)

- Scraping: `BeautifulSoup4`

- Concurrency: `asyncio` Semaphores & Queues

- Secrets: `python-dotenv`

## Installation

- Install dependencies:

```bash
pip install -r requirements.txt
```

- Configuration:

Copy `config.json.example` to `config.json` and customize your leagues and
settings.

Create a `.env` file with your Discord Webhook URLs and Reddit User-Agent.

_Note: This project is for personal educational use and internal analysis only._
