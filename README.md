# qbitdebrid: Local Automation Daemon & Streaming Proxy

qbitdebrid transforms qBittorrent into a high-speed HTTP download manager backed by TorBox (a Debrid provider). It implements an intelligent proxy layer that intercepts torrent metadata, verifies cache status, isolates P2P traffic, and streams content with just-in-time prefetching.

## Features

- **Torrent Watchdog**: Continuous monitoring of qBittorrent for new torrents
- **Cache Verification**: Intelligent detection of TorBox-cached content
- **P2P Isolation**: Automatic injection of Web Seed URLs and tracker removal
- **Transparent Proxy**: Byte-range mapping and high-efficiency chunked streaming
- **JIT Prefetching**: Smart background link generation at 95% file completion
- **Edge Case Handling**: ZIP extraction, piece boundary overlap, Cloudflare resilience

## Setup

```bash
# 1. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# Edit .env and set TORBOX_API_KEY, QBIT_HOST, QBIT_PORT

# 4. Run
python -m qbitdebrid.main
```

## How it works

The daemon monitors qBittorrent for new torrents (all states including paused), checks if they're cached on TorBox, and if yes:
- Injects a web seed URL pointing to the local proxy
- Forces sequential download mode
- Removes trackers to suppress P2P
- Limits connections to avoid DHT/PeX traffic

The proxy streams content through HTTP range requests without buffering.