# qBitdebrid

**qBitdebrid** is an ultra-fast, lightweight local HTTP Web Seed proxy that allows you to download cached TorBox debrid files directly inside **qBittorrent** at gigabit speeds without requiring a VPN. 

By acting as a bridge, it leverages qBittorrent's native **Web Seeds** feature to stream content securely over a single connection, combining the speed of TorBox with the polished torrent management interface of qBittorrent.

---

##  Features

* **Simple Automation:** Just add a torrent with your specific tag. The daemon automatically detects it, verifies cache, deletes it, and replaces it with a mutated version configured to download directly from TorBox CDN instead of the peer-to-peer network.
* **Maximum Speed:** Downloads directly from TorBox's high-speed, multi-threaded CDN pipelines rather than slow, peer-dependent torrent swarms, easily saturating gigabit connections.
* **Completely Isolated:** Automatically strips out all trackers, DHT, and peer exchange, keeping your network traffic 100% private.
* **No VPN Needed:** Since all P2P torrent activity is handled securely on TorBox's servers, you only download direct files from TorBox's CDN. No local VPN is required!
* **Ultra-Lightweight:** Since you connect only to TorBox's fast server instead of hundreds of P2P peers, CPU and memory usage on your system is virtually non-existent.
* **Native Control:** Pause, play, queue, and manage your downloads anytime utilizing the full power, speed limits, and polished features of qBittorrent.

---

## Docker Deployment

The official Docker image is available on Docker Hub as [akhilrana/qbitdebrid](https://hub.docker.com/r/akhilrana/qbitdebrid).

> 💡 **Configuration Note:** For a full explanation of all available configuration variables, please check the [**.env.example**](./.env.example) file.

### Option A: Docker Compose (Recommended)

Create a `docker-compose.yml` file (check [**docker-compose.yml**](./docker-compose.yml) file for reference):

```yaml
services:
  qbitdebrid:
    image: akhilrana/qbitdebrid:latest
    container_name: qbitdebrid
    restart: unless-stopped
    ports:
      - "8593:8593"
    environment:
      - TORBOX_API_KEY=your_api_key_here
      - QBIT_HOST=192.168.1.50 # IP of your qBittorrent instance
      - QBIT_PORT=8080         # Port of your qBittorrent WebUI
      - PROXY_HOST=0.0.0.0     # Bind to all interfaces inside container
      - PROXY_PREFETCH_BUFFER_MB=256
```

Run the container:
```bash
docker compose up -d
```

### Option B: Docker Run Command

```bash
docker run -d \
  --name qbitdebrid \
  -p 8593:8593 \
  -e TORBOX_API_KEY="your_api_key_here" \
  -e QBIT_HOST="192.168.1.50" \
  -e QBIT_PORT="8080" \
  -e PROXY_HOST="0.0.0.0" \
  -e PROXY_PREFETCH_BUFFER_MB="256" \
  akhilrana/qbitdebrid:latest
```

---

## Local Development Setup

If you prefer to run or build the project locally without Docker:

### 1. Set Up Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
```bash
cp .env.example .env
# Edit .env and input your TORBOX_API_KEY, QBIT_HOST, and QBIT_PORT
```

### 4. Run the Server
```bash
python -m qbitdebrid.main
```

---

## How It Works (The Lifecycle)

qBitdebrid operates seamlessly in the background as an automated bridge:

1. **Torrent Detection:** You add a magnet link or `.torrent` file to qBittorrent using your configured tag (default: `qbitdebrid`).
2. **Metadata Extraction:** The background watchdog immediately detects the torrent, pauses it, and exports its raw binary from qBittorrent's memory (ensuring compatibility with private trackers).
3. **TorBox Handshake:** The daemon uploads this file to TorBox's API and verifies whether the torrent is already cached in TorBox's database.
4. **The Swap (Mutation):** 
   * The daemon deletes the original torrent from qBittorrent.
   * It constructs a new, mutated `.torrent` binary containing no trackers, no DHT, and a single **Web Seed URL** pointing directly to your local proxy (`http://<PROXY_HOST>:8593/proxy/<hash>/<path>`).
   * It injects this mutated torrent back into qBittorrent with the connection count strictly locked to `max_connections=1`.
5. **Streaming Proxy Proxying:** qBittorrent begins "downloading" the file. Under the hood, it sends HTTP range requests to our local proxy. The proxy dynamically resolves the real TorBox CDN download link, prefetches 4MB chunks of the file in memory, and feeds them back to qBittorrent over a single, highly stable TCP pipe. No P2P connections are ever established locally, meaning **no VPN is required!**

---

##  Limitations & Disclaimer

* **Unconventional Use Case:** Using qBittorrent strictly as a direct HTTP download manager is highly unconventional. We strongly prefer and encourage you to seed and actively participate in P2P networks to support the sharing community whenever possible.
* **Flaky Web Seed Implementation:** The HTTP and Web Peer (Web Seed) implementation in `libtorrent` (which powers qBittorrent) is notoriously flaky and unreliable. It can occasionally stall or break, requiring a manual restart of the qBittorrent client to restore the stream connection.
* **No Anonymity Guarantee:** We **do not guarantee anonymity** when running without a VPN. If you choose not to use a VPN, you should completely secure your torrent client (for example, by binding qBittorrent's network interface strictly to a local IP address) to prevent accidental leaks.

