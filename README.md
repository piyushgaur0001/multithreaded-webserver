# Multithreaded Proxy Web Server

A multithreaded HTTP proxy server in C with LRU caching.

---

## How It Works

```
Client → Proxy → Remote Server
                     ↓
              Cache response
         (serve from cache next time)
```

- Accepts HTTP GET requests from browser/client
- Forwards to remote server
- Caches response (LRU eviction)
- Subsequent same requests served from cache

---

## Features

- Multi-client support via **pthreads** (max 10 concurrent)
- **Semaphore** to limit concurrent connections
- **LRU Cache** with mutex-protected access
- Supports **HTTP/1.0** and **HTTP/1.1**

---

## Prerequisites

```bash
gcc / g++
make
pthreads (libpthread)
```

---

## Build

```bash
make
```

## Run

```bash
./proxy <port>

# Example
./proxy 8080
```

## Clean

```bash
make clean
```

---

## Usage

Set your browser's HTTP proxy to `localhost:<port>`, then browse normally.

---

## Cache

| Property | Value |
|---|---|
| Type | LRU (Least Recently Used) |
| Max cache size | 10 MB |
| Max element size | 1 MB |
| Thread safety | `pthread_mutex_t` |

When cache is full, the least recently used entry is evicted.

---

## Project Structure

```
.
├── proxy_server.c      # Main server + cache logic
├── proxy_parse.c       # HTTP request parser
├── proxy_parse.h       # Parser header
└── Makefile
```

---

## Limitations

- Only supports **GET** method
- No HTTPS/CONNECT tunneling
- No persistent connection support (`Connection: close` forced)