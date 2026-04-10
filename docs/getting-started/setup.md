---
title: Setup
---

# Prefect Server Setup

This guide walks through setting up the Prefect server for the first time on a
host that already has Docker and Docker Compose installed.

## Prerequisites

- Docker Engine and **Docker Compose v2** installed on the host machine.
- The OpticStream source repository cloned on the host (see [Installation](installation.md)).
- Network access to the host from any machine that will use the Prefect UI or
  submit flows (if running remotely).

## 1. Configure the server URL

The `docker-compose.yaml` in the repository root uses a `HOST_IP` variable to
set the Prefect API and UI URLs. By default it falls back to `127.0.0.1`, which
only allows access from the Docker host itself.

If you need to reach the Prefect UI or API from **other machines on the
network** (e.g. workers or browsers on different hosts), set `HOST_IP` to the
host's LAN IP address or resolvable hostname.

### Option A — create a `.env` file (recommended)

Copy the provided example and edit the IP:

```bash
cd opticstream            # repository root
cp .env.example .env
```

Then open `.env` and replace the default value:

```dotenv
# Use the host's LAN IP so other machines can reach the Prefect server.
# Use 127.0.0.1 if you only access the server from the Docker host.
HOST_IP=192.168.1.100
```

Docker Compose automatically loads `.env` from the same directory as the
compose file, so no extra flags are needed.

### Option B — export the variable in your shell

```bash
export HOST_IP=192.168.1.100
```

This is useful for one-off launches or CI environments where you don't want a
persisted `.env` file.

### What the variable controls

Inside `docker-compose.yaml`, the `server` service references `HOST_IP` in two
environment variables:

```yaml
- PREFECT_UI_URL=http://${HOST_IP:-127.0.0.1}:4200/api
- PREFECT_API_URL=http://${HOST_IP:-127.0.0.1}:4200/api
```

`PREFECT_API_URL` tells Prefect clients where to send API requests, and
`PREFECT_UI_URL` tells the browser-based dashboard where to find the API
backend. Both must be reachable from whatever machine is connecting.

## 2. Start the Prefect server

From the repository root, bring up the server profile (which starts the
PostgreSQL database and the Prefect server):

```bash
docker compose --profile server up -d
```

This pulls the required images on first run (`postgres:alpine` and
`prefecthq/prefect:3-python3.12`), creates the `prefect-network` Docker
network, and starts both containers in detached mode.

### Verify the services are running

```bash
docker compose --profile server ps
```

You should see two containers (`database` and `server`) with status **Up**.

Check the server logs to confirm a healthy start:

```bash
docker compose --profile server logs server
```

Look for a line similar to:

```
Configure Prefect to communicate with the server with:

    prefect config set PREFECT_API_URL=http://…:4200/api
```

## 3. Open the Prefect dashboard

In a browser, navigate to:

```
http://<HOST_IP>:4200
```

Replace `<HOST_IP>` with the value you configured (or `127.0.0.1` / `localhost`
if accessing from the Docker host). You should see the Prefect UI dashboard.

## 4. Point local clients at the server

On any machine that will submit or monitor flows, tell Prefect where the server
lives:

```bash
prefect config set PREFECT_API_URL=http://<HOST_IP>:4200/api
```

Or set it as an environment variable in your shell:

```bash
export PREFECT_API_URL=http://<HOST_IP>:4200/api
```

You can verify the connection with:

```bash
prefect version   # prints client version
prefect server health  # should return healthy
```

## 5. Stopping and restarting

Stop the server (containers are removed but volumes persist):

```bash
docker compose --profile server down
```

Restart later with the same command used to start:

```bash
docker compose --profile server up -d
```

All flow-run history and configuration is stored in the `db` Docker volume and
survives restarts.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Browser cannot reach `http://<HOST_IP>:4200` | `HOST_IP` is wrong or firewall blocks port 4200 | Verify `HOST_IP` matches the host's actual LAN IP; open port 4200 in the firewall |
| `prefect server health` fails from another machine | `PREFECT_API_URL` on the client doesn't match `HOST_IP` | Re-run `prefect config set PREFECT_API_URL=…` with the correct address |
| Database connection errors in server logs | The `database` container isn't running | Run `docker compose --profile server up -d` and check `docker compose --profile server ps` |
| Containers exit immediately | Port 4200 or 5432 already in use on the host | Stop the conflicting service or change the port mapping in `docker-compose.yaml` |

## Next steps

- Continue with the [Quickstart](quickstart.md) to configure a scan and run
  your first flow.
- See [Configuration](configuration.md) for scan and project settings.
