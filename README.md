# Polycopy – Polymarket Copy Trading Bot

Polycopy is a fail-fast, resilient MVP bot that mirrors a target trader’s positions on Polymarket with configurable risk controls and sub-second detection-to-submit latency (target best ~100ms, worst-case <=1s for bot processing). It combines a best-effort RTDS WebSocket fast-path with an HTTP polling backstop plus periodic reconciliation to keep your portfolio aligned.

## How it works
- **Config & startup checks**: Typed settings from `.env` + CLI overrides. On boot the bot validates required secrets, Data-API reachability, and basic clock sanity before running.
- **Signal ingestion**:
  - **RTDS WebSocket** (`activity/trades`) for fast detection with heartbeat, backoff, and circuit breaker.
  - **HTTP backstop** polls `/trades?user=<target>` every second (configurable) with high-watermark dedupe.
- **State & dedupe**: Positions cached in memory; intents stored in SQLite for idempotent order submission (target_tx_hash + intent key).
- **Risk & execution**: COPY_FACTOR scaling, per-trade/market/portfolio notional caps, blacklists, min size, and optional slippage cap. Execution posts marketable limit orders through the CLOB REST API (dry-run/paper modes available).
- **Reconciliation**: Periodic full position refresh (default 30s) corrects drift.
- **Ops**: Structured JSON logs, bounded queues, kill switch on repeated failures.

## Setup
1. Install dependencies:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and fill in credentials (private key, wallets). API credentials are derived automatically from your private key; run `python -m polycopy.credentials --env-format` if you need to persist them.

## Running
```bash
python -m polycopy.main               # start the bot
python -m polycopy.main --dry-run     # log intended orders only
python -m polycopy.main --paper       # simulate fills
python -m polycopy.main --healthcheck # run startup checks then exit
python -m polycopy.live_view          # live terminal view fed by dry-run/paper shared memory
```

### Make targets
- `make install` – install dependencies
- `make test` – run unit tests (dedupe + risk controls)
- `make run` – start the bot
- `make healthcheck` – fail-fast preflight

## Configuration (.env)
See `.env.example` for required keys. Notable settings:
- `SIGNATURE_TYPE` controls CLOB signing: `1` for Email/Magic proxy (default), `2` for Web3 browser wallets (Metamask/Coinbase, etc.).
- `COPY_FACTOR` (0–1) scales target sizes.
- `HTTP_POLL_INTERVAL`, `RECONCILE_INTERVAL`, `QUEUE_MAXSIZE`.
- Risk caps: `MAX_NOTIONAL_PER_TRADE`, `MAX_NOTIONAL_PER_MARKET`, `MAX_PORTFOLIO_EXPOSURE`, `MIN_TRADE_SIZE`, `SLIPPAGE_BPS`.
- Blacklists: comma-separated `BLACKLIST_MARKETS`, `BLACKLIST_OUTCOMES`.

## Fail-fast philosophy
- Startup exits non-zero if required env vars are missing, Data-API is unreachable, or clock skew is excessive.
- Circuit breaker disables WS fast-path after repeated failures; HTTP backstop continues.
- Kill switch stops trading after too many consecutive processing errors.

## Deployment notes
- Container-friendly Dockerfile included. Example systemd unit:
  ```
  [Unit]
  Description=Polycopy Bot
  After=network.target

  [Service]
  WorkingDirectory=/opt/polycopy
  ExecStart=/opt/polycopy/.venv/bin/python -m polycopy.main
  Restart=on-failure
  EnvironmentFile=/opt/polycopy/.env

  [Install]
  WantedBy=multi-user.target
  ```

## Troubleshooting
- **Healthcheck fails**: verify `.env` values and network egress to `data-api.polymarket.com`.
- **Queue full warnings**: increase `QUEUE_MAXSIZE` or reduce poll interval.
- **Kill switch triggered**: inspect logs for upstream errors, fix, then restart manually.

In dry-run or paper modes the bot writes the target wallet's trades and positions to CSV files (`target_trades.csv`, `target_positions.csv`) alongside the SQLite state file for easy offline review and deduplicated logging. It also populates a shared memory segment read by `python -m polycopy.live_view`, which renders target vs simulated positions and trade/order waterfalls. Raw logs are streamed to the terminal and mirrored to `polycopy.log` at the project root.
