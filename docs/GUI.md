# Atlas Terminal GUI

## Overview

`platform/apps/trading-ui` is a React + Vite operator console for the trading platform.

Core features:
- live health status (`/v1/health`)
- live server events (`/v1/stream` SSE)
- login/refresh-backed authenticated session
- portfolio + history + recent order polling
- manual order ticket for paper/shadow/live mode requests
- activity stream, risk metrics, and bankroll curve chart
- day/night theme toggle with persistent local state

## Run Backend

```powershell
cd D:\PythonProjects\polymarket\platform
cargo run -p trading-server -- serve
```

## Run GUI

```powershell
cd D:\PythonProjects\polymarket\platform\apps\trading-ui
Copy-Item .env.example .env -Force
npm install
npm run dev
```

By default the GUI expects:
- `VITE_API_BASE_URL=http://127.0.0.1:8080`
- backend stream buffer is configurable with `STREAM_EVENT_BUFFER` in `platform/.env`.

## Build GUI

```powershell
cd D:\PythonProjects\polymarket\platform\apps\trading-ui
npm run check
npm run build
```

## Desktop Packaging (Tauri)

The GUI includes Tauri v2 desktop scaffolding at `platform/apps/trading-ui/src-tauri`.

```powershell
cd D:\PythonProjects\polymarket\platform\apps\trading-ui
npm run tauri:dev
```

```powershell
cd D:\PythonProjects\polymarket\platform\apps\trading-ui
npm run tauri:build
```

Debug binary only (skip installer bundling):

```powershell
cd D:\PythonProjects\polymarket\platform\apps\trading-ui
npm run tauri:build:nobundle
```

Windows build artifacts:
- `platform/apps/trading-ui/src-tauri/target/release/bundle/nsis/*.exe`
- `platform/apps/trading-ui/src-tauri/target/release/bundle/msi/*.msi`
