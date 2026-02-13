import { FormEvent, useEffect, useMemo, useState } from "react";
import { ApiClient, defaultApiBaseUrl } from "./api";
import { SparklineChart } from "./components/SparklineChart";
import type {
  AuthLoginResponse,
  ExecutionMode,
  HealthResponse,
  PlaceOrderInput,
  PortfolioHistorySnapshot,
  PortfolioResponse,
  RecentOrderRecord
} from "./types";

type Theme = "night" | "day";

interface SessionState {
  email: string;
  accessToken: string;
  refreshToken: string;
  expiresAtEpochMs: number;
}

interface ActivityItem {
  id: string;
  level: "info" | "warn" | "error";
  message: string;
  at: string;
}

const SESSION_STORAGE_KEY = "atlas_trade_ui_session";
const THEME_STORAGE_KEY = "atlas_trade_ui_theme";
const BASE_URL_STORAGE_KEY = "atlas_trade_ui_base_url";

const defaultOrderDraft = {
  venue: "coinbase" as PlaceOrderInput["venue"],
  symbol: "BTC-USD",
  side: "buy" as PlaceOrderInput["side"],
  orderType: "limit" as PlaceOrderInput["order_type"],
  quantity: "0.10",
  limitPrice: "100",
  mode: "paper" as ExecutionMode
};

export default function App() {
  const [theme, setTheme] = useState<Theme>(() => {
    const saved = localStorage.getItem(THEME_STORAGE_KEY);
    return saved === "day" || saved === "night" ? saved : "night";
  });
  const [baseUrl, setBaseUrl] = useState<string>(() => localStorage.getItem(BASE_URL_STORAGE_KEY) ?? defaultApiBaseUrl());
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [portfolio, setPortfolio] = useState<PortfolioResponse | null>(null);
  const [history, setHistory] = useState<PortfolioHistorySnapshot[]>([]);
  const [orders, setOrders] = useState<RecentOrderRecord[]>([]);
  const [session, setSession] = useState<SessionState | null>(() => {
    const raw = localStorage.getItem(SESSION_STORAGE_KEY);
    if (!raw) return null;
    try {
      return JSON.parse(raw) as SessionState;
    } catch {
      return null;
    }
  });
  const [credentials, setCredentials] = useState({
    email: "admin@localhost",
    password: ""
  });
  const [orderDraft, setOrderDraft] = useState(defaultOrderDraft);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activity, setActivity] = useState<ActivityItem[]>([]);
  const [streamStatus, setStreamStatus] = useState<"idle" | "connecting" | "live" | "error">("idle");

  const api = useMemo(() => new ApiClient(baseUrl, session?.accessToken), [baseUrl, session?.accessToken]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  }, [theme]);

  useEffect(() => {
    localStorage.setItem(BASE_URL_STORAGE_KEY, baseUrl);
  }, [baseUrl]);

  useEffect(() => {
    if (session) {
      localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(session));
    } else {
      localStorage.removeItem(SESSION_STORAGE_KEY);
    }
  }, [session]);

  useEffect(() => {
    void loadHealth();
    const intervalId = window.setInterval(() => {
      void loadHealth();
    }, 8000);
    return () => window.clearInterval(intervalId);
  }, [api]);

  useEffect(() => {
    if (!session) {
      setPortfolio(null);
      setHistory([]);
      setOrders([]);
      setStreamStatus("idle");
      return;
    }
    void loadProtectedData();
    const intervalId = window.setInterval(() => {
      void loadProtectedData();
    }, 10000);
    return () => window.clearInterval(intervalId);
  }, [session, api]);

  useEffect(() => {
    if (!session) return;

    const streamBase = baseUrl.replace(/\/+$/, "");
    const streamUrl = `${streamBase}/v1/stream?token=${encodeURIComponent(session.accessToken)}`;
    const source = new EventSource(streamUrl);
    let emittedError = false;

    setStreamStatus("connecting");

    const pushActivity = (level: ActivityItem["level"], message: string) => {
      setActivity((prev) => [
        {
          id: crypto.randomUUID(),
          level,
          message,
          at: new Date().toLocaleTimeString()
        },
        ...prev
      ].slice(0, 14));
    };

    const refreshFromStream = async () => {
      try {
        const [portfolioData, historyData, ordersData] = await Promise.all([
          api.portfolio(),
          api.portfolioHistory(40),
          api.recentOrders(25)
        ]);
        setPortfolio(portfolioData);
        setHistory(historyData.snapshots);
        setOrders(ordersData.records);
      } catch {
        // polling loop continues as fallback
      }
    };

    source.addEventListener("open", () => {
      emittedError = false;
      setStreamStatus("live");
      pushActivity("info", "Realtime stream connected.");
    });

    source.addEventListener("order_ack", (event) => {
      const payload = parseStreamEnvelope((event as MessageEvent<string>).data);
      const requestId =
        payload && typeof payload === "object" && "payload" in payload
          ? String((payload as { payload?: { request_id?: unknown } }).payload?.request_id ?? "")
          : "";
      pushActivity("info", `Realtime order update ${requestId ? requestId.slice(0, 8) : ""}`.trim());
      void refreshFromStream();
    });

    source.addEventListener("portfolio_snapshot", () => {
      pushActivity("info", "Realtime portfolio snapshot received.");
      void refreshFromStream();
    });

    source.addEventListener("system", (event) => {
      const payload = parseStreamEnvelope((event as MessageEvent<string>).data);
      if (
        payload &&
        typeof payload === "object" &&
        "payload" in payload &&
        (payload as { payload?: { message?: unknown } }).payload?.message === "stream_connected"
      ) {
        pushActivity("info", "Stream authenticated.");
      }
    });

    source.onerror = () => {
      setStreamStatus("error");
      if (!emittedError) {
        pushActivity("warn", "Realtime stream disconnected. Falling back to polling.");
        emittedError = true;
      }
    };

    return () => {
      source.close();
    };
  }, [api, baseUrl, session]);

  const cashValue = portfolio?.cash_usd ?? 0;
  const positions = portfolio?.positions ?? [];
  const activePositions = positions.filter((position) => Math.abs(position.quantity) > 0);
  const exposure = activePositions.reduce(
    (sum, position) => sum + Math.abs(position.quantity * (position.mark_price || position.average_price || 0)),
    0
  );
  const unrealizedPnl = activePositions.reduce((sum, position) => sum + (position.unrealized_pnl || 0), 0);
  const sparklinePoints = useMemo(() => buildSparklinePoints(history, cashValue), [history, cashValue]);

  async function loadHealth() {
    try {
      const data = await api.health();
      setHealth(data);
      setError(null);
    } catch (err) {
      setError(errorMessage(err));
    }
  }

  async function loadProtectedData() {
    if (!session) return;
    try {
      const [portfolioData, historyData, ordersData] = await Promise.all([
        api.portfolio(),
        api.portfolioHistory(40),
        api.recentOrders(25)
      ]);
      setPortfolio(portfolioData);
      setHistory(historyData.snapshots);
      setOrders(ordersData.records);
      setError(null);
    } catch (err) {
      const message = errorMessage(err);
      if (message.toLowerCase().includes("401") || message.toLowerCase().includes("unauthorized")) {
        const refreshed = await tryRefreshSession(session);
        if (refreshed) {
          appendActivity("info", "Session refreshed.");
          return;
        }
      }
      setError(message);
      appendActivity("error", `Data refresh failed: ${message}`);
    }
  }

  async function tryRefreshSession(current: SessionState): Promise<boolean> {
    try {
      const refreshed = await api.refresh(current.refreshToken);
      setSession(buildSession(current.email, refreshed));
      return true;
    } catch {
      setSession(null);
      appendActivity("warn", "Session expired. Please log in again.");
      return false;
    }
  }

  async function handleLogin(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    try {
      const response = await api.login(credentials.email.trim(), credentials.password);
      setSession(buildSession(credentials.email.trim(), response));
      setCredentials((prev) => ({ ...prev, password: "" }));
      setError(null);
      appendActivity("info", "Authenticated.");
      await loadProtectedData();
    } catch (err) {
      setError(errorMessage(err));
      appendActivity("error", `Login failed: ${errorMessage(err)}`);
    } finally {
      setBusy(false);
    }
  }

  function handleLogout() {
    setSession(null);
    appendActivity("warn", "Session cleared.");
  }

  async function handleManualRefresh() {
    setBusy(true);
    try {
      await Promise.all([loadHealth(), loadProtectedData()]);
      appendActivity("info", "Manual refresh completed.");
    } finally {
      setBusy(false);
    }
  }

  async function handleSubmitOrder(event: FormEvent) {
    event.preventDefault();
    if (!session) {
      setError("Login required to submit orders.");
      return;
    }
    const quantity = Number(orderDraft.quantity);
    const limitPrice = Number(orderDraft.limitPrice);
    if (!Number.isFinite(quantity) || quantity <= 0) {
      setError("Quantity must be a positive number.");
      return;
    }
    if (orderDraft.orderType === "limit" && (!Number.isFinite(limitPrice) || limitPrice <= 0)) {
      setError("Limit price must be a positive number.");
      return;
    }

    setBusy(true);
    try {
      const payload: PlaceOrderInput = {
        venue: orderDraft.venue,
        symbol: orderDraft.symbol.trim(),
        side: orderDraft.side,
        order_type: orderDraft.orderType,
        quantity,
        mode: orderDraft.mode
      };
      if (orderDraft.orderType === "limit") {
        payload.limit_price = limitPrice;
      }
      const response = await api.placeOrder(payload);
      appendActivity(
        "info",
        `Order ${response.ack.request_id.slice(0, 8)} ${response.ack.status}: ${response.ack.message}`
      );
      await loadProtectedData();
    } catch (err) {
      const message = errorMessage(err);
      setError(message);
      appendActivity("error", `Order rejected: ${message}`);
    } finally {
      setBusy(false);
    }
  }

  function appendActivity(level: ActivityItem["level"], message: string) {
    setActivity((prev) => [
      {
        id: crypto.randomUUID(),
        level,
        message,
        at: new Date().toLocaleTimeString()
      },
      ...prev
    ].slice(0, 14));
  }

  return (
    <div className="app-shell">
      <div className="orb orb-a" />
      <div className="orb orb-b" />

      <aside className="left-rail">
        <div className="logo-wrap">
          <div className="logo-mark">AT</div>
          <div>
            <p className="eyebrow">Trading Console</p>
            <h1 className="product-title">Atlas Terminal</h1>
          </div>
        </div>

        <div className="panel compact">
          <label className="field-label" htmlFor="api-base-url">
            API Base URL
          </label>
          <input
            id="api-base-url"
            className="field"
            value={baseUrl}
            onChange={(event) => setBaseUrl(event.target.value)}
            placeholder="http://127.0.0.1:8080"
          />
          <p className="hint">Point this UI at your local or remote trading-server.</p>
        </div>

        {!session ? (
          <form className="panel compact" onSubmit={handleLogin}>
            <h2 className="panel-title">Authenticate</h2>
            <label className="field-label" htmlFor="email">
              Email
            </label>
            <input
              id="email"
              className="field"
              value={credentials.email}
              onChange={(event) => setCredentials((prev) => ({ ...prev, email: event.target.value }))}
              autoComplete="username"
            />
            <label className="field-label" htmlFor="password">
              Password
            </label>
            <input
              id="password"
              className="field"
              type="password"
              value={credentials.password}
              onChange={(event) => setCredentials((prev) => ({ ...prev, password: event.target.value }))}
              autoComplete="current-password"
            />
            <button type="submit" className="button primary" disabled={busy}>
              {busy ? "Connecting..." : "Sign In"}
            </button>
          </form>
        ) : (
          <div className="panel compact">
            <h2 className="panel-title">Session</h2>
            <p className="mono">{session.email}</p>
            <p className="hint">Token expires {new Date(session.expiresAtEpochMs).toLocaleTimeString()}</p>
            <button className="button ghost" onClick={handleLogout}>
              Sign Out
            </button>
          </div>
        )}

        <div className="panel compact">
          <h2 className="panel-title">System Snapshot</h2>
          <ul className="metric-list">
            <li>
              <span>Default mode</span>
              <strong>{health?.execution_mode_default ?? "--"}</strong>
            </li>
            <li>
              <span>Audit rows</span>
              <strong>{health?.audit_recent_count ?? 0}</strong>
            </li>
            <li>
              <span>Database</span>
              <strong>{health?.db_enabled ? "online" : "offline"}</strong>
            </li>
          </ul>
        </div>
      </aside>

      <main className="main-stage">
        <header className="top-bar">
          <div className="status-row">
            <span className={`pill ${health?.ok ? "good" : "warn"}`}>{health?.ok ? "Connected" : "Disconnected"}</span>
            <span className="pill neutral">Env: {health?.execution_mode_default ?? "unknown"}</span>
            <span className={`pill ${streamStatusTone(streamStatus)}`}>Stream: {streamStatus}</span>
            {error && <span className="pill danger">Issue: {error}</span>}
          </div>
          <div className="actions-row">
            <button className="button ghost" onClick={handleManualRefresh} disabled={busy}>
              Refresh
            </button>
            <button
              className="button ghost"
              onClick={() => setTheme((prev) => (prev === "night" ? "day" : "night"))}
            >
              {theme === "night" ? "Day Theme" : "Night Theme"}
            </button>
          </div>
        </header>

        <section className="hero-strip">
          <div>
            <p className="eyebrow">Global risk cockpit</p>
            <h2 className="hero-title">Execution-grade observability for autonomous trading</h2>
            <p className="hero-copy">
              Built for low-latency operations with paper, shadow, and live mode continuity.
            </p>
          </div>
          <div className="hero-stats">
            <StatCard label="Cash" value={`$${cashValue.toFixed(2)}`} />
            <StatCard label="Exposure" value={`$${exposure.toFixed(2)}`} />
            <StatCard
              label="Unrealized PnL"
              value={`${unrealizedPnl >= 0 ? "+" : ""}$${unrealizedPnl.toFixed(2)}`}
              accent={unrealizedPnl >= 0 ? "positive" : "negative"}
            />
          </div>
        </section>

        <section className="grid">
          <article className="panel chart-panel">
            <div className="panel-header">
              <h3 className="panel-title">Bankroll Curve</h3>
              <p className="hint">Portfolio snapshots over time</p>
            </div>
            <SparklineChart points={sparklinePoints} />
          </article>

          <article className="panel">
            <div className="panel-header">
              <h3 className="panel-title">Positions</h3>
              <p className="hint">{portfolio?.source ? `Source: ${portfolio.source}` : "Waiting for data"}</p>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Qty</th>
                    <th>Mark</th>
                    <th>UPnL</th>
                  </tr>
                </thead>
                <tbody>
                  {activePositions.length === 0 ? (
                    <tr>
                      <td colSpan={4} className="empty-row">
                        No active positions
                      </td>
                    </tr>
                  ) : (
                    activePositions.map((position) => (
                      <tr key={`${position.symbol}-${position.quantity}`}>
                        <td>{position.symbol}</td>
                        <td>{position.quantity.toFixed(3)}</td>
                        <td>${(position.mark_price || 0).toFixed(2)}</td>
                        <td className={position.unrealized_pnl >= 0 ? "pos" : "neg"}>
                          {(position.unrealized_pnl || 0).toFixed(2)}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </article>

          <article className="panel">
            <div className="panel-header">
              <h3 className="panel-title">Order Ticket</h3>
              <p className="hint">Low-friction manual intervention</p>
            </div>
            <form className="order-form" onSubmit={handleSubmitOrder}>
              <div className="split two">
                <label>
                  Venue
                  <select
                    className="field"
                    value={orderDraft.venue}
                    onChange={(event) =>
                      setOrderDraft((prev) => ({
                        ...prev,
                        venue: event.target.value as PlaceOrderInput["venue"]
                      }))
                    }
                  >
                    <option value="coinbase">Coinbase</option>
                    <option value="ibkr">IBKR</option>
                    <option value="kalshi">Kalshi</option>
                  </select>
                </label>
                <label>
                  Mode
                  <select
                    className="field"
                    value={orderDraft.mode}
                    onChange={(event) =>
                      setOrderDraft((prev) => ({
                        ...prev,
                        mode: event.target.value as ExecutionMode
                      }))
                    }
                  >
                    <option value="paper">Paper</option>
                    <option value="shadow">Shadow</option>
                    <option value="live">Live</option>
                  </select>
                </label>
              </div>
              <label>
                Symbol
                <input
                  className="field"
                  value={orderDraft.symbol}
                  onChange={(event) => setOrderDraft((prev) => ({ ...prev, symbol: event.target.value }))}
                />
              </label>
              <div className="split three">
                <label>
                  Side
                  <select
                    className="field"
                    value={orderDraft.side}
                    onChange={(event) =>
                      setOrderDraft((prev) => ({
                        ...prev,
                        side: event.target.value as PlaceOrderInput["side"]
                      }))
                    }
                  >
                    <option value="buy">Buy</option>
                    <option value="sell">Sell</option>
                  </select>
                </label>
                <label>
                  Type
                  <select
                    className="field"
                    value={orderDraft.orderType}
                    onChange={(event) =>
                      setOrderDraft((prev) => ({
                        ...prev,
                        orderType: event.target.value as PlaceOrderInput["order_type"]
                      }))
                    }
                  >
                    <option value="limit">Limit</option>
                    <option value="market">Market</option>
                  </select>
                </label>
                <label>
                  Quantity
                  <input
                    className="field"
                    inputMode="decimal"
                    value={orderDraft.quantity}
                    onChange={(event) => setOrderDraft((prev) => ({ ...prev, quantity: event.target.value }))}
                  />
                </label>
              </div>
              {orderDraft.orderType === "limit" && (
                <label>
                  Limit Price
                  <input
                    className="field"
                    inputMode="decimal"
                    value={orderDraft.limitPrice}
                    onChange={(event) => setOrderDraft((prev) => ({ ...prev, limitPrice: event.target.value }))}
                  />
                </label>
              )}
              <button type="submit" className="button primary" disabled={busy || !session}>
                {busy ? "Submitting..." : "Submit Order"}
              </button>
            </form>
          </article>

          <article className="panel wide">
            <div className="panel-header">
              <h3 className="panel-title">Recent Orders</h3>
              <p className="hint">Most recent execution events from audit trail</p>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Market</th>
                    <th>Side</th>
                    <th>Qty</th>
                    <th>Price</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {orders.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="empty-row">
                        No order activity
                      </td>
                    </tr>
                  ) : (
                    orders.map((record) => (
                      <tr key={record.order.request_id}>
                        <td className="mono">{new Date(record.order.submitted_at).toLocaleTimeString()}</td>
                        <td>{record.order.instrument.symbol}</td>
                        <td className="mono">{record.order.side.toUpperCase()}</td>
                        <td>{record.order.quantity.toFixed(3)}</td>
                        <td>{record.order.limit_price ? `$${record.order.limit_price.toFixed(2)}` : "--"}</td>
                        <td>
                          <span className={`pill small ${statusTone(record.ack.status)}`}>{record.ack.status}</span>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </article>

          <article className="panel">
            <div className="panel-header">
              <h3 className="panel-title">Activity Stream</h3>
              <p className="hint">Operator-facing runbook events</p>
            </div>
            <div className="activity-list">
              {activity.length === 0 ? (
                <p className="hint">No events yet.</p>
              ) : (
                activity.map((entry) => (
                  <div key={entry.id} className={`activity-item ${entry.level}`}>
                    <span className="mono">{entry.at}</span>
                    <span>{entry.message}</span>
                  </div>
                ))
              )}
            </div>
          </article>
        </section>
      </main>
    </div>
  );
}

function buildSession(email: string, auth: AuthLoginResponse): SessionState {
  return {
    email,
    accessToken: auth.access_token,
    refreshToken: auth.refresh_token,
    expiresAtEpochMs: Date.now() + auth.expires_in * 1000
  };
}

function buildSparklinePoints(history: PortfolioHistorySnapshot[], cashFallback: number) {
  if (history.length === 0) {
    return [
      { label: "Now", value: cashFallback || 0 },
      { label: "Now", value: cashFallback || 0 }
    ];
  }
  return history.map((snapshot) => ({
    label: new Date(snapshot.created_at).toLocaleTimeString(),
    value: snapshot.cash_usd
  }));
}

function errorMessage(value: unknown): string {
  if (value instanceof Error) return value.message;
  if (typeof value === "string") return value;
  return "Unknown error";
}

function parseStreamEnvelope(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function statusTone(status: string): "good" | "warn" | "danger" | "neutral" {
  if (status === "accepted" || status === "simulated") return "good";
  if (status === "rejected") return "danger";
  return "neutral";
}

function streamStatusTone(status: "idle" | "connecting" | "live" | "error"): "neutral" | "warn" | "good" | "danger" {
  switch (status) {
    case "live":
      return "good";
    case "connecting":
      return "warn";
    case "error":
      return "danger";
    default:
      return "neutral";
  }
}

function StatCard({
  label,
  value,
  accent = "neutral"
}: {
  label: string;
  value: string;
  accent?: "neutral" | "positive" | "negative";
}) {
  return (
    <div className={`stat-card ${accent}`}>
      <p>{label}</p>
      <strong>{value}</strong>
    </div>
  );
}
