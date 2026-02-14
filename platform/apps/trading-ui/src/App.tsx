import { FormEvent, useEffect, useMemo, useState } from "react";
import { ApiClient, defaultApiBaseUrl, discoverApiBaseUrl } from "./api";
import { SparklineChart } from "./components/SparklineChart";
import type {
  AuthLoginResponse,
  ClosePositionInput,
  HealthResponse,
  LiveGuardSnapshot,
  LiveGuardVenueState,
  OrchestratorSnapshot,
  PortfolioHistorySnapshot,
  PortfolioResponse,
  RecentOrderRecord,
  SocialClaim,
  SocialTagSignal
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

export default function App() {
  const [theme, setTheme] = useState<Theme>(() => {
    const saved = localStorage.getItem(THEME_STORAGE_KEY);
    return saved === "day" || saved === "night" ? saved : "night";
  });
  const [baseUrl, setBaseUrl] = useState<string>(defaultApiBaseUrl());
  const [baseUrlResolved, setBaseUrlResolved] = useState<boolean>(false);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [liveGuards, setLiveGuards] = useState<LiveGuardSnapshot | null>(null);
  const [portfolio, setPortfolio] = useState<PortfolioResponse | null>(null);
  const [history, setHistory] = useState<PortfolioHistorySnapshot[]>([]);
  const [orders, setOrders] = useState<RecentOrderRecord[]>([]);
  const [intelClaims, setIntelClaims] = useState<SocialClaim[]>([]);
  const [intelTags, setIntelTags] = useState<SocialTagSignal[]>([]);
  const [orchestrator, setOrchestrator] = useState<OrchestratorSnapshot | null>(null);
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
  const [busy, setBusy] = useState(false);
  const [guardBusy, setGuardBusy] = useState(false);
  const [automationBusy, setAutomationBusy] = useState(false);
  const [closeBusyKeys, setCloseBusyKeys] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [killSwitchReason, setKillSwitchReason] = useState("");
  const [strategyDraft, setStrategyDraft] = useState({
    name: "",
    venue: "coinbase",
    tags: "multi_agent,momentum",
    allocation: "5000"
  });
  const [activity, setActivity] = useState<ActivityItem[]>([]);
  const [streamStatus, setStreamStatus] = useState<"idle" | "connecting" | "live" | "error">("idle");

  const api = useMemo(() => new ApiClient(baseUrl, session?.accessToken), [baseUrl, session?.accessToken]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  }, [theme]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const discovered = await discoverApiBaseUrl();
      if (!cancelled) {
        setBaseUrl(discovered);
        setBaseUrlResolved(true);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (session) {
      localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(session));
    } else {
      localStorage.removeItem(SESSION_STORAGE_KEY);
    }
  }, [session]);

  useEffect(() => {
    if (!baseUrlResolved) return;
    void loadHealth();
    const intervalId = window.setInterval(() => {
      void loadHealth();
    }, 8000);
    return () => window.clearInterval(intervalId);
  }, [api, baseUrlResolved]);

  useEffect(() => {
    if (!baseUrlResolved) return;
    if (!session) {
      setPortfolio(null);
      setHistory([]);
      setOrders([]);
      setLiveGuards(null);
      setIntelClaims([]);
      setIntelTags([]);
      setOrchestrator(null);
      setStreamStatus("idle");
      return;
    }
    void loadProtectedData();
    const intervalId = window.setInterval(() => {
      void loadProtectedData();
    }, 10000);
    return () => window.clearInterval(intervalId);
  }, [session, api, baseUrlResolved]);

  useEffect(() => {
    if (!session || !baseUrlResolved) return;

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
        const [portfolioData, historyData, ordersData, guardsData, intelData, tagData, orchestratorData] =
          await Promise.all([
          api.portfolio(),
          api.portfolioHistory(40),
          api.recentOrders(25),
          api.liveGuards(),
          api.intelClaims(40),
          api.intelTags(24, 10),
          api.orchestrator()
        ]);
        setPortfolio(portfolioData);
        setHistory(historyData.snapshots);
        setOrders(ordersData.records);
        setLiveGuards(guardsData);
        setIntelClaims(intelData.claims);
        setIntelTags(tagData.tags);
        setOrchestrator(orchestratorData);
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

    source.addEventListener("execution_guard", () => {
      pushActivity("warn", "Execution guard state changed.");
      void refreshFromStream();
    });

    source.addEventListener("social_intel", () => {
      pushActivity("info", "Social intel updated.");
      void refreshFromStream();
    });

    source.addEventListener("strategy_orchestrator", () => {
      pushActivity("warn", "Strategy orchestrator updated.");
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
  }, [api, baseUrl, session, baseUrlResolved]);

  const cashValue = portfolio?.cash_usd ?? 0;
  const positions = portfolio?.positions ?? [];
  const activePositions = positions.filter((position) => Math.abs(position.quantity) > 0);
  const exposure = activePositions.reduce(
    (sum, position) =>
      sum +
      Math.abs(position.quantity * (position.market_price ?? position.mark_price ?? position.average_price ?? 0)),
    0
  );
  const unrealizedPnl = activePositions.reduce((sum, position) => sum + (position.unrealized_pnl ?? 0), 0);
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
      const [portfolioData, historyData, ordersData, guardData, intelData, tagData, orchestratorData] =
        await Promise.all([
        api.portfolio(),
        api.portfolioHistory(40),
        api.recentOrders(25),
        api.liveGuards(),
        api.intelClaims(50),
        api.intelTags(24, 12),
        api.orchestrator()
      ]);
      setPortfolio(portfolioData);
      setHistory(historyData.snapshots);
      setOrders(ordersData.records);
      setLiveGuards(guardData);
      setIntelClaims(intelData.claims);
      setIntelTags(tagData.tags);
      setOrchestrator(orchestratorData);
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

  async function handleKillSwitchToggle(enabled: boolean) {
    if (!session) {
      setError("Login required to control kill switch.");
      return;
    }
    setGuardBusy(true);
    try {
      const payload = {
        enabled,
        reason: enabled ? killSwitchReason.trim() || undefined : undefined
      };
      const snapshot = await api.setLiveKillSwitch(payload);
      setLiveGuards(snapshot);
      setError(null);
      appendActivity(enabled ? "warn" : "info", enabled ? "Global kill switch engaged." : "Global kill switch released.");
    } catch (err) {
      const message = errorMessage(err);
      setError(message);
      appendActivity("error", `Kill switch update failed: ${message}`);
    } finally {
      setGuardBusy(false);
    }
  }

  async function handleVenueGuardControl(
    venue: LiveGuardVenueState["venue"],
    liveEnabled?: boolean,
    resetBreaker = false
  ) {
    if (!session) {
      setError("Login required to control venue guards.");
      return;
    }
    setGuardBusy(true);
    try {
      const snapshot = await api.setVenueGuard(venue, {
        live_enabled: liveEnabled,
        reset_breaker: resetBreaker
      });
      setLiveGuards(snapshot);
      setError(null);
      const action = resetBreaker
        ? `${venue.toUpperCase()} breaker reset.`
        : `${venue.toUpperCase()} live ${liveEnabled ? "enabled" : "disabled"}.`;
      appendActivity("warn", action);
    } catch (err) {
      const message = errorMessage(err);
      setError(message);
      appendActivity("error", `Venue guard update failed: ${message}`);
    } finally {
      setGuardBusy(false);
    }
  }

  async function handleRunIntelScan() {
    if (!session) {
      setError("Login required to run social intel scans.");
      return;
    }
    setAutomationBusy(true);
    try {
      const result = await api.runIntelScan({
        lookback_days: 4,
        max_per_query: 20
      });
      const [claims, tags] = await Promise.all([api.intelClaims(50), api.intelTags(24, 12)]);
      setIntelClaims(claims.claims);
      setIntelTags(tags.tags);
      appendActivity("info", `Intel scan inserted ${result.inserted} claims.`);
      setError(null);
    } catch (err) {
      const message = errorMessage(err);
      setError(message);
      appendActivity("error", `Intel scan failed: ${message}`);
    } finally {
      setAutomationBusy(false);
    }
  }

  async function handleEvaluateOrchestrator() {
    if (!session) {
      setError("Login required to evaluate strategy orchestrator.");
      return;
    }
    setAutomationBusy(true);
    try {
      const snapshot = await api.evaluateOrchestrator(true);
      setOrchestrator(snapshot);
      appendActivity("warn", `Orchestrator evaluated. ${snapshot.last_actions.length} actions.`);
      setError(null);
    } catch (err) {
      const message = errorMessage(err);
      setError(message);
      appendActivity("error", `Orchestrator evaluation failed: ${message}`);
    } finally {
      setAutomationBusy(false);
    }
  }

  async function handleAddStrategyAgent(event: FormEvent) {
    event.preventDefault();
    if (!session) {
      setError("Login required to add strategy agents.");
      return;
    }
    const name = strategyDraft.name.trim();
    if (!name) {
      setError("Strategy name is required.");
      return;
    }
    const allocation = Number(strategyDraft.allocation);
    if (!Number.isFinite(allocation) || allocation <= 0) {
      setError("Allocation must be a positive number.");
      return;
    }

    const tags = strategyDraft.tags
      .split(",")
      .map((value) => value.trim())
      .filter((value) => value.length > 0);

    setAutomationBusy(true);
    try {
      await api.upsertStrategyAgent({
        name,
        venue: strategyDraft.venue,
        tags,
        allocation_usd: allocation,
        active: true
      });
      setStrategyDraft((prev) => ({ ...prev, name: "", tags: "multi_agent,momentum" }));
      await loadProtectedData();
      appendActivity("info", `Strategy agent added: ${name}`);
      setError(null);
    } catch (err) {
      const message = errorMessage(err);
      setError(message);
      appendActivity("error", `Strategy create failed: ${message}`);
    } finally {
      setAutomationBusy(false);
    }
  }

  async function handleClosePosition(position: PortfolioResponse["positions"][number]) {
    if (!session) {
      setError("Login required to close positions.");
      return;
    }
    const instrument = position.instrument;
    if (!instrument) {
      setError("Position instrument metadata missing; cannot close position.");
      return;
    }
    const venue = normalizeVenueCode(instrument.venue);
    if (!venue) {
      setError(`Unsupported venue for manual close: ${instrument.venue}`);
      return;
    }
    const symbol = instrument.symbol.trim();
    if (!symbol) {
      setError("Position symbol missing; cannot close position.");
      return;
    }

    const busyKey = `${venue}:${symbol}`;
    setCloseBusyKeys((prev) => Array.from(new Set([...prev, busyKey])));
    try {
      const payload: ClosePositionInput = {
        venue,
        symbol,
        mode: health?.execution_mode_default ?? "paper"
      };
      const response = await api.closePosition(payload);
      appendActivity(
        "warn",
        `Close ${symbol} -> ${response.ack.status}: ${response.ack.message}`
      );
      await loadProtectedData();
      setError(null);
    } catch (err) {
      const message = errorMessage(err);
      setError(message);
      appendActivity("error", `Close ${symbol} failed: ${message}`);
    } finally {
      setCloseBusyKeys((prev) => prev.filter((entry) => entry !== busyKey));
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
          <h2 className="panel-title">Backend</h2>
          <p className="mono">{baseUrlResolved ? baseUrl : "Detecting..."}</p>
          <p className="hint">Auto-discovered trading-server endpoint.</p>
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

        <div className="panel compact guard-panel">
          <div className="panel-header">
            <h2 className="panel-title">Live Execution Guards</h2>
            <span className={`pill ${liveGuards?.global.kill_switch_enabled ? "danger" : "good"}`}>
              {liveGuards?.global.kill_switch_enabled ? "Kill ON" : "Kill OFF"}
            </span>
          </div>
          {!session ? (
            <p className="hint">Sign in to view and manage venue execution guards.</p>
          ) : (
            <>
              <label className="field-label" htmlFor="kill-switch-reason">
                Kill Switch Reason
              </label>
              <input
                id="kill-switch-reason"
                className="field"
                value={killSwitchReason}
                onChange={(event) => setKillSwitchReason(event.target.value)}
                placeholder="Manual halt reason"
              />
              <div className="split two">
                <button
                  className="button danger"
                  type="button"
                  onClick={() => void handleKillSwitchToggle(true)}
                  disabled={guardBusy}
                >
                  Engage Kill
                </button>
                <button
                  className="button ghost"
                  type="button"
                  onClick={() => void handleKillSwitchToggle(false)}
                  disabled={guardBusy}
                >
                  Release
                </button>
              </div>
              <div className="guard-venue-list">
                {liveGuards?.venues.map((venueState) => (
                  <div className="guard-venue-row" key={venueState.venue}>
                    <div>
                      <strong>{venueState.venue.toUpperCase()}</strong>
                      <p className={`hint ${venueState.live_allowed_now ? "" : "danger-text"}`}>
                        {venueState.block_reason ?? "Live route open"}
                      </p>
                    </div>
                    <div className="guard-actions">
                      <span className={`pill small ${venueState.live_allowed_now ? "good" : "danger"}`}>
                        {venueState.live_allowed_now ? "open" : "blocked"}
                      </span>
                      <button
                        className="button tiny ghost"
                        type="button"
                        onClick={() =>
                          void handleVenueGuardControl(venueState.venue, !venueState.live_enabled, false)
                        }
                        disabled={guardBusy}
                      >
                        {venueState.live_enabled ? "Disable" : "Enable"}
                      </button>
                      <button
                        className="button tiny ghost"
                        type="button"
                        onClick={() => void handleVenueGuardControl(venueState.venue, undefined, true)}
                        disabled={guardBusy || (!venueState.breaker_open && venueState.consecutive_live_failures === 0)}
                      >
                        Reset
                      </button>
                    </div>
                  </div>
                ))}
                {!liveGuards && <p className="hint">Loading guard state...</p>}
              </div>
            </>
          )}
        </div>
      </aside>

      <main className="main-stage">
        <header className="top-bar">
          <div className="status-row">
            <span className={`pill ${health?.ok ? "good" : "warn"}`}>{health?.ok ? "Connected" : "Disconnected"}</span>
            <span className="pill neutral">Env: {health?.execution_mode_default ?? "unknown"}</span>
            <span className={`pill ${liveGuards?.global.kill_switch_enabled ? "danger" : "good"}`}>
              Kill: {liveGuards?.global.kill_switch_enabled ? "ENGAGED" : "OFF"}
            </span>
            <span className={`pill ${streamStatusTone(streamStatus)}`}>Stream: {streamStatus}</span>
            <span className="pill neutral">Intel: {health?.intel_claims ?? intelClaims.length}</span>
            <span className="pill neutral">Agents: {orchestrator?.agents.length ?? health?.orchestrator_agents ?? 0}</span>
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
                      <tr key={`${positionInstrument(position).venue}:${positionSymbol(position)}:${position.quantity}`}>
                        <td>{positionSymbol(position)}</td>
                        <td>{position.quantity.toFixed(3)}</td>
                        <td>${positionMarketPrice(position).toFixed(2)}</td>
                        <td className={(position.unrealized_pnl ?? 0) >= 0 ? "pos" : "neg"}>
                          {(position.unrealized_pnl ?? 0).toFixed(2)}
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
              <h3 className="panel-title">Open Trades</h3>
              <p className="hint">Agent-managed positions with manual force-close</p>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Venue</th>
                    <th>Symbol</th>
                    <th>Qty</th>
                    <th>Mark</th>
                    <th>UPnL</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {activePositions.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="empty-row">
                        No open trades
                      </td>
                    </tr>
                  ) : (
                    activePositions.map((position) => {
                      const instrument = positionInstrument(position);
                      const venue = normalizeVenueCode(instrument.venue);
                      const symbol = positionSymbol(position);
                      const busyKey = venue ? `${venue}:${symbol}` : "";
                      const isClosing = busyKey ? closeBusyKeys.includes(busyKey) : false;
                      return (
                        <tr key={`${instrument.venue}:${symbol}:${position.quantity}`}>
                          <td>{instrument.venue.toUpperCase()}</td>
                          <td>{symbol}</td>
                          <td>{position.quantity.toFixed(3)}</td>
                          <td>${positionMarketPrice(position).toFixed(2)}</td>
                          <td className={(position.unrealized_pnl ?? 0) >= 0 ? "pos" : "neg"}>
                            {(position.unrealized_pnl ?? 0).toFixed(2)}
                          </td>
                          <td>
                            <button
                              type="button"
                              className="button tiny danger"
                              disabled={!session || !venue || isClosing || automationBusy}
                              onClick={() => void handleClosePosition(position)}
                            >
                              {isClosing ? "Closing..." : "Close"}
                            </button>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </article>

          <article className="panel">
            <div className="panel-header">
              <h3 className="panel-title">Social Intel Scanner</h3>
              <p className="hint">Fresh crowd strategy claims (X + Reddit)</p>
            </div>
            <div className="split two">
              <button className="button ghost" onClick={() => void handleRunIntelScan()} disabled={!session || automationBusy}>
                {automationBusy ? "Scanning..." : "Run Scan"}
              </button>
              <button className="button ghost" onClick={() => void handleEvaluateOrchestrator()} disabled={!session || automationBusy}>
                Rank + Rebalance
              </button>
            </div>
            <div className="tag-cloud">
              {intelTags.length === 0 ? (
                <p className="hint">No tag signals yet.</p>
              ) : (
                intelTags.map((signal) => (
                  <span key={signal.tag} className="pill small neutral">
                    {signal.tag} ({signal.mentions})
                  </span>
                ))
              )}
            </div>
            <div className="activity-list mini">
              {intelClaims.length === 0 ? (
                <p className="hint">No captured claims.</p>
              ) : (
                intelClaims.slice(0, 5).map((claim) => (
                  <div key={claim.id} className="activity-item info">
                    <span className="mono">{claim.source.toUpperCase()}</span>
                    <span title={claim.text}>{claim.text.slice(0, 120)}{claim.text.length > 120 ? "..." : ""}</span>
                  </div>
                ))
              )}
            </div>
          </article>

          <article className="panel wide">
            <div className="panel-header">
              <h3 className="panel-title">Strategy Orchestrator</h3>
              <p className="hint">Promote top performers, kill worst performers</p>
            </div>
            <form className="strategy-form" onSubmit={handleAddStrategyAgent}>
              <div className="split two">
                <label>
                  Strategy Name
                  <input
                    className="field"
                    value={strategyDraft.name}
                    onChange={(event) => setStrategyDraft((prev) => ({ ...prev, name: event.target.value }))}
                    placeholder="Cross-venue momentum"
                  />
                </label>
                <label>
                  Venue
                  <select
                    className="field"
                    value={strategyDraft.venue}
                    onChange={(event) => setStrategyDraft((prev) => ({ ...prev, venue: event.target.value }))}
                  >
                    <option value="coinbase">Coinbase</option>
                    <option value="ibkr">IBKR</option>
                    <option value="kalshi">Kalshi</option>
                  </select>
                </label>
              </div>
              <div className="split two">
                <label>
                  Tags (comma separated)
                  <input
                    className="field"
                    value={strategyDraft.tags}
                    onChange={(event) => setStrategyDraft((prev) => ({ ...prev, tags: event.target.value }))}
                    placeholder="multi_agent,momentum,cross_venue"
                  />
                </label>
                <label>
                  Initial Allocation (USD)
                  <input
                    className="field"
                    inputMode="decimal"
                    value={strategyDraft.allocation}
                    onChange={(event) => setStrategyDraft((prev) => ({ ...prev, allocation: event.target.value }))}
                  />
                </label>
              </div>
              <button type="submit" className="button primary" disabled={!session || automationBusy}>
                Add Strategy Agent
              </button>
            </form>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Strategy</th>
                    <th>State</th>
                    <th>Score</th>
                    <th>Alloc</th>
                    <th>Sharpe</th>
                    <th>Ret%</th>
                    <th>DD%</th>
                    <th>Trades</th>
                  </tr>
                </thead>
                <tbody>
                  {!orchestrator || orchestrator.agents.length === 0 ? (
                    <tr>
                      <td colSpan={9} className="empty-row">
                        No strategy agents configured
                      </td>
                    </tr>
                  ) : (
                    orchestrator.agents.map((agent) => (
                      <tr key={agent.id}>
                        <td>{agent.rank > 1_000_000 ? "--" : agent.rank}</td>
                        <td title={agent.tags.join(", ")}>{agent.name}</td>
                        <td>
                          <span className={`pill small ${agent.lifecycle === "killed" ? "danger" : agent.lifecycle === "active" ? "good" : "warn"}`}>
                            {agent.lifecycle}
                          </span>
                        </td>
                        <td>{agent.score.toFixed(2)}</td>
                        <td>${agent.allocation_usd.toFixed(0)}</td>
                        <td>{agent.performance.sharpe.toFixed(2)}</td>
                        <td>{agent.performance.total_return_pct.toFixed(2)}</td>
                        <td>{agent.performance.max_drawdown_pct.toFixed(2)}</td>
                        <td>{agent.performance.trades}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
            {orchestrator && orchestrator.last_actions.length > 0 && (
              <div className="activity-list mini">
                {orchestrator.last_actions.slice(0, 5).map((action) => (
                  <div key={`${action.at}-${action.agent_id}-${action.action}`} className="activity-item warn">
                    <span className="mono">{new Date(action.at).toLocaleTimeString()}</span>
                    <span>
                      {action.action.toUpperCase()} {action.agent_name}: {action.reason}
                    </span>
                  </div>
                ))}
              </div>
            )}
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

function positionInstrument(position: PortfolioResponse["positions"][number]) {
  if (position.instrument) {
    return position.instrument;
  }
  return {
    venue: "unknown",
    symbol: position.symbol ?? "UNKNOWN",
    asset_class: "other",
    quote_currency: "USD"
  };
}

function positionSymbol(position: PortfolioResponse["positions"][number]): string {
  return positionInstrument(position).symbol;
}

function positionMarketPrice(position: PortfolioResponse["positions"][number]): number {
  return position.market_price ?? position.mark_price ?? position.average_price ?? 0;
}

function normalizeVenueCode(raw: string): ClosePositionInput["venue"] | null {
  const lowered = raw.trim().toLowerCase();
  if (lowered === "coinbase") return "coinbase";
  if (lowered === "ibkr") return "ibkr";
  if (lowered === "kalshi") return "kalshi";
  return null;
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
