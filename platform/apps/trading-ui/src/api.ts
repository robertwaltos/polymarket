import type {
  AuthLoginResponse,
  HealthResponse,
  LiveGuardSnapshot,
  LiveKillSwitchInput,
  PlaceOrderInput,
  PlaceOrderResponse,
  PortfolioHistoryResponse,
  PortfolioResponse,
  RecentOrdersResponse,
  VenueGuardControlInput
} from "./types";

export class ApiClient {
  private readonly baseUrl: string;
  private readonly accessToken?: string;

  constructor(baseUrl: string, accessToken?: string) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.accessToken = accessToken;
  }

  withToken(accessToken?: string): ApiClient {
    return new ApiClient(this.baseUrl, accessToken);
  }

  async health(): Promise<HealthResponse> {
    return this.request<HealthResponse>("/v1/health");
  }

  async login(email: string, password: string): Promise<AuthLoginResponse> {
    return this.request<AuthLoginResponse>("/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password })
    });
  }

  async refresh(refreshToken: string): Promise<AuthLoginResponse> {
    return this.request<AuthLoginResponse>("/v1/auth/refresh", {
      method: "POST",
      body: JSON.stringify({ refresh_token: refreshToken })
    });
  }

  async portfolio(): Promise<PortfolioResponse> {
    return this.request<PortfolioResponse>("/v1/portfolio", { method: "GET" }, true);
  }

  async portfolioHistory(limit = 48): Promise<PortfolioHistoryResponse> {
    return this.request<PortfolioHistoryResponse>(
      `/v1/portfolio/history?limit=${encodeURIComponent(limit)}`,
      { method: "GET" },
      true
    );
  }

  async recentOrders(limit = 30): Promise<RecentOrdersResponse> {
    return this.request<RecentOrdersResponse>(
      `/v1/orders/recent?limit=${encodeURIComponent(limit)}`,
      { method: "GET" },
      true
    );
  }

  async liveGuards(): Promise<LiveGuardSnapshot> {
    return this.request<LiveGuardSnapshot>("/v1/execution/guards", { method: "GET" }, true);
  }

  async setLiveKillSwitch(payload: LiveKillSwitchInput): Promise<LiveGuardSnapshot> {
    return this.request<LiveGuardSnapshot>(
      "/v1/execution/guards/kill-switch",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      true
    );
  }

  async setVenueGuard(
    venue: "coinbase" | "ibkr" | "kalshi",
    payload: VenueGuardControlInput
  ): Promise<LiveGuardSnapshot> {
    return this.request<LiveGuardSnapshot>(
      `/v1/execution/guards/venues/${encodeURIComponent(venue)}`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      true
    );
  }

  async placeOrder(payload: PlaceOrderInput): Promise<PlaceOrderResponse> {
    return this.request<PlaceOrderResponse>(
      "/v1/orders",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      true
    );
  }

  private async request<T>(path: string, init?: RequestInit, auth = false): Promise<T> {
    const headers = new Headers(init?.headers);
    if (!headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }
    if (auth) {
      if (!this.accessToken) {
        throw new Error("missing access token");
      }
      headers.set("Authorization", `Bearer ${this.accessToken}`);
    }

    const response = await fetch(`${this.baseUrl}${path}`, {
      ...init,
      headers
    });

    const text = await response.text();
    const payload = text.length > 0 ? safeJsonParse(text) : null;
    if (!response.ok) {
      if (payload && typeof payload === "object" && "error" in payload) {
        throw new Error(String((payload as { error: unknown }).error));
      }
      throw new Error(`HTTP ${response.status}`);
    }
    return payload as T;
  }
}

function safeJsonParse(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

export function defaultApiBaseUrl(): string {
  return import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8080";
}
