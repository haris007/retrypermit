export type JsonRecord = Record<string, unknown>;

export class ApiError extends Error {
  readonly code: string;
  readonly traceId?: string;
  readonly retryable: boolean;
  readonly status: number;

  constructor(
    message: string,
    options: {
      code?: string;
      traceId?: string;
      retryable?: boolean;
      status: number;
    },
  ) {
    super(message);
    this.name = "ApiError";
    this.code = options.code ?? "REQUEST_FAILED";
    this.traceId = options.traceId;
    this.retryable = options.retryable ?? false;
    this.status = options.status;
  }
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value : undefined;
}

async function parseResponse(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text) return null;

  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

async function request(
  path: string,
  init: RequestInit = {},
  adminToken = "",
): Promise<unknown> {
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");

  if (init.body !== undefined && !(init.body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }

  if (adminToken.trim()) {
    const token = adminToken.trim();
    headers.set("X-Admin-Token", token);
  }

  let response: Response;
  try {
    response = await fetch(path, { ...init, headers });
  } catch (error) {
    throw new ApiError(
      error instanceof Error ? error.message : "Could not reach RetryPermit.",
      { code: "NETWORK_ERROR", retryable: true, status: 0 },
    );
  }

  const payload = await parseResponse(response);
  if (response.ok) return payload;

  const errorObject = isRecord(payload)
    ? isRecord(payload.error)
      ? payload.error
      : payload
    : {};

  const message =
    optionalString(errorObject.message) ??
    optionalString(errorObject.detail) ??
    (typeof payload === "string" ? payload : undefined) ??
    `Request failed with status ${response.status}.`;

  throw new ApiError(message, {
    code:
      optionalString(errorObject.code) ??
      optionalString(errorObject.error_code) ??
      "REQUEST_FAILED",
    traceId:
      optionalString(errorObject.trace_id) ?? optionalString(errorObject.traceId),
    retryable:
      typeof errorObject.retryable === "boolean"
        ? errorObject.retryable
        : response.status >= 500,
    status: response.status,
  });
}

export const api = {
  stats: () => request("/api/stats"),
  messages: () => request("/api/messages"),
  message: (messageId: string) =>
    request(`/api/messages/${encodeURIComponent(messageId)}`),
  transitions: (messageId: string) =>
    request(`/api/messages/${encodeURIComponent(messageId)}/transitions`),
  receipts: (messageId: string) =>
    request(`/api/messages/${encodeURIComponent(messageId)}/receipts`),
  proof: (messageId: string) =>
    request(`/api/messages/${encodeURIComponent(messageId)}/proof`),
  policies: () => request("/api/policies"),
  reset: (adminToken: string) =>
    request("/api/demo/reset", { method: "POST", body: "{}" }, adminToken),
  seed: (adminToken: string) =>
    request("/api/demo/seed", { method: "POST", body: "{}" }, adminToken),
  start: (adminToken: string) =>
    request("/api/demo/start", { method: "POST", body: "{}" }, adminToken),
  startWithFailure: (adminToken: string) =>
    request(
      "/api/demo/start-with-failure",
      { method: "POST", body: "{}" },
      adminToken,
    ),
  extractPolicy: (file: File, adminToken: string) => {
    const body = new FormData();
    body.set("file", file);
    return request("/api/policies/extract", { method: "POST", body }, adminToken);
  },
  approvePolicy: (version: string, adminToken: string) =>
    request(
      `/api/policies/${encodeURIComponent(version)}/approve`,
      { method: "POST", body: JSON.stringify({ actor: "retrypermit-demo-admin" }) },
      adminToken,
    ),
  activatePolicy: (version: string, adminToken: string) =>
    request(
      `/api/policies/${encodeURIComponent(version)}/activate`,
      { method: "POST", body: JSON.stringify({ actor: "retrypermit-demo-admin" }) },
      adminToken,
    ),
};

export function record(value: unknown): JsonRecord {
  return isRecord(value) ? value : {};
}

export function list(value: unknown, keys: string[] = []): unknown[] {
  if (Array.isArray(value)) return value;
  if (!isRecord(value)) return [];

  for (const key of keys) {
    const candidate = value[key];
    if (Array.isArray(candidate)) return candidate;
  }

  const data = value.data;
  return Array.isArray(data) ? data : [];
}

export function first(
  source: JsonRecord,
  keys: string[],
  fallback?: unknown,
): unknown {
  for (const key of keys) {
    if (source[key] !== undefined && source[key] !== null) return source[key];
  }
  return fallback;
}

export function stringValue(value: unknown, fallback = "—"): string {
  if (typeof value === "string" && value.trim()) return value;
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return fallback;
}

export function numberValue(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
}
