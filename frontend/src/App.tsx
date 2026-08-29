import {
  type FormEvent,
  type ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  ApiError,
  api,
  first,
  list,
  numberValue,
  record,
  stringValue,
  type JsonRecord,
} from "./api";

type ActionName =
  | "reset"
  | "seed"
  | "start"
  | "startWithFailure"
  | "extractPolicy"
  | "approvePolicy"
  | "activatePolicy"
  | "prove";

interface MessageSummary {
  id: string;
  state: string;
  orderId: string;
  amount: number;
  currency: string;
  failureClass: string;
  raw: JsonRecord;
}

interface Transition {
  id: string;
  sequence: number;
  from: string;
  to: string;
  at: string;
  reason: string;
}

interface Receipt {
  id: string;
  type: string;
  outcome: string;
  phase: "planned" | "attempted" | "confirmed" | "failed";
  errorCode: string;
  at: string;
  detail: unknown;
}

interface PolicySummary {
  version: string;
  status: string;
  source: string;
  cap: number | null;
  sourceHash: string;
  extractedAt: string;
  clauses: unknown[];
  raw: JsonRecord;
}

interface DiffItem {
  path: string;
  before: unknown;
  after: unknown;
}

const STATE_ORDER = [
  "RECEIVED",
  "TRIAGING",
  "TRIAGED",
  "PLANNED",
  "DEFERRED",
  "RECHECKING",
  "REPAIRING",
  "REPLAYING",
  "REPLAYED",
];

const STATUS_LABELS: Record<string, string> = {
  RECEIVED: "Received",
  TRIAGING: "Triaging",
  TRIAGED: "Triaged",
  PLANNED: "Planned",
  REPAIRING: "Repairing",
  REPLAYING: "Replaying",
  REPLAYED: "Confirmed",
  CONFIRMED: "Confirmed",
  ESCALATED: "Escalated",
  DEFERRED: "Deferred",
  RECHECKING: "Rechecking",
  QUARANTINED: "Quarantined",
  FAILED_RETRYABLE: "Retryable failure",
  FAILED_FINAL: "Failed",
};

function normalizeState(value: unknown): string {
  return stringValue(value, "UNKNOWN").trim().toUpperCase().replaceAll(" ", "_");
}

function stateLabel(state: string): string {
  return STATUS_LABELS[state] ?? state.toLowerCase().replaceAll("_", " ");
}

function stateTone(state: string): string {
  if (state === "REPLAYED" || state === "CONFIRMED") return "success";
  if (state === "QUARANTINED") return "security";
  if (state.includes("FAIL") || state === "ESCALATED") return "danger";
  if (state === "DEFERRED" || state === "RECHECKING") return "warning";
  if (state === "TRIAGING" || state === "TRIAGED" || state === "PLANNED") {
    return "active";
  }
  return "neutral";
}

function receiptPhase(type: string, outcome: string): Receipt["phase"] {
  const normalizedOutcome = normalizeState(outcome);
  if (normalizedOutcome === "FAILED" || normalizedOutcome === "REJECTED") return "failed";
  if (type === "task_schedule_attempt") return "planned";
  if (normalizedOutcome === "STARTED" || normalizedOutcome === "IN_PROGRESS") return "attempted";
  return "confirmed";
}

function receiptIcon(phase: Receipt["phase"]): string {
  return { planned: "P", attempted: "A", confirmed: "✓", failed: "!" }[phase];
}

function unwrapRecord(value: unknown, keys: string[]): JsonRecord {
  const base = record(value);
  for (const key of keys) {
    const nested = record(base[key]);
    if (Object.keys(nested).length) return nested;
  }
  return base;
}

function normalizeMessage(value: unknown, index: number): MessageSummary {
  const raw = record(value);
  const payload = unwrapRecord(first(raw, ["original_payload", "payload", "raw_payload"]), []);
  const id = stringValue(
    first(raw, ["message_id", "messageId", "id"]),
    `message-${index + 1}`,
  );
  const orderId = stringValue(
    first(raw, ["order_id", "orderId"], first(payload, ["order_id", "orderId"])),
    id,
  );

  return {
    id,
    state: normalizeState(first(raw, ["current_state", "currentState", "state", "status"])),
    orderId,
    amount: numberValue(first(raw, ["amount"], first(payload, ["amount"])), 0),
    currency: stringValue(
      first(raw, ["currency"], first(payload, ["currency"])),
      "USD",
    ),
    failureClass: stringValue(first(raw, ["failure_class", "failureClass"]), "pending"),
    raw,
  };
}

function normalizeTransition(value: unknown, index: number): Transition {
  const raw = record(value);
  const to = normalizeState(
    first(raw, ["new_state", "newState", "to_state", "toState", "state", "status"]),
  );
  return {
    id: stringValue(first(raw, ["transition_id", "transitionId", "id"]), `transition-${index}`),
    sequence: numberValue(first(raw, ["sequence", "seq"]), index + 1),
    from: normalizeState(
      first(raw, ["previous_state", "previousState", "from_state", "fromState"], ""),
    ),
    to,
    at: stringValue(first(raw, ["created_at", "createdAt", "timestamp", "at"]), ""),
    reason: stringValue(
      first(raw, ["triggering_event", "triggeringEvent", "reason", "summary", "event"]),
      "State recorded",
    ),
  };
}

function normalizeReceipt(value: unknown, index: number): Receipt {
  const raw = record(value);
  const knownKeys = new Set([
    "receipt_id",
    "receiptId",
    "id",
    "receipt_type",
    "receiptType",
    "type",
    "tool_name",
    "toolName",
    "kind",
    "outcome",
    "status",
    "result",
    "created_at",
    "createdAt",
    "timestamp",
    "at",
  ]);
  const fallbackDetail = Object.fromEntries(
    Object.entries(raw).filter(([key]) => !knownKeys.has(key)),
  );

  const type = stringValue(
    first(raw, ["receipt_type", "receiptType", "type", "tool_name", "toolName", "kind"]),
    "operation",
  );
  const outcome = stringValue(first(raw, ["outcome", "status", "result"]), "recorded");
  return {
    id: stringValue(first(raw, ["receipt_id", "receiptId", "id"]), `receipt-${index}`),
    type,
    outcome,
    phase: receiptPhase(type, outcome),
    errorCode: stringValue(first(raw, ["error_code", "errorCode"]), ""),
    at: stringValue(first(raw, ["created_at", "createdAt", "timestamp", "at"]), ""),
    detail: first(raw, ["details", "detail", "payload", "data"], fallbackDetail),
  };
}

function normalizePolicy(value: unknown): PolicySummary {
  const raw = record(value);
  const definition = record(first(raw, ["definition", "policy"]));
  const capValue = first(
    raw,
    ["effective_replay_cap", "effectiveReplayCap", "auto_replay_cap", "autoReplayCap"],
    first(definition, ["effective_replay_cap", "effectiveReplayCap", "auto_replay_cap", "autoReplayCap"]),
  );
  return {
    version: stringValue(
      first(
        raw,
        ["version", "policy_version", "policyVersion", "id"],
        first(definition, ["version", "policy_version", "policyVersion", "id"]),
      ),
    ),
    status: normalizeState(first(raw, ["status", "approval_status", "approvalStatus"])),
    source: stringValue(
      first(raw, ["source_kind", "sourceKind", "source", "provider", "source_type", "sourceType"]),
      "Seeded JSON",
    ),
    cap: capValue === undefined || capValue === null ? null : numberValue(capValue),
    sourceHash: stringValue(first(raw, ["source_hash", "sourceHash"]), "Pending"),
    extractedAt: stringValue(first(raw, ["extracted_at", "extractedAt"]), ""),
    clauses: list(first(definition, ["clauses"]), ["items"]),
    raw,
  };
}

function displayJson(value: unknown): string {
  if (value === undefined || value === null) return "No payload recorded yet.";
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function flatten(value: unknown, prefix = ""): Map<string, unknown> {
  const result = new Map<string, unknown>();
  if (Array.isArray(value)) {
    value.forEach((item, index) => {
      const path = `${prefix}[${index}]`;
      const nested = flatten(item, path);
      if (nested.size) nested.forEach((entry, key) => result.set(key, entry));
      else result.set(path, item);
    });
    return result;
  }

  if (typeof value === "object" && value !== null) {
    Object.entries(value).forEach(([key, item]) => {
      const path = prefix ? `${prefix}.${key}` : key;
      const nested = flatten(item, path);
      if (nested.size) nested.forEach((entry, nestedKey) => result.set(nestedKey, entry));
      else result.set(path, item);
    });
    return result;
  }

  if (prefix) result.set(prefix, value);
  return result;
}

function computeDiff(before: unknown, after: unknown): DiffItem[] {
  const left = flatten(before);
  const right = flatten(after);
  const paths = [...new Set([...left.keys(), ...right.keys()])].sort();
  return paths
    .filter((path) => JSON.stringify(left.get(path)) !== JSON.stringify(right.get(path)))
    .map((path) => ({ path, before: left.get(path), after: right.get(path) }));
}

function formatMoment(value: string): string {
  if (!value) return "Pending";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

function formatElapsed(seconds: number): string {
  if (seconds <= 0) return "0s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = Math.round(seconds % 60);
  return `${minutes}m ${remainder}s`;
}

function formatAmount(amount: number, currency: string): string {
  try {
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency,
      maximumFractionDigits: 2,
    }).format(amount);
  } catch {
    return `${currency} ${amount.toFixed(2)}`;
  }
}

function compactValue(value: unknown): string {
  if (value === undefined) return "∅";
  if (typeof value === "string") return value;
  return displayJson(value).replaceAll("\n", " ");
}

function StatusPill({ state }: { state: string }) {
  return (
    <span className={`status-pill status-${stateTone(state)}`}>
      <span className="status-dot" aria-hidden="true" />
      {stateLabel(state)}
    </span>
  );
}

function Panel({
  title,
  kicker,
  action,
  children,
  className = "",
}: {
  title: string;
  kicker?: string;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`panel ${className}`}>
      <header className="panel-header">
        <div>
          {kicker ? <p className="eyebrow">{kicker}</p> : null}
          <h2>{title}</h2>
        </div>
        {action}
      </header>
      {children}
    </section>
  );
}

function EmptyState({ children }: { children: ReactNode }) {
  return (
    <div className="empty-state">
      <span className="empty-mark" aria-hidden="true">R/</span>
      <p>{children}</p>
    </div>
  );
}

export default function App() {
  const [statsPayload, setStatsPayload] = useState<unknown>({});
  const [messagePayload, setMessagePayload] = useState<unknown[]>([]);
  const [detailPayload, setDetailPayload] = useState<unknown>({});
  const [transitionPayload, setTransitionPayload] = useState<unknown[]>([]);
  const [receiptPayload, setReceiptPayload] = useState<unknown[]>([]);
  const [policyPayload, setPolicyPayload] = useState<unknown[]>([]);
  const [proofPayload, setProofPayload] = useState<unknown>({});
  const [selectedPolicyVersion, setSelectedPolicyVersion] = useState("");
  const [selectedId, setSelectedId] = useState("");
  const [adminToken, setAdminToken] = useState("");
  const [showToken, setShowToken] = useState(false);
  const [busyAction, setBusyAction] = useState<ActionName | null>(null);
  const [polling, setPolling] = useState(true);
  const [lastSyncedAt, setLastSyncedAt] = useState<Date | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const pollInFlight = useRef(false);

  const stats = record(statsPayload);
  const messages = useMemo(
    () => messagePayload.map(normalizeMessage),
    [messagePayload],
  );
  const transitions = useMemo(
    () => transitionPayload.map(normalizeTransition).sort((a, b) => a.sequence - b.sequence),
    [transitionPayload],
  );
  const receipts = useMemo(
    () => receiptPayload.map(normalizeReceipt),
    [receiptPayload],
  );
  const policies = useMemo(
    () => policyPayload.map(normalizePolicy),
    [policyPayload],
  );

  const selectedSummary = messages.find((message) => message.id === selectedId);
  const detail = {
    ...(selectedSummary?.raw ?? {}),
    ...unwrapRecord(detailPayload, ["message", "data"]),
  };

  const currentState = normalizeState(
    first(detail, ["current_state", "currentState", "state", "status"], selectedSummary?.state),
  );
  const originalPayload = first(detail, ["original_payload", "originalPayload", "payload", "raw_payload"]);
  const repairedPayload = first(detail, ["repaired_payload", "repairedPayload", "validated_payload", "validatedPayload"]);
  const hasRepairedPayload = repairedPayload !== undefined && repairedPayload !== null;
  const payloadDiff = useMemo(
    () => (hasRepairedPayload ? computeDiff(originalPayload, repairedPayload) : []),
    [hasRepairedPayload, originalPayload, repairedPayload],
  );

  const activePolicy =
    policies.find((policy) => policy.status === "ACTIVE") ?? policies[0];
  const mode = stringValue(
    first(stats, ["mode", "demo_mode", "demoMode", "store_mode", "storeMode"]),
    "Local",
  );
  const isCloudMode = mode === "Google Cloud" || stats.cloud_mode === true;
  const activePolicyVersion = stringValue(
    first(stats, ["active_policy_version", "activePolicyVersion"], activePolicy?.version),
    "Not loaded",
  );
  const selectedPolicy =
    policies.find((policy) => policy.version === selectedPolicyVersion) ?? activePolicy;
  const proof = unwrapRecord(proofPayload, ["proof", "data"]);
  const total = numberValue(first(stats, ["total", "total_messages", "totalMessages"]), messages.length);
  const resolved = numberValue(
    first(stats, ["resolved", "resolved_count", "resolvedCount", "replayed"]),
    messages.filter((message) => message.state === "REPLAYED" || message.state === "CONFIRMED").length,
  );
  const deferredTotal = numberValue(
    first(stats, ["deferred_total", "deferredTotal"]),
    messages.filter((message) => message.failureClass === "transient_downstream").length,
  );
  const quarantinedTotal = numberValue(
    first(stats, ["quarantined_count", "quarantinedCount"]),
    messages.filter((message) => message.state === "QUARANTINED").length,
  );
  const escalatedTotal = numberValue(
    first(stats, ["escalated_count", "escalatedCount"]),
    messages.filter((message) => message.state === "ESCALATED").length,
  );
  const dlqDepth = numberValue(
    first(stats, ["dlq_depth", "dlqDepth", "pending", "pending_count", "pendingCount"]),
    Math.max(total - resolved, 0),
  );
  const elapsed = numberValue(first(stats, ["elapsed_seconds", "elapsedSeconds", "elapsed"]));
  const runId = stringValue(first(stats, ["run_id", "runId", "current_run_id", "currentRunId"]), "No active run");

  const refreshSelected = useCallback(async (messageId: string) => {
    if (!messageId) return;
    const results = await Promise.allSettled([
      api.message(messageId),
      api.transitions(messageId),
      api.receipts(messageId),
    ]);

    const [messageResult, transitionResult, receiptResult] = results;
    if (messageResult?.status === "fulfilled") setDetailPayload(messageResult.value);
    if (transitionResult?.status === "fulfilled") {
      setTransitionPayload(list(transitionResult.value, ["transitions", "items"]));
    }
    if (receiptResult?.status === "fulfilled") {
      setReceiptPayload(list(receiptResult.value, ["receipts", "items"]));
    }

    const rejected = results.find(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );
    if (rejected && rejected.reason instanceof ApiError) throw rejected.reason;
  }, []);

  const refresh = useCallback(async () => {
    if (pollInFlight.current) return;
    pollInFlight.current = true;

    try {
      const [statsResult, messagesResult, policiesResult] = await Promise.all([
        api.stats(),
        api.messages(),
        api.policies(),
      ]);
      const nextMessages = list(messagesResult, ["messages", "items"]);
      const policyEnvelope = record(policiesResult);
      const policyItems = list(policiesResult, ["policies", "items", "versions"]);
      const explicitActivePolicy = policyEnvelope.active_policy ?? policyEnvelope.activePolicy;
      const nextPolicies =
        policyItems.length > 0
          ? policyItems
          : explicitActivePolicy && typeof explicitActivePolicy === "object"
            ? [explicitActivePolicy]
            : [];
      setStatsPayload(statsResult);
      setMessagePayload(nextMessages);
      setPolicyPayload(nextPolicies);
      if (!selectedPolicyVersion && nextPolicies.length) {
        const normalizedPolicies = nextPolicies.map(normalizePolicy);
        setSelectedPolicyVersion(
          normalizedPolicies.find((policy) => policy.status === "ACTIVE")?.version ??
            normalizedPolicies[0]?.version ??
            "",
        );
      }

      const normalized = nextMessages.map(normalizeMessage);
      const nextSelected =
        normalized.find((message) => message.id === selectedId)?.id ??
        normalized[0]?.id ??
        "";
      if (nextSelected !== selectedId) setSelectedId(nextSelected);
      if (nextSelected) await refreshSelected(nextSelected);

      setLastSyncedAt(new Date());
      setError(null);
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause
          : new ApiError(
              cause instanceof Error ? cause.message : "Unexpected refresh error.",
              { status: 0, retryable: true },
            ),
      );
    } finally {
      pollInFlight.current = false;
    }
  }, [refreshSelected, selectedId, selectedPolicyVersion]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    if (!polling) return;
    const timer = window.setInterval(() => void refresh(), 1800);
    return () => window.clearInterval(timer);
  }, [polling, refresh]);

  useEffect(() => {
    if (!selectedId) {
      setDetailPayload({});
      setTransitionPayload([]);
      setReceiptPayload([]);
      return;
    }

    void refreshSelected(selectedId).catch((cause: unknown) => {
      if (cause instanceof ApiError) setError(cause);
    });
  }, [refreshSelected, selectedId]);

  async function runAction(action: ActionName) {
    setBusyAction(action);
    setError(null);
    try {
      if (action === "reset" || action === "seed" || action === "start") {
        await api[action](adminToken);
      } else if (action === "startWithFailure") {
        await api.startWithFailure(adminToken);
      } else {
        throw new ApiError("This action needs a selected item.", { status: 400 });
      }
      if (action === "reset") {
        setSelectedId("");
        setDetailPayload({});
        setTransitionPayload([]);
        setReceiptPayload([]);
        setProofPayload({});
      }
      await refresh();
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause
          : new ApiError("The control action failed.", { status: 0 }),
      );
    } finally {
      setBusyAction(null);
    }
  }

  async function proveSelected() {
    if (!selectedId) return;
    setBusyAction("prove");
    setError(null);
    try {
      setProofPayload(await api.proof(selectedId));
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause
          : new ApiError("The live proof query failed.", { status: 0 }),
      );
    } finally {
      setBusyAction(null);
    }
  }

  async function uploadPolicy(file: File) {
    setBusyAction("extractPolicy");
    setError(null);
    try {
      const result = await api.extractPolicy(file, adminToken);
      const extracted = unwrapRecord(result, ["policy"]);
      setSelectedPolicyVersion(
        stringValue(first(unwrapRecord(extracted, ["definition"]), ["version"]), ""),
      );
      await refresh();
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause
          : new ApiError("Policy extraction failed.", { status: 0 }),
      );
    } finally {
      setBusyAction(null);
    }
  }

  async function mutatePolicy(action: "approvePolicy" | "activatePolicy") {
    if (!selectedPolicy?.version) return;
    setBusyAction(action);
    setError(null);
    try {
      if (action === "approvePolicy") {
        await api.approvePolicy(selectedPolicy.version, adminToken);
      } else {
        await api.activatePolicy(selectedPolicy.version, adminToken);
      }
      await refresh();
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause
          : new ApiError("Policy lifecycle action failed.", { status: 0 }),
      );
    } finally {
      setBusyAction(null);
    }
  }

  function submitAction(event: FormEvent<HTMLFormElement>, action: ActionName) {
    event.preventDefault();
    void runAction(action);
  }

  const repairs = list(first(detail, ["applied_repairs", "appliedRepairs", "repairs", "proposed_repairs"]), ["items"]);
  const confidence = numberValue(first(detail, ["confidence"]), -1);
  const classification = stringValue(first(detail, ["failure_class", "failureClass", "classification"]), "Pending classification");
  const decision = stringValue(first(detail, ["decision_summary", "decisionSummary", "decision", "recommended_action"]), "No decision recorded yet.");
  const clause = stringValue(first(detail, ["runbook_clause", "runbookClause", "clause"]), "Pending citation");
  const page = stringValue(first(detail, ["runbook_page", "runbookPage", "page"]), "—");
  const idempotencyKey = stringValue(first(detail, ["idempotency_key", "idempotencyKey"]), "Not reserved");
  const downstreamReference = stringValue(first(detail, ["downstream_reference", "downstreamReference", "downstream_ref"]), "Awaiting confirmation");
  const proposedFix = stringValue(first(detail, ["proposed_fix", "proposedFix"]), "No proposed fix recorded.");
  const withheldReason = stringValue(first(detail, ["withheld_reason", "withheldReason"]), "Replay is withheld.");
  const severity = stringValue(first(detail, ["severity"]), "—");
  const sla = stringValue(first(detail, ["sla"]), "—");
  const escalationRecipient = stringValue(first(detail, ["escalation_recipient", "escalationRecipient"]), "—");
  const quarantineReason = stringValue(first(detail, ["quarantine_reason", "quarantineReason"]), "Payload instruction refused.");
  const deferralReason = stringValue(first(detail, ["deferral_reason", "deferralReason"]), "Awaiting dependency recovery.");
  const recheckAt = stringValue(first(detail, ["recheck_at", "recheckAt"]), "—");
  const recheckAttempts = numberValue(first(detail, ["recheck_attempts", "recheckAttempts"]));
  const demoRecheckSeconds = numberValue(first(stats, ["demo_recheck_seconds", "demoRecheckSeconds"]), 10);
  const attemptCount = numberValue(first(detail, ["attempts", "attempt_count", "attemptCount"]));
  const injectionDetected = first(detail, ["contains_injection_attempt", "containsInjectionAttempt"]) === true;
  const errorCodes = [...new Set([
    stringValue(first(detail, ["last_error_code", "lastErrorCode"]), ""),
    ...receipts.map((receipt) => receipt.errorCode),
  ].filter(Boolean))];

  return (
    <div className="app-shell min-h-screen antialiased">
      <header className="masthead">
        <a className="brand" href="#top" aria-label="RetryPermit home">
          <span className="brand-mark" aria-hidden="true">R/</span>
          <span>
            <strong>RetryPermit</strong>
            <small>Policy-governed recovery</small>
          </span>
        </a>

        <div className="masthead-meta">
          <span className={`mode-badge ${isCloudMode ? "mode-cloud" : "mode-local"}`}>
            <span className="status-dot" aria-hidden="true" />
            {isCloudMode ? "Google Cloud" : "Local / in-memory"}
          </span>
          <button
            type="button"
            className="sync-button"
            onClick={() => setPolling((value) => !value)}
            aria-pressed={!polling}
          >
            {polling ? "Live polling" : "Polling paused"}
          </button>
          <span className="sync-time">
            {lastSyncedAt ? `Synced ${formatMoment(lastSyncedAt.toISOString())}` : "Connecting…"}
          </span>
        </div>
      </header>

      <main id="top">
        <section className="hero-band" aria-labelledby="page-title">
          <div>
            <p className="eyebrow">Dead-letter operations console</p>
            <h1 id="page-title">Recovery that earns permission.</h1>
            <p className="hero-copy">
              {isCloudMode
                ? "Gemini proposes. "
                : "A deterministic fixture proposes in local mode. "}
              Deterministic policy validates. Every external effect leaves a receipt.
            </p>
          </div>
          <div className="disclosure-stack" aria-label="Demo disclosures">
            <span>Synthetic data</span>
            <span>Simulated downstream</span>
            <span>Demo mode</span>
          </div>
        </section>

        {error ? (
          <div className="error-banner" role="alert">
            <div>
              <strong>{error.code}</strong>
              <span>{error.message}</span>
            </div>
            <div className="error-meta">
              {error.traceId ? <span>Trace {error.traceId}</span> : null}
              <span>{error.retryable ? "Retryable" : `HTTP ${error.status || "offline"}`}</span>
              <button type="button" onClick={() => void refresh()}>Retry now</button>
            </div>
          </div>
        ) : null}

        <section className="overview-grid" aria-label="Run overview">
          <article className="metric metric-primary">
            <span>DLQ depth</span>
            <strong>{dlqDepth}</strong>
            <small>{total ? `${total} messages in this run` : "Waiting for seed"}</small>
          </article>
          <article className="metric">
            <span>Auto-resolved</span>
            <strong>{resolved}<i>/{total || 12}</i></strong>
            <small>Confirmed business effects</small>
          </article>
          <article className="metric metric-deferred">
            <span>Deferred</span>
            <strong>{deferredTotal}</strong>
            <small>Scheduled dependency rechecks</small>
          </article>
          <article className="metric metric-security">
            <span>Quarantined</span>
            <strong>{quarantinedTotal}</strong>
            <small>Security refusals</small>
          </article>
          <article className="metric metric-escalated">
            <span>Escalated</span>
            <strong>{escalatedTotal}</strong>
            <small>Human confirmation required</small>
          </article>
          <article className="metric">
            <span>Elapsed</span>
            <strong>{formatElapsed(elapsed)}</strong>
            <small>Current scenario runtime</small>
          </article>
          <article className="metric metric-policy">
            <span>Active policy</span>
            <strong>{activePolicyVersion}</strong>
            <small>{activePolicy?.source ?? "Validated, approved policy only"}</small>
          </article>
          <article className="run-meta-card">
            <span>Active run</span>
            <code title={runId}>{runId}</code>
            <span className="contract-note">At-least-once delivery · effectively-once effects</span>
          </article>
        </section>

        <Panel title="Demo controls" kicker="Administrator access" className="control-panel">
          <form className="control-form" onSubmit={(event) => submitAction(event, "start")}>
            <label className="token-field">
              <span>Admin token</span>
              <span className="token-input-wrap">
                <input
                  type={showToken ? "text" : "password"}
                  value={adminToken}
                  onChange={(event) => setAdminToken(event.target.value)}
                  autoComplete="off"
                  spellCheck={false}
                  placeholder="Required for protected controls"
                />
                <button type="button" onClick={() => setShowToken((value) => !value)}>
                  {showToken ? "Hide" : "Show"}
                </button>
              </span>
            </label>
            <p className="token-help">Held in memory only. Required for every protected control.</p>
            <div className="control-actions">
              <button
                className="button button-quiet"
                type="button"
                disabled={busyAction !== null}
                onClick={() => void runAction("reset")}
              >
                {busyAction === "reset" ? "Resetting…" : "Reset"}
              </button>
              <button
                className="button button-secondary"
                type="button"
                disabled={busyAction !== null}
                onClick={() => void runAction("seed")}
              >
                {busyAction === "seed" ? "Seeding…" : "Seed 12"}
              </button>
              <button className="button button-primary" type="submit" disabled={busyAction !== null}>
                {busyAction === "start" ? "Starting…" : "Start run"}
                <span aria-hidden="true">→</span>
              </button>
              <button
                className="button button-danger"
                type="button"
                disabled={busyAction !== null}
                onClick={() => void runAction("startWithFailure")}
              >
                {busyAction === "startWithFailure"
                  ? "Injecting failure…"
                  : "Start with lost response"}
              </button>
            </div>
            <p className="failure-disclosure">
              The injected path commits one simulated downstream effect and then
              returns a deliberate 503. RetryPermit retries the same idempotency key
              and must receive the original reference.
            </p>
            <div className="policy-controls">
              <label>
                <span>Runbook version</span>
                <select
                  value={selectedPolicy?.version ?? ""}
                  onChange={(event) => setSelectedPolicyVersion(event.target.value)}
                >
                  {policies.map((policy) => (
                    <option key={policy.version} value={policy.version}>
                      {policy.version} · {policy.status} · cap ${policy.cap ?? "—"}
                    </option>
                  ))}
                </select>
              </label>
              <label className="button button-quiet file-button">
                {busyAction === "extractPolicy" ? "Extracting PDF…" : "Rebuild from PDF"}
                <input
                  type="file"
                  accept="application/pdf,.pdf"
                  disabled={busyAction !== null}
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    if (file) void uploadPolicy(file);
                    event.target.value = "";
                  }}
                />
              </label>
              <button
                className="button button-secondary"
                type="button"
                disabled={busyAction !== null || selectedPolicy?.status !== "PENDING_APPROVAL"}
                onClick={() => void mutatePolicy("approvePolicy")}
              >
                {busyAction === "approvePolicy" ? "Approving…" : "Approve seeded policy"}
              </button>
              <button
                className="button button-primary"
                type="button"
                disabled={
                  busyAction !== null ||
                  !selectedPolicy ||
                  !["APPROVED", "ACTIVE"].includes(selectedPolicy.status)
                }
                onClick={() => void mutatePolicy("activatePolicy")}
              >
                {busyAction === "activatePolicy" ? "Activating…" : "Activate selected"}
              </button>
            </div>
            {selectedPolicy ? (
              <div className="policy-evidence">
                <span>
                  <strong>{selectedPolicy.status}</strong> · effective cap ${selectedPolicy.cap ?? "—"}
                </span>
                <code title={selectedPolicy.sourceHash}>
                  Source hash {selectedPolicy.sourceHash.slice(0, 16)}…
                </code>
                <small>
                  Extracted {formatMoment(selectedPolicy.extractedAt)} · {selectedPolicy.clauses.length} validated clauses
                </small>
              </div>
            ) : null}
          </form>
        </Panel>

        <section className="workspace-grid">
          <Panel
            title="Messages"
            kicker={`${messages.length || 0} synthetic orders`}
            action={<span className="panel-live"><i /> Live</span>}
            className="message-panel"
          >
            {messages.length ? (
              <div className="message-list" role="list" aria-label="Run messages">
                {messages.map((message, index) => {
                  const progressIndex = STATE_ORDER.indexOf(message.state);
                  const progress = progressIndex < 0 ? 0 : (progressIndex / (STATE_ORDER.length - 1)) * 100;
                  return (
                    <button
                      type="button"
                      role="listitem"
                      key={message.id}
                      className={`message-chip chip-${stateTone(message.state)} ${message.id === selectedId ? "is-selected" : ""}`}
                      onClick={() => setSelectedId(message.id)}
                      aria-pressed={message.id === selectedId}
                    >
                      <span className="message-index">{String(index + 1).padStart(2, "0")}</span>
                      <span className="message-body">
                        <span className="message-title-row">
                          <strong>{message.orderId}</strong>
                          <StatusPill state={message.state} />
                        </span>
                        <span className="message-amount">
                          {formatAmount(message.amount, message.currency)} · {message.failureClass === "schema_drift"
                            ? "Schema v3 drift"
                            : message.failureClass === "transient_downstream"
                              ? "Inventory 503"
                              : message.failureClass === "invalid_data"
                                ? "Invalid business data"
                                : message.failureClass === "prompt_injection"
                                  ? "Untrusted instruction"
                                  : "Awaiting triage"}
                        </span>
                        <span className="progress-track" aria-hidden="true">
                          <i style={{ width: `${progress}%` }} />
                        </span>
                      </span>
                    </button>
                  );
                })}
              </div>
            ) : (
              <EmptyState>Seed the 12 synthetic messages across Classes A–D to begin.</EmptyState>
            )}
          </Panel>

          <div className="detail-column">
            <Panel
              title={selectedSummary?.orderId ?? "Message detail"}
              kicker={selectedId ? `Message ${selectedId}` : "Select a message"}
              action={selectedId ? <StatusPill state={currentState} /> : undefined}
              className="decision-panel"
            >
              {selectedId ? (
                <>
                  <div className="decision-grid">
                    <div>
                      <span className="field-label">Classification</span>
                      <strong className="classification">{classification.replaceAll("_", " ")}</strong>
                    </div>
                    <div>
                      <span className="field-label">Confidence</span>
                      <strong className="confidence">
                        {confidence >= 0 ? `${Math.round(confidence * 100)}%` : "Pending"}
                      </strong>
                    </div>
                    <div className="decision-summary">
                      <span className="field-label">Validated decision</span>
                      <p>{decision}</p>
                    </div>
                  </div>
                  <div className="clause-callout">
                    <span className="clause-icon" aria-hidden="true">§</span>
                    <div>
                      <span className="field-label">Authorising runbook clause</span>
                      <strong>{clause}</strong>
                      <small>Page {page} · active policy {activePolicyVersion}</small>
                    </div>
                  </div>
                  {injectionDetected ? (
                    <div className="injection-warning" role="alert">
                      <strong>Injection warning</strong>
                      <span>Untrusted payload instructions were refused and rendered as inert text.</span>
                    </div>
                  ) : null}
                  <div className="execution-facts" aria-label="Execution facts">
                    <div><span>Attempts</span><strong>{attemptCount + recheckAttempts}</strong></div>
                    <div><span>Error codes</span><code>{errorCodes.join(", ") || "none"}</code></div>
                    <div><span>Idempotency</span><code>{idempotencyKey === "Not reserved" ? idempotencyKey : `${idempotencyKey.slice(0, 18)}…`}</code></div>
                    <div><span>Downstream</span><code>{downstreamReference}</code></div>
                  </div>
                </>
              ) : (
                <EmptyState>Choose a message to inspect its policy-governed decision.</EmptyState>
              )}
            </Panel>

            {selectedId && classification === "transient_downstream" ? (
              <div className="outcome-callout outcome-deferred" role="status">
                <strong>{currentState === "REPLAYED" ? "RECOVERED & REPLAYED" : "DEFERRED"}</strong>
                <p>{deferralReason}</p>
                <small>
                  {recheckAttempts} recheck{recheckAttempts === 1 ? "" : "s"} · last scheduled {formatMoment(recheckAt)} · demo override {demoRecheckSeconds}s
                </small>
              </div>
            ) : null}

            {selectedId && classification === "invalid_data" ? (
              <div className="outcome-callout outcome-withheld" role="status">
                <strong>WITHHELD — NO REPLAY</strong>
                <p>{withheldReason}</p>
                <dl>
                  <div><dt>Proposed fix</dt><dd>{proposedFix}</dd></div>
                  <div><dt>Severity</dt><dd>{severity}</dd></div>
                  <div><dt>SLA</dt><dd>{sla}</dd></div>
                  <div><dt>Recipient</dt><dd>{escalationRecipient}</dd></div>
                </dl>
              </div>
            ) : null}

            {selectedId && classification === "prompt_injection" ? (
              <div className="outcome-callout outcome-quarantine" role="status">
                <strong>QUARANTINED — INSTRUCTION REFUSED</strong>
                <p>{quarantineReason}</p>
                <small>Rendered as inert text; no downstream effect is permitted.</small>
              </div>
            ) : null}

            {selectedId ? (
              <Panel title="Payload evidence" kicker="Untrusted input stays inert" className="payload-panel">
                <div className="payload-grid">
                  <article className="payload-block">
                    <header>
                      <span>Original payload</span>
                      <small>Untrusted input</small>
                    </header>
                    <pre tabIndex={0}>{displayJson(originalPayload)}</pre>
                  </article>
                  <article className="payload-block payload-repaired">
                    <header>
                      <span>{hasRepairedPayload ? "Replay payload" : "Replay payload withheld"}</span>
                      <small>{hasRepairedPayload ? "Policy validated" : "No executable payload"}</small>
                    </header>
                    <pre tabIndex={0}>{displayJson(repairedPayload)}</pre>
                  </article>
                </div>

                <div className="diff-section">
                  <div className="subsection-heading">
                    <span>Changed fields</span>
                    <small>
                      {hasRepairedPayload
                        ? `${payloadDiff.length} deterministic differences`
                        : "Awaiting validated repair"}
                    </small>
                  </div>
                  {hasRepairedPayload && payloadDiff.length ? (
                    <div className="diff-list">
                      {payloadDiff.map((item) => (
                        <div className="diff-row" key={item.path}>
                          <code>{item.path}</code>
                          <span className="diff-before">{compactValue(item.before)}</span>
                          <span className="diff-arrow" aria-hidden="true">→</span>
                          <span className="diff-after">{compactValue(item.after)}</span>
                        </div>
                      ))}
                    </div>
                  ) : hasRepairedPayload ? (
                    <p className="quiet-copy">No repaired payload differences recorded yet.</p>
                  ) : (
                    <p className="quiet-copy">The diff appears after deterministic repair validation.</p>
                  )}
                </div>

                {repairs.length ? (
                  <div className="repair-tags" aria-label="Applied repairs">
                    {repairs.map((repair, index) => (
                      <code key={`${displayJson(repair)}-${index}`}>{compactValue(repair)}</code>
                    ))}
                  </div>
                ) : null}
              </Panel>
            ) : null}

            {selectedId ? (
              <section className="evidence-grid">
            <Panel title="Transition timeline" kicker="Persisted state history">
              {transitions.length ? (
                <ol className="timeline">
                  {transitions.map((transition, index) => (
                    <li key={transition.id}>
                      <span className={`timeline-marker marker-${stateTone(transition.to)}`}>
                        {index + 1}
                      </span>
                      <div>
                        <span className="timeline-state">
                          {transition.from && transition.from !== "UNKNOWN"
                            ? `${stateLabel(transition.from)} → `
                            : ""}
                          <strong>{stateLabel(transition.to)}</strong>
                        </span>
                        <p>{transition.reason}</p>
                      </div>
                      <time dateTime={transition.at}>{formatMoment(transition.at)}</time>
                    </li>
                  ))}
                </ol>
              ) : (
                <EmptyState>Transitions appear as the message moves through recovery.</EmptyState>
              )}
            </Panel>

            <Panel title="Receipt trail" kicker="External effects need proof">
              {receipts.length ? (
                <div className="receipt-list">
                  {receipts.map((receipt) => (
                    <article className={`receipt-card receipt-${receipt.phase}`} key={receipt.id}>
                      <div className="receipt-head">
                        <span className="receipt-icon" aria-hidden="true">{receiptIcon(receipt.phase)}</span>
                        <div>
                          <strong>{receipt.type.replaceAll("_", " ")}</strong>
                          <small>{receipt.id}</small>
                        </div>
                        <span className="receipt-outcome">{receipt.phase}</span>
                      </div>
                      <pre tabIndex={0}>{displayJson(receipt.detail)}</pre>
                      <time dateTime={receipt.at}>{formatMoment(receipt.at)}</time>
                    </article>
                  ))}
                </div>
              ) : (
                <EmptyState>Attempt and result receipts will be recorded here.</EmptyState>
              )}
            </Panel>

            <Panel
              title="Prove it"
              kicker="Live effectively-once query"
              className="proof-panel"
              action={
                <button
                  type="button"
                  className="button button-primary"
                  disabled={busyAction !== null}
                  onClick={() => void proveSelected()}
                >
                  {busyAction === "prove" ? "Querying…" : "Prove effectively once"}
                </button>
              }
            >
              {Object.keys(proof).length ? (
                <div className="proof-equation" role="status">
                  <strong>
                    {numberValue(proof.execution_attempt_count)} execution attempts
                  </strong>
                  <span aria-hidden="true">→</span>
                  <strong>
                    {numberValue(proof.unique_downstream_effect_count)} unique downstream effect
                  </strong>
                </div>
              ) : null}
              <div className="proof-item">
                <span>Deliveries recorded by RetryPermit</span>
                <code>
                  {Object.keys(proof).length
                    ? numberValue(proof.recorded_pubsub_delivery_count)
                    : "Run proof"}
                </code>
              </div>
              <div className="proof-item">
                <span>Downstream requests</span>
                <code>
                  {Object.keys(proof).length
                    ? numberValue(proof.downstream_request_count)
                    : "Run proof"}
                </code>
              </div>
              <div className="proof-item">
                <span>Idempotency prefix</span>
                <code title={idempotencyKey}>
                  {idempotencyKey.length > 28 ? `${idempotencyKey.slice(0, 28)}…` : idempotencyKey}
                </code>
              </div>
              <div className="proof-connector" aria-hidden="true"><i /></div>
              <div className="proof-item">
                <span>Downstream reference</span>
                <code title={downstreamReference}>
                  {stringValue(proof.final_downstream_reference, downstreamReference)}
                </code>
              </div>
              <div className="proof-verdict">
                <span className="proof-shield" aria-hidden="true">✓</span>
                <p>
                  <strong>{downstreamReference === "Awaiting confirmation" ? "Awaiting confirmed result" : "Confirmed result receipt"}</strong>
                  <small>Repeated delivery must resolve to the same simulated business effect.</small>
                </p>
              </div>
            </Panel>
              </section>
            ) : null}
          </div>
        </section>
      </main>

      <footer>
        <p>
          <strong>Demo boundary:</strong> All orders, customers, runbooks, and effects shown here are synthetic or simulated.
        </p>
        <p>Model output is advisory. Deterministic code alone authorises and executes.</p>
      </footer>
    </div>
  );
}
