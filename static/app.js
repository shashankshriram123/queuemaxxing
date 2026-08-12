"use strict";

const ui = {
  body: document.body,
  dashboard: document.querySelector("#dashboard"),
  status: document.querySelector("#server-status"),
  statusText: document.querySelector("#server-status-text"),
  connectionBanner: document.querySelector("#connection-banner"),
  feedback: document.querySelector("#feedback"),
  lastUpdated: document.querySelector("#last-updated"),
  configBadge: document.querySelector("#config-badge"),
  configSummary: document.querySelector("#config-summary"),
  configForm: document.querySelector("#config-form"),
  order: document.querySelector("#queue-order"),
  priority: document.querySelector("#priority-enabled"),
  enqueueForm: document.querySelector("#enqueue-form"),
  payload: document.querySelector("#payload-editor"),
  messagePriority: document.querySelector("#message-priority"),
  messageDelay: document.querySelector("#message-delay"),
  deliverySummary: document.querySelector(".delivery-options summary span"),
  burstSize: document.querySelector("#burst-size"),
  burstSizeField: document.querySelector("#burst-size-field"),
  sendOne: document.querySelector("#send-one"),
  sendBurst: document.querySelector("#send-burst"),
  burstStatus: document.querySelector("#burst-status"),
  composerModes: document.querySelectorAll("[data-composer-mode]"),
  exampleSelect: document.querySelector("#payload-example"),
  loadExample: document.querySelector("#load-example"),
  consumerForm: document.querySelector("#consumer-form"),
  workerCount: document.querySelector("#worker-count"),
  visibility: document.querySelector("#visibility-timeout"),
  processingTime: document.querySelector("#processing-time"),
  runPane: document.querySelector(".run-pane"),
  workerPrompt: document.querySelector("#worker-prompt"),
  workerPromptTitle: document.querySelector("#worker-prompt-title"),
  workerPromptCopy: document.querySelector("#worker-prompt-copy"),
  startWorkers: document.querySelector("#start-workers"),
  stopWorkers: document.querySelector("#stop-workers"),
  workerActivity: document.querySelector("#worker-activity"),
  workerPoolStatus: document.querySelector("#worker-pool-status"),
  currentLease: document.querySelector("#current-lease"),
  workerOptionsSummary: document.querySelector(".worker-options summary span"),
  scenarioSelect: document.querySelector("#scenario-select"),
  scenarioDescription: document.querySelector("#scenario-description"),
  runScenario: document.querySelector("#run-scenario"),
  workflowSteps: document.querySelectorAll("[data-workflow-step]"),
  clearCompleted: document.querySelector("#clear-completed"),
  eventList: document.querySelector("#event-list"),
  eventCount: document.querySelector("#event-count"),
};

const laneIds = {
  delayed: "lane-delayed",
  ready: "lane-ready",
  in_flight: "lane-in-flight",
  completed: "lane-completed",
};
const laneCountIds = {
  delayed: "lane-count-delayed",
  ready: "lane-count-ready",
  in_flight: "lane-count-in-flight",
  completed: "lane-count-completed",
};
const emptyStateCopy = {
  delayed: ["◷", "No delayed messages", "Messages with a future availability time appear here."],
  ready: ["→", "Nothing ready", "Enqueue a message or wait for a delay to expire."],
  in_flight: ["↗", "No active leases", "Receive a ready message to create a worker lease."],
  completed: ["✓", "No completed work", "Acknowledged messages remain visible here."],
};
const examples = {
  standard: {
    payload: { event: "order.updated", order_id: 104, status: "shipped" },
    priority: 5,
    delay: 0,
  },
  priority: {
    payload: { event: "inventory.low", sku: "QM-204", remaining: 3 },
    priority: 50,
    delay: 0,
  },
  delayed: {
    payload: { event: "sync.scheduled", source: "orders", batch: "nightly" },
    priority: 5,
    delay: 5,
  },
};
const scenarioCopy = {
  flash: "Send 40 messages concurrently using FIFO ordering.",
  vip: "Mix standard traffic with a final priority-100 checkout.",
  backfill: "Queue 20 jobs that become available after five seconds.",
  failure: "Expire a real lease, redeliver it, then acknowledge the retry.",
};

const browserReceipts = new Map();
let pollInFlight = false;
let configDirty = false;
let feedbackTimer;
let connectionState = "connecting";
let hasLoaded = false;
let workersRunning = false;
let workerGeneration = 0;
let workerTasks = [];
let workerProcessed = 0;
let scenarioRunning = false;
let composerMode = "burst";
let latestStats = null;
let clearCompletedArmed = false;
let clearCompletedTimer;

class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  let body = null;
  try {
    body = await response.json();
  } catch {
    body = null;
  }
  if (!response.ok) {
    const detail = typeof body?.detail === "string" ? body.detail : `Request failed (${response.status})`;
    throw new ApiError(detail, response.status);
  }
  return body;
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function shortId(value) {
  return value ? value.slice(0, 8) : "—";
}

function formatTime(value) {
  if (!value) return "—";
  return new Date(value).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatCountdown(target) {
  if (!target) return "—";
  const milliseconds = new Date(target).getTime() - Date.now();
  if (milliseconds <= 0) return "due now";
  return `${(milliseconds / 1000).toFixed(milliseconds < 10000 ? 1 : 0)}s`;
}

function wait(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function setWorkflowStep(step) {
  ui.workflowSteps.forEach((node) => {
    const nodeStep = Number(node.dataset.workflowStep);
    node.classList.toggle("done", nodeStep < step);
    node.classList.toggle("current", nodeStep === step);
    if (nodeStep === step) node.setAttribute("aria-current", "step");
    else node.removeAttribute("aria-current");
  });
}

function updateOptionSummaries() {
  const priority = ui.messagePriority.value || "0";
  const delay = Number(ui.messageDelay.value);
  ui.deliverySummary.textContent = `Priority ${priority} · ${delay > 0 ? `${delay} sec delay` : "No delay"}`;
  ui.workerOptionsSummary.textContent = `Visibility ${ui.visibility.value || "—"} sec`;
}

function updateBurstLabel() {
  const count = ui.burstSize.value || "—";
  const label = `Send ${count}-message burst`;
  ui.sendBurst.dataset.idleLabel = label;
  if (ui.sendBurst.dataset.busy !== "true") {
    ui.sendBurst.querySelector("span:first-child").textContent = label;
  }
}

function setComposerMode(mode) {
  composerMode = mode === "single" ? "single" : "burst";
  ui.composerModes.forEach((button) => {
    const active = button.dataset.composerMode === composerMode;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", String(active));
    button.tabIndex = active ? 0 : -1;
  });
  const burstMode = composerMode === "burst";
  ui.burstSizeField.hidden = !burstMode;
  ui.sendBurst.hidden = !burstMode;
  ui.sendOne.hidden = burstMode;
  ui.burstStatus.textContent = burstMode
    ? "Ready to dispatch concurrent requests."
    : "Ready to enqueue one durable message.";
  setWorkflowStep(2);
}

function parseProducerInput() {
  const payload = JSON.parse(ui.payload.value);
  if (!payload || Array.isArray(payload) || typeof payload !== "object") {
    throw new Error("Payload must be a JSON object.");
  }
  const priority = Number(ui.messagePriority.value);
  const delay = Number(ui.messageDelay.value);
  if (!Number.isInteger(priority)) throw new Error("Priority must be an integer.");
  if (!Number.isFinite(delay) || delay < 0) throw new Error("Delay must be non-negative.");
  return { payload, priority, delay };
}

function validateCount(value, label, minimum, maximum) {
  const count = Number(value);
  if (!Number.isInteger(count) || count < minimum || count > maximum) {
    throw new Error(`${label} must be an integer from ${minimum} to ${maximum}.`);
  }
  return count;
}

function burstPayload(payload, burstId, index, count, scenario) {
  return {
    ...payload,
    _queuemaxxing: {
      burst_id: burstId,
      index: index + 1,
      size: count,
      scenario,
    },
  };
}

async function enqueueConcurrent({ count, payload, priority, delay, scenario = "custom", priorityForIndex }) {
  const burstId = crypto.randomUUID ? crypto.randomUUID() : `burst-${Date.now()}`;
  const requests = Array.from({ length: count }, (_, index) => api("/api/messages", {
    method: "POST",
    body: JSON.stringify({
      payload: burstPayload(payload, burstId, index, count, scenario),
      priority: priorityForIndex ? priorityForIndex(index) : priority,
      delay_seconds: delay,
    }),
  }));
  const results = await Promise.allSettled(requests);
  const succeeded = results.filter((result) => result.status === "fulfilled").length;
  const failed = results.length - succeeded;
  return { burstId, succeeded, failed };
}

async function updateQueueConfig(order, priorityEnabled) {
  const config = await api("/api/config", {
    method: "PUT",
    body: JSON.stringify({ order, priority_enabled: priorityEnabled }),
  });
  configDirty = false;
  renderConfig(config);
  return config;
}

function setConnection(mode) {
  const previous = connectionState;
  connectionState = mode;
  ui.body.dataset.connection = mode;
  ui.status.classList.remove("connecting", "live", "offline");
  ui.status.classList.add(mode);
  ui.statusText.textContent = mode === "live" ? "API live" : mode === "offline" ? "API offline" : "Connecting";
  ui.connectionBanner.hidden = mode !== "offline";
  document.querySelectorAll("[data-requires-online]").forEach((control) => {
    control.disabled = mode !== "live" || control.dataset.busy === "true";
  });
  updateWorkerGuidance();
  updateClearCompletedControl();
  return previous;
}

function setBusy(button, busy, busyLabel) {
  if (!button) return;
  const label = button.querySelector("span:first-child") || button;
  if (!button.dataset.idleLabel) button.dataset.idleLabel = label.textContent;
  button.dataset.busy = String(busy);
  button.setAttribute("aria-busy", String(busy));
  button.disabled = busy || connectionState !== "live";
  label.textContent = busy ? busyLabel : button.dataset.idleLabel;
}

function notify(message, type = "success", persistent = false) {
  clearTimeout(feedbackTimer);
  ui.feedback.textContent = message;
  ui.feedback.className = `feedback ${type}`;
  ui.feedback.hidden = false;
  if (!persistent) {
    feedbackTimer = setTimeout(() => {
      ui.feedback.hidden = true;
      ui.feedback.textContent = "";
    }, 5200);
  }
}

function updateWorkerGuidance() {
  const ready = Number(latestStats?.ready || 0);
  const delayed = Number(latestStats?.delayed || 0);
  const queued = ready + delayed;
  const shouldPrompt = connectionState === "live" && !workersRunning && queued > 0;

  ui.runPane.classList.toggle("has-ready-work", shouldPrompt);
  ui.workerPrompt.hidden = !shouldPrompt;
  ui.startWorkers.classList.toggle("attention", shouldPrompt);
  if (!shouldPrompt) return;

  if (ready > 0) {
    ui.workerPromptTitle.textContent = `${ready} ready message${ready === 1 ? "" : "s"} waiting`;
    ui.workerPromptCopy.textContent = "Start workers to process them now.";
  } else {
    ui.workerPromptTitle.textContent = `${delayed} delayed message${delayed === 1 ? "" : "s"} queued`;
    ui.workerPromptCopy.textContent = "Start workers now and they will wait for availability.";
  }
  setWorkflowStep(3);
}

function disarmClearCompleted() {
  clearTimeout(clearCompletedTimer);
  clearCompletedArmed = false;
  ui.clearCompleted.classList.remove("armed");
  ui.clearCompleted.querySelector("span").textContent = "Clear";
  ui.clearCompleted.setAttribute("aria-label", "Clear completed messages");
}

function updateClearCompletedControl() {
  const completed = Number(latestStats?.completed || 0);
  if (completed === 0) disarmClearCompleted();
  if (clearCompletedArmed) {
    ui.clearCompleted.querySelector("span").textContent = `Confirm ${completed}`;
    ui.clearCompleted.setAttribute(
      "aria-label",
      `Confirm clearing ${completed} completed messages`,
    );
  }
  ui.clearCompleted.disabled = (
    connectionState !== "live"
    || completed === 0
    || ui.clearCompleted.dataset.busy === "true"
  );
}

function renderStats(stats) {
  latestStats = stats;
  document.querySelector("#stat-total").textContent = stats.total;
  document.querySelector("#stat-delayed").textContent = stats.delayed;
  document.querySelector("#stat-ready").textContent = stats.ready;
  document.querySelector("#stat-in-flight").textContent = stats.in_flight;
  document.querySelector("#stat-completed").textContent = stats.completed;
  document.querySelector("#stat-redeliveries").textContent = stats.redelivery_count;
  document.querySelector("#stat-active-workers").textContent = stats.active_worker_count;
  updateWorkerGuidance();
  updateClearCompletedControl();
}

function describeConfig(config) {
  const order = config.order.toUpperCase();
  return `${order} · ${config.priority_enabled ? "priority on" : "standard"}`;
}

function renderConfig(config) {
  ui.configBadge.textContent = describeConfig(config);
  if (configDirty) return;
  ui.order.value = config.order;
  ui.priority.checked = config.priority_enabled;
  ui.configSummary.textContent = `${config.order.toUpperCase()} ties · priority ${config.priority_enabled ? "enabled" : "disabled"}`;
}

function appendMetaPill(parent, text, className = "") {
  parent.append(element("span", `meta-pill ${className}`.trim(), text));
}

function addLeaseActions(card, message, receipt) {
  const actions = element("div", "lease-actions");
  const ack = element("button", "button primary", "ACK");
  ack.type = "button";
  ack.dataset.requiresOnline = "";
  const nack = element("button", "button dark", "NACK");
  nack.type = "button";
  nack.dataset.requiresOnline = "";
  const retry = document.createElement("input");
  retry.type = "number";
  retry.min = "0";
  retry.step = "0.5";
  retry.value = "0";
  retry.setAttribute("aria-label", "NACK retry delay in seconds");
  ack.disabled = connectionState !== "live";
  nack.disabled = connectionState !== "live";
  ack.addEventListener("click", () => settleLease(message.id, receipt, "ack", 0, ack));
  nack.addEventListener("click", () => settleLease(message.id, receipt, "nack", Number(retry.value), nack));
  actions.append(ack, nack, retry);
  card.append(actions);
}

function createMessageCard(message) {
  const card = element("article", "message-card");
  const toneIndex = Math.max(0, Number(message.sequence || 1) - 1) % 5;
  card.classList.add(`tone-${toneIndex}`);
  card.dataset.messageId = message.id;
  const top = element("div", "card-top");
  const id = element("span", "message-id", shortId(message.id));
  id.title = message.id;
  top.append(id, element("span", "sequence", `SEQ ${message.sequence}`));

  const payload = element("pre", "payload-summary", JSON.stringify(message.payload, null, 2));
  const meta = element("div", "card-meta");
  appendMetaPill(meta, message.state.replace("_", " "), "state-pill");
  appendMetaPill(meta, `P${message.priority}`);
  appendMetaPill(meta, `${message.delivery_attempts} attempt${message.delivery_attempts === 1 ? "" : "s"}`);
  if (message.leased_by) appendMetaPill(meta, message.leased_by);
  card.append(top, payload, meta);

  const created = element("div", "time-row");
  created.append(element("span", "", "Created"), element("span", "", formatTime(message.created_at)));
  card.append(created);

  if (message.state === "delayed") {
    const timing = element("div", "time-row");
    const countdown = element("span", "countdown");
    countdown.dataset.target = message.available_at;
    timing.append(element("span", "", "Available in"), countdown);
    card.append(timing);
  } else if (message.state === "in_flight") {
    const timing = element("div", "time-row");
    const countdown = element("span", "countdown");
    countdown.dataset.target = message.lease_expires_at;
    timing.append(element("span", "", "Lease expires"), countdown);
    card.append(timing);
  } else if (message.state === "completed") {
    const timing = element("div", "time-row");
    timing.append(element("span", "", "Completed"), element("span", "", formatTime(message.completed_at)));
    card.append(timing);
  }

  const receipt = browserReceipts.get(message.id);
  if (message.state === "in_flight" && receipt) {
    addLeaseActions(card, message, receipt);
  } else if (message.state === "in_flight") {
    card.append(element("p", "external-lease", "Lease owned by another consumer"));
  } else {
    browserReceipts.delete(message.id);
  }
  return card;
}

function createEmptyLane(state) {
  const [icon, title, description] = emptyStateCopy[state];
  const wrapper = element("div", "empty-lane");
  const content = document.createElement("div");
  content.append(element("span", "", icon), element("strong", "", title), element("p", "", description));
  wrapper.append(content);
  return wrapper;
}

function renderMessages(messages) {
  const grouped = { delayed: [], ready: [], in_flight: [], completed: [] };
  messages.forEach((message) => {
    if (grouped[message.state]) grouped[message.state].push(message);
  });
  Object.entries(grouped).forEach(([state, stateMessages]) => {
    const lane = document.querySelector(`#${laneIds[state]}`);
    lane.replaceChildren();
    document.querySelector(`#${laneCountIds[state]}`).textContent = stateMessages.length;
    if (!stateMessages.length) lane.append(createEmptyLane(state));
    stateMessages.forEach((message) => lane.append(createMessageCard(message)));
  });
  renderCurrentLease(messages);
  updateCountdowns();
}

function renderCurrentLease(messages) {
  const held = messages.find((message) => browserReceipts.has(message.id) && message.state === "in_flight");
  ui.currentLease.classList.toggle("held", Boolean(held));
  ui.currentLease.classList.toggle("running", workersRunning && !held);
  ui.workerPoolStatus.textContent = held
    ? `Manual lease ${shortId(held.id)} held by this tab`
    : workersRunning
      ? `${ui.workerCount.value} workers running · ${workerProcessed} completed`
      : workerProcessed
        ? `Worker pool stopped · ${workerProcessed} completed`
        : "Worker pool stopped";
}

function initializeWorkerActivity(count) {
  ui.workerActivity.replaceChildren();
  for (let index = 0; index < count; index += 1) {
    const worker = element("span", "worker-state", `W${index + 1} idle`);
    worker.dataset.workerIndex = String(index);
    ui.workerActivity.append(worker);
  }
}

function updateWorkerActivity(index, state, text) {
  const worker = ui.workerActivity.querySelector(`[data-worker-index="${index}"]`);
  if (!worker) return;
  worker.className = `worker-state ${state}`.trim();
  worker.textContent = `W${index + 1} ${text}`;
}

function workerSettings() {
  const count = validateCount(ui.workerCount.value, "Worker count", 1, 12);
  const visibility = Number(ui.visibility.value);
  const processing = Number(ui.processingTime.value);
  if (!Number.isFinite(visibility) || visibility <= 0) {
    throw new Error("Visibility timeout must be positive.");
  }
  if (!Number.isFinite(processing) || processing < 0 || processing > 30) {
    throw new Error("Processing time must be from 0 to 30 seconds.");
  }
  if (processing + 0.1 >= visibility) {
    throw new Error("Visibility must be longer than processing time.");
  }
  return { count, visibility, processing };
}

async function simulatedWorkerLoop(index, generation, settings) {
  const workerId = `browser-worker-${index + 1}`;
  while (workersRunning && generation === workerGeneration) {
    try {
      updateWorkerActivity(index, "", "polling");
      const response = await api("/api/messages/receive", {
        method: "POST",
        body: JSON.stringify({
          worker_id: workerId,
          visibility_timeout_seconds: settings.visibility,
        }),
      });
      if (!response.message) {
        updateWorkerActivity(index, "", "idle");
        await wait(350);
        continue;
      }

      const message = response.message;
      updateWorkerActivity(index, "processing", shortId(message.id));
      await wait(settings.processing * 1000);
      const shouldAck = workersRunning && generation === workerGeneration;
      const action = shouldAck ? "ack" : "nack";
      const body = shouldAck
        ? { receipt_handle: message.receipt_handle }
        : { receipt_handle: message.receipt_handle, retry_delay_seconds: 0 };
      await api(`/api/messages/${message.id}/${action}`, {
        method: "POST",
        body: JSON.stringify(body),
      });
      if (shouldAck) {
        workerProcessed += 1;
        updateWorkerActivity(index, "success", `ACK ${workerProcessed}`);
        ui.workerPoolStatus.textContent = `${settings.count} workers running · ${workerProcessed} completed`;
      } else {
        updateWorkerActivity(index, "", "released");
      }
      await wait(100);
    } catch (error) {
      updateWorkerActivity(index, "error", error.status === 409 ? "lease expired" : "retrying");
      await wait(600);
    }
  }
  updateWorkerActivity(index, "", "stopped");
}

function startWorkerPool() {
  if (workersRunning) return;
  let settings;
  try {
    settings = workerSettings();
  } catch (error) {
    notify(error.message, "error");
    return;
  }
  workersRunning = true;
  updateWorkerGuidance();
  workerGeneration += 1;
  workerProcessed = 0;
  initializeWorkerActivity(settings.count);
  ui.currentLease.classList.add("running");
  ui.workerPoolStatus.textContent = `${settings.count} workers running · 0 completed`;
  ui.startWorkers.dataset.busy = "true";
  ui.startWorkers.disabled = true;
  ui.workerCount.disabled = true;
  ui.visibility.disabled = true;
  ui.processingTime.disabled = true;
  ui.stopWorkers.disabled = false;
  setWorkflowStep(4);
  const generation = workerGeneration;
  workerTasks = Array.from({ length: settings.count }, (_, index) => simulatedWorkerLoop(index, generation, settings));
  notify(`${settings.count} simulated workers started against the real receive/ACK API.`);
}

async function stopWorkerPool() {
  if (!workersRunning) return;
  workersRunning = false;
  workerGeneration += 1;
  ui.stopWorkers.disabled = true;
  ui.workerPoolStatus.textContent = "Stopping workers and releasing active leases…";
  await Promise.allSettled(workerTasks);
  workerTasks = [];
  ui.startWorkers.dataset.busy = "false";
  ui.startWorkers.disabled = connectionState !== "live";
  ui.workerCount.disabled = false;
  ui.visibility.disabled = false;
  ui.processingTime.disabled = false;
  ui.currentLease.classList.remove("running");
  ui.workerPoolStatus.textContent = `Worker pool stopped · ${workerProcessed} completed`;
  notify(`Worker pool stopped after completing ${workerProcessed} messages.`);
  await refreshAll();
}

function renderEvents(eventsResponse) {
  const events = eventsResponse.events;
  ui.eventCount.textContent = `${events.length} recent event${events.length === 1 ? "" : "s"}`;
  ui.eventList.replaceChildren();
  if (!events.length) {
    ui.eventList.append(element("li", "event-empty", "The WAL has no durable events yet."));
    return;
  }
  events.forEach((event) => {
    const row = element("li", "event-row");
    row.append(
      element("span", "event-number", `#${event.record_number}`),
      element("span", "event-type", event.event_type),
      element("span", "event-message", event.message_id ? shortId(event.message_id) : "queue"),
    );
    ui.eventList.append(row);
  });
}

function renderOfflineLanes() {
  Object.entries(laneIds).forEach(([state, laneId]) => {
    const lane = document.querySelector(`#${laneId}`);
    const wrapper = element("div", "empty-lane");
    const content = document.createElement("div");
    content.append(
      element("span", "", "!"),
      element("strong", "", "API unavailable"),
      element("p", "", "Live queue state will appear after the connection recovers."),
    );
    wrapper.append(content);
    lane.replaceChildren(wrapper);
    document.querySelector(`#${laneCountIds[state]}`).textContent = "—";
  });
}

function updateCountdowns() {
  document.querySelectorAll(".countdown").forEach((node) => {
    node.textContent = formatCountdown(node.dataset.target);
  });
}

async function refreshAll() {
  if (pollInFlight || document.hidden) return;
  pollInFlight = true;
  try {
    const [config, messages, stats, events] = await Promise.all([
      api("/api/config"),
      api("/api/messages"),
      api("/api/stats"),
      api("/api/events?limit=40"),
    ]);
    renderConfig(config);
    renderMessages(messages);
    renderStats(stats);
    renderEvents(events);
    const previous = setConnection("live");
    hasLoaded = true;
    ui.dashboard.setAttribute("aria-busy", "false");
    ui.lastUpdated.textContent = `Synced ${new Date().toLocaleTimeString()}`;
    if (previous === "offline") notify("Connection restored. Live queue data is current.");
  } catch (error) {
    const previous = setConnection("offline");
    ui.dashboard.setAttribute("aria-busy", "false");
    ui.lastUpdated.textContent = "Sync paused";
    if (!hasLoaded) renderOfflineLanes();
    if (previous !== "offline") notify(error.message || "Unable to reach the queue API.", "error");
  } finally {
    pollInFlight = false;
  }
}

async function settleLease(messageId, receipt, action, retryDelay, button) {
  if (action === "nack" && (!Number.isFinite(retryDelay) || retryDelay < 0)) {
    notify("Retry delay must be a non-negative number.", "error");
    return;
  }
  setBusy(button, true, action === "ack" ? "ACK…" : "NACK…");
  try {
    const body = action === "ack"
      ? { receipt_handle: receipt }
      : { receipt_handle: receipt, retry_delay_seconds: retryDelay };
    await api(`/api/messages/${messageId}/${action}`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    browserReceipts.delete(messageId);
    notify(action === "ack" ? "Message acknowledged and completed." : "Message returned to the queue.");
    await refreshAll();
  } catch (error) {
    if (error.status === 409) {
      browserReceipts.delete(messageId);
      notify("That receipt is stale or its lease expired. The live state has been refreshed.", "stale");
    } else {
      notify(error.message || `Unable to ${action.toUpperCase()} the message.`, "error");
    }
    await refreshAll();
  } finally {
    setBusy(button, false, "");
  }
}

function markConfigDirty() {
  configDirty = true;
  ui.configSummary.textContent = "Unsaved policy changes";
}

ui.order.addEventListener("change", markConfigDirty);
ui.priority.addEventListener("change", markConfigDirty);

ui.configForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter || ui.configForm.querySelector("button[type='submit']");
  setBusy(button, true, "Applying…");
  try {
    const config = await api("/api/config", {
      method: "PUT",
      body: JSON.stringify({ order: ui.order.value, priority_enabled: ui.priority.checked }),
    });
    configDirty = false;
    renderConfig(config);
    notify(`Policy updated: ${describeConfig(config)}.`);
    await refreshAll();
  } catch (error) {
    notify(error.message || "Unable to update queue policy.", "error");
  } finally {
    setBusy(button, false, "");
  }
});

ui.enqueueForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter || ui.enqueueForm.querySelector("button[type='submit']");
  setBusy(button, true, "Sending…");
  try {
    const { payload, priority, delay } = parseProducerInput();
    const message = await api("/api/messages", {
      method: "POST",
      body: JSON.stringify({ payload, priority, delay_seconds: delay }),
    });
    notify(`Message ${shortId(message.id)} durably enqueued as ${message.state}.`);
    setWorkflowStep(3);
    await refreshAll();
  } catch (error) {
    notify(error.message || "Unable to enqueue the message.", "error");
  } finally {
    setBusy(button, false, "");
  }
});

ui.sendBurst.addEventListener("click", async () => {
  setBusy(ui.sendBurst, true, "Sending…");
  try {
    const { payload, priority, delay } = parseProducerInput();
    const count = validateCount(ui.burstSize.value, "Burst size", 2, 100);
    ui.burstStatus.textContent = `Dispatching ${count} concurrent POST requests…`;
    const result = await enqueueConcurrent({ count, payload, priority, delay });
    ui.burstStatus.textContent = `${result.succeeded}/${count} persisted · burst ${shortId(result.burstId)}`;
    if (result.failed) {
      notify(`${result.succeeded} messages persisted; ${result.failed} requests failed.`, "error");
    } else {
      notify(`${count} concurrent messages durably enqueued.`);
    }
    setWorkflowStep(3);
    await refreshAll();
  } catch (error) {
    ui.burstStatus.textContent = "Burst stopped before completion.";
    notify(error.message || "Unable to send the burst.", "error");
  } finally {
    setBusy(ui.sendBurst, false, "");
  }
});

ui.consumerForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter || ui.consumerForm.querySelector("button[type='submit']");
  setBusy(button, true, "Claiming…");
  try {
    const visibility = Number(ui.visibility.value);
    if (!Number.isFinite(visibility) || visibility <= 0) throw new Error("Visibility timeout must be positive.");
    const response = await api("/api/messages/receive", {
      method: "POST",
      body: JSON.stringify({ worker_id: "browser-manual", visibility_timeout_seconds: visibility }),
    });
    if (!response.message) {
      notify("No ready message is available.", "stale");
    } else {
      browserReceipts.set(response.message.id, response.message.receipt_handle);
      notify(`Message ${shortId(response.message.id)} manually leased to this tab.`);
    }
    await refreshAll();
  } catch (error) {
    notify(error.message || "Unable to receive a message.", "error");
  } finally {
    setBusy(button, false, "");
  }
});

ui.startWorkers.addEventListener("click", startWorkerPool);
ui.stopWorkers.addEventListener("click", stopWorkerPool);

ui.clearCompleted.addEventListener("click", async () => {
  const completed = Number(latestStats?.completed || 0);
  if (!clearCompletedArmed) {
    clearCompletedArmed = true;
    ui.clearCompleted.classList.add("armed");
    updateClearCompletedControl();
    clearCompletedTimer = setTimeout(() => {
      disarmClearCompleted();
      updateClearCompletedControl();
    }, 8000);
    return;
  }

  clearTimeout(clearCompletedTimer);
  setBusy(ui.clearCompleted, true, "Clearing…");
  try {
    const result = await api("/api/messages/completed", { method: "DELETE" });
    disarmClearCompleted();
    notify(
      result.cleared === 1
        ? "Cleared 1 completed message."
        : `Cleared ${result.cleared} completed messages.`,
    );
    await refreshAll();
  } catch (error) {
    disarmClearCompleted();
    notify(error.message || `Unable to clear ${completed} completed messages.`, "error");
  } finally {
    setBusy(ui.clearCompleted, false, "");
    updateClearCompletedControl();
  }
});

async function runScenario(name, button) {
  if (scenarioRunning) return;
  scenarioRunning = true;
  ui.scenarioSelect.disabled = true;
  setBusy(button, true, "Running…");
  try {
    if (name === "flash") {
      await updateQueueConfig("fifo", false);
      const result = await enqueueConcurrent({
        count: 40,
        payload: { event: "order.updated", campaign: "flash-sale", status: "changed" },
        priority: 0,
        delay: 0,
        scenario: "flash-sale",
      });
      notify(`Flash sale sent ${result.succeeded} real messages concurrently.`);
    } else if (name === "vip") {
      await updateQueueConfig("fifo", true);
      const result = await enqueueConcurrent({
        count: 16,
        payload: { event: "checkout.requested", cohort: "mixed-traffic" },
        priority: 1,
        priorityForIndex: (index) => index === 15 ? 100 : 1,
        delay: 0,
        scenario: "vip-priority",
      });
      notify(`VIP scenario queued ${result.succeeded} messages; the last request has priority 100.`);
    } else if (name === "backfill") {
      const result = await enqueueConcurrent({
        count: 20,
        payload: { event: "backfill.partition", dataset: "orders" },
        priority: 5,
        delay: 5,
        scenario: "delayed-backfill",
      });
      notify(`Delayed backfill queued ${result.succeeded} jobs for availability in 5 seconds.`);
    } else if (name === "failure") {
      if (workersRunning) await stopWorkerPool();
      await updateQueueConfig("fifo", true);
      await api("/api/messages", {
        method: "POST",
        body: JSON.stringify({
          payload: { event: "worker.failure.demo", expected: "lease-expiration" },
          priority: 10000,
          delay_seconds: 0,
        }),
      });
      const response = await api("/api/messages/receive", {
        method: "POST",
        body: JSON.stringify({ worker_id: "simulated-failure", visibility_timeout_seconds: 2 }),
      });
      if (!response.message) throw new Error("No message was available for the failure lease.");
      notify(`Worker failure leased ${shortId(response.message.id)} and intentionally left it unacknowledged. Watch it return in 2 seconds.`, "stale");
      await refreshAll();
      await wait(2300);
      await refreshAll();
      const retry = await api("/api/messages/receive", {
        method: "POST",
        body: JSON.stringify({ worker_id: "recovery-worker", visibility_timeout_seconds: 5 }),
      });
      if (!retry.message) throw new Error("The expired lease did not become available for retry.");
      await api(`/api/messages/${retry.message.id}/ack`, {
        method: "POST",
        body: JSON.stringify({ receipt_handle: retry.message.receipt_handle }),
      });
      notify(`Lease expired, message ${shortId(retry.message.id)} was redelivered on attempt ${retry.message.delivery_attempts}, then ACKed.`);
    }
    setWorkflowStep(4);
    await refreshAll();
  } catch (error) {
    notify(error.message || "The scenario could not complete.", "error");
  } finally {
    scenarioRunning = false;
    ui.scenarioSelect.disabled = false;
    setBusy(button, false, "");
  }
}

ui.scenarioSelect.addEventListener("change", () => {
  ui.scenarioDescription.textContent = scenarioCopy[ui.scenarioSelect.value];
});

ui.runScenario.addEventListener("click", () => {
  runScenario(ui.scenarioSelect.value, ui.runScenario);
});

ui.loadExample.addEventListener("click", () => {
  const example = examples[ui.exampleSelect.value];
  ui.payload.value = JSON.stringify(example.payload, null, 2);
  ui.messagePriority.value = example.priority;
  ui.messageDelay.value = example.delay;
  updateOptionSummaries();
  ui.payload.focus();
  notify("Example loaded. Review it, then enqueue through the real API.");
});

ui.composerModes.forEach((button) => {
  button.addEventListener("click", () => setComposerMode(button.dataset.composerMode));
  button.addEventListener("keydown", (event) => {
    if (!["ArrowLeft", "ArrowRight"].includes(event.key)) return;
    event.preventDefault();
    const nextMode = button.dataset.composerMode === "single" ? "burst" : "single";
    setComposerMode(nextMode);
    document.querySelector(`[data-composer-mode="${nextMode}"]`).focus();
  });
});

ui.burstSize.addEventListener("input", updateBurstLabel);
ui.messagePriority.addEventListener("input", updateOptionSummaries);
ui.messageDelay.addEventListener("input", updateOptionSummaries);
ui.visibility.addEventListener("input", updateOptionSummaries);

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) refreshAll();
});

function showDirectFileNotice() {
  setConnection("offline");
  ui.statusText.textContent = "Start FastAPI";
  ui.connectionBanner.hidden = false;
  ui.connectionBanner.replaceChildren(
    element("strong", "", "This is a FastAPI dashboard, not a standalone HTML file."),
    element("span", "", " Run .venv/bin/python run.py, then open http://localhost:8000."),
  );
  ui.lastUpdated.textContent = "Server required";
  renderOfflineLanes();
}

updateOptionSummaries();
updateBurstLabel();
setComposerMode("burst");
ui.scenarioDescription.textContent = scenarioCopy[ui.scenarioSelect.value];

if (window.location.protocol === "file:") {
  showDirectFileNotice();
} else {
  setInterval(refreshAll, 1000);
  setInterval(updateCountdowns, 200);
  setConnection("connecting");
  refreshAll();
}
