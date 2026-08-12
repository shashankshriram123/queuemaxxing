"use strict";

const ui = {
  status: document.querySelector("#server-status"), statusText: document.querySelector("#server-status-text"),
  feedback: document.querySelector("#feedback"), lastUpdated: document.querySelector("#last-updated"),
  configForm: document.querySelector("#config-form"), order: document.querySelector("#queue-order"), priority: document.querySelector("#priority-enabled"),
  enqueueForm: document.querySelector("#enqueue-form"), payload: document.querySelector("#payload-editor"), messagePriority: document.querySelector("#message-priority"), messageDelay: document.querySelector("#message-delay"),
  consumerForm: document.querySelector("#consumer-form"), workerId: document.querySelector("#worker-id"), visibility: document.querySelector("#visibility-timeout"), currentLease: document.querySelector("#current-lease"),
  eventList: document.querySelector("#event-list"),
};

const laneIds = { delayed: "lane-delayed", ready: "lane-ready", in_flight: "lane-in-flight", completed: "lane-completed" };
const laneCountIds = { delayed: "lane-count-delayed", ready: "lane-count-ready", in_flight: "lane-count-in-flight", completed: "lane-count-completed" };
const browserReceipts = new Map();
let pollInFlight = false;
let configDirty = false;
let feedbackTimer;

async function api(path, options = {}) {
  const response = await fetch(path, { headers: { "Content-Type": "application/json", ...(options.headers || {}) }, ...options });
  let body = null;
  try { body = await response.json(); } catch { body = null; }
  if (!response.ok) {
    const detail = typeof body?.detail === "string" ? body.detail : `Request failed (${response.status})`;
    throw new Error(detail);
  }
  return body;
}

function setServerStatus(live) {
  ui.status.classList.toggle("live", live);
  ui.status.classList.toggle("offline", !live);
  ui.statusText.textContent = live ? "Server live" : "Server offline";
}

function notify(message, isError = false) {
  clearTimeout(feedbackTimer);
  ui.feedback.textContent = message;
  ui.feedback.classList.add("visible");
  ui.feedback.classList.toggle("error", isError);
  feedbackTimer = setTimeout(() => { ui.feedback.classList.remove("visible", "error"); ui.feedback.textContent = ""; }, 4500);
}

function shortId(value) { return value ? value.slice(0, 8) : "—"; }
function formatTime(value) { return value ? new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "—"; }
function formatCountdown(target) {
  const milliseconds = new Date(target).getTime() - Date.now();
  if (milliseconds <= 0) return "due now";
  return `${(milliseconds / 1000).toFixed(milliseconds < 10000 ? 1 : 0)}s`;
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function renderStats(stats) {
  document.querySelector("#stat-delayed").textContent = stats.delayed;
  document.querySelector("#stat-ready").textContent = stats.ready;
  document.querySelector("#stat-in-flight").textContent = stats.in_flight;
  document.querySelector("#stat-completed").textContent = stats.completed;
  document.querySelector("#stat-redeliveries").textContent = stats.redelivery_count;
}

function renderConfig(config) {
  if (configDirty) return;
  ui.order.value = config.order;
  ui.priority.checked = config.priority_enabled;
}

function addMeta(card, message) {
  const meta = element("div", "card-meta");
  meta.append(
    element("span", "", `State ${message.state.replace("_", " ")}`),
    element("span", "", `Priority ${message.priority}`),
    element("span", "", `Attempts ${message.delivery_attempts}`),
  );
  if (message.leased_by) meta.append(element("span", "", `Worker ${message.leased_by}`));
  card.append(meta);
}

function addLeaseActions(card, message, receipt) {
  const actions = element("div", "lease-actions");
  const ack = element("button", "button primary", "ACK"); ack.type = "button";
  const nack = element("button", "button dark", "NACK"); nack.type = "button";
  const retry = document.createElement("input"); retry.type = "number"; retry.min = "0"; retry.step = "1"; retry.value = "0"; retry.setAttribute("aria-label", "NACK retry delay in seconds");
  ack.addEventListener("click", () => settleLease(message.id, receipt, "ack", 0, ack));
  nack.addEventListener("click", () => settleLease(message.id, receipt, "nack", Number(retry.value), nack));
  actions.append(ack, nack, retry); card.append(actions);
}

function createMessageCard(message) {
  const card = element("article", "message-card");
  const top = element("div", "card-top");
  top.append(element("span", "message-id", shortId(message.id)), element("span", "sequence", `#${message.sequence}`));
  const payload = element("div", "payload-summary", JSON.stringify(message.payload));
  card.append(top, payload); addMeta(card, message);

  card.append(
    element("p", "time-line", `Created ${formatTime(message.created_at)} · Available ${formatTime(message.available_at)}`),
  );
  const stateTime = element("p", "time-line");
  if (message.state === "delayed") { stateTime.textContent = "Available in "; const countdown = element("span", "countdown"); countdown.dataset.target = message.available_at; countdown.dataset.prefix = ""; stateTime.append(countdown); }
  else if (message.state === "in_flight") { stateTime.textContent = "Lease expires in "; const countdown = element("span", "countdown"); countdown.dataset.target = message.lease_expires_at; countdown.dataset.prefix = ""; stateTime.append(countdown); }
  else if (message.state === "completed") stateTime.textContent = `Completed ${formatTime(message.completed_at)}`;
  if (stateTime.textContent) card.append(stateTime);

  const receipt = browserReceipts.get(message.id);
  if (message.state === "in_flight" && receipt) addLeaseActions(card, message, receipt);
  if (message.state !== "in_flight") browserReceipts.delete(message.id);
  return card;
}

function renderMessages(messages) {
  const grouped = { delayed: [], ready: [], in_flight: [], completed: [] };
  messages.forEach((message) => grouped[message.state].push(message));
  Object.entries(grouped).forEach(([state, stateMessages]) => {
    const lane = document.querySelector(`#${laneIds[state]}`);
    lane.replaceChildren();
    document.querySelector(`#${laneCountIds[state]}`).textContent = stateMessages.length;
    if (!stateMessages.length) lane.append(element("p", "empty-lane", "No messages in this state"));
    stateMessages.forEach((message) => lane.append(createMessageCard(message)));
  });
  renderCurrentLease(messages);
  updateCountdowns();
}

function renderCurrentLease(messages) {
  const held = messages.find((message) => browserReceipts.has(message.id) && message.state === "in_flight");
  ui.currentLease.textContent = held ? `Holding ${shortId(held.id)} for ${held.leased_by}; use the controls on its In flight card.` : "No browser-held lease.";
}

function renderEvents(eventsResponse) {
  ui.eventList.replaceChildren();
  if (!eventsResponse.events.length) { ui.eventList.append(element("li", "empty-lane", "No durable events yet")); return; }
  eventsResponse.events.forEach((event) => {
    const row = element("li", "event-row");
    row.append(element("span", "event-number", `#${event.record_number}`), element("span", "event-type", event.event_type), element("span", "event-message", event.message_id ? shortId(event.message_id) : "queue"));
    ui.eventList.append(row);
  });
  ui.eventList.scrollTop = ui.eventList.scrollHeight;
}

function updateCountdowns() { document.querySelectorAll(".countdown").forEach((node) => { node.textContent = `${node.dataset.prefix || ""}${formatCountdown(node.dataset.target)}`; }); }

async function refreshAll() {
  if (pollInFlight || document.hidden) return;
  pollInFlight = true;
  try {
    const [config, messages, stats, events] = await Promise.all([api("/api/config"), api("/api/messages"), api("/api/stats"), api("/api/events?limit=40")]);
    renderConfig(config); renderMessages(messages); renderStats(stats); renderEvents(events);
    setServerStatus(true); ui.lastUpdated.textContent = `Synced ${new Date().toLocaleTimeString()}`;
  } catch (error) { setServerStatus(false); notify(error.message || "Server connection lost", true); }
  finally { pollInFlight = false; }
}

async function settleLease(messageId, receipt, action, retryDelay, button) {
  if (action === "nack" && (!Number.isFinite(retryDelay) || retryDelay < 0)) { notify("Retry delay must be non-negative.", true); return; }
  button.disabled = true;
  try {
    const body = action === "ack" ? { receipt_handle: receipt } : { receipt_handle: receipt, retry_delay_seconds: retryDelay };
    await api(`/api/messages/${messageId}/${action}`, { method: "POST", body: JSON.stringify(body) });
    browserReceipts.delete(messageId); notify(action === "ack" ? "Message acknowledged." : "Message returned for retry."); await refreshAll();
  } catch (error) { notify(error.message || `Unable to ${action.toUpperCase()} message.`, true); await refreshAll(); }
  finally { button.disabled = false; }
}

ui.order.addEventListener("change", () => { configDirty = true; });
ui.priority.addEventListener("change", () => { configDirty = true; });
ui.configForm.addEventListener("submit", async (event) => {
  event.preventDefault(); const button = event.submitter; button.disabled = true;
  try { await api("/api/config", { method: "PUT", body: JSON.stringify({ order: ui.order.value, priority_enabled: ui.priority.checked }) }); configDirty = false; notify("Queue configuration updated."); await refreshAll(); }
  catch (error) { notify(error.message || "Unable to update configuration.", true); }
  finally { button.disabled = false; }
});

ui.enqueueForm.addEventListener("submit", async (event) => {
  event.preventDefault(); const button = event.submitter; button.disabled = true;
  try {
    const payload = JSON.parse(ui.payload.value);
    if (!payload || Array.isArray(payload) || typeof payload !== "object") throw new Error("Payload must be a JSON object.");
    const priority = Number(ui.messagePriority.value); const delay = Number(ui.messageDelay.value);
    if (!Number.isInteger(priority)) throw new Error("Priority must be an integer.");
    if (!Number.isFinite(delay) || delay < 0) throw new Error("Delay must be non-negative.");
    await api("/api/messages", { method: "POST", body: JSON.stringify({ payload, priority, delay_seconds: delay }) }); notify("Message durably enqueued."); await refreshAll();
  } catch (error) { notify(error.message || "Unable to enqueue message.", true); }
  finally { button.disabled = false; }
});

ui.consumerForm.addEventListener("submit", async (event) => {
  event.preventDefault(); const button = event.submitter; button.disabled = true;
  try {
    const workerId = ui.workerId.value.trim(); const visibility = Number(ui.visibility.value);
    if (!workerId) throw new Error("Worker ID cannot be blank.");
    if (!Number.isFinite(visibility) || visibility <= 0) throw new Error("Visibility timeout must be positive.");
    const response = await api("/api/messages/receive", { method: "POST", body: JSON.stringify({ worker_id: workerId, visibility_timeout_seconds: visibility }) });
    if (!response.message) notify("No ready message is available.");
    else { browserReceipts.set(response.message.id, response.message.receipt_handle); notify(`Leased message ${shortId(response.message.id)}.`); }
    await refreshAll();
  } catch (error) { notify(error.message || "Unable to receive a message.", true); }
  finally { button.disabled = false; }
});

document.addEventListener("visibilitychange", () => { if (!document.hidden) refreshAll(); });
setInterval(refreshAll, 900);
setInterval(updateCountdowns, 250);
refreshAll();
