const $ = (id) => document.getElementById(id);
let selectedId = '';

function setStatus(msg, cls = '') {
  const el = $('status');
  el.textContent = msg;
  el.className = 'status ' + cls;
}

function deliveryBadge(delivery) {
  if (!delivery) {
    return '<span class="badge badge-none">—</span>';
  }
  const cls = {
    delivered: 'badge-delivered',
    failed: 'badge-failed',
    pending: 'badge-pending',
  }[delivery.status] || 'badge-none';
  const label = delivery.status;
  const extra = delivery.status_code ? ` (${delivery.status_code})` : '';
  return `<span class="badge ${cls}">${label}${extra}</span>`;
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { Accept: 'application/json', ...(opts.headers || {}) },
    ...opts,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = body?.error?.message || res.statusText;
    throw new Error(msg);
  }
  return body;
}

function renderRows(events) {
  const tbody = $('rows');
  tbody.innerHTML = '';
  for (const ev of events) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><code>${ev.id}</code></td>
      <td>${ev.topic}</td>
      <td>${ev.payload_size}</td>
      <td>${deliveryBadge(ev.latest_delivery)}</td>
      <td>${new Date(ev.created_at).toLocaleString()}</td>
      <td>${ev.is_replay ? 'yes' : ''}</td>`;
    tr.onclick = () => selectEvent(ev.id);
    tbody.appendChild(tr);
  }
}

function renderDeliveries(deliveries) {
  const section = $('deliveries');
  const tbody = $('delivery-rows');
  tbody.innerHTML = '';

  if (!deliveries || deliveries.length === 0) {
    section.hidden = true;
    return;
  }

  section.hidden = false;
  for (const d of deliveries) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><code>${d.id}</code></td>
      <td>${d.kind}</td>
      <td>${deliveryBadge(d)}</td>
      <td>${d.status_code ?? '—'}</td>
      <td>${d.error || ''}</td>
      <td>${d.latency_ms != null ? d.latency_ms + ' ms' : '—'}</td>
      <td>${new Date(d.created_at).toLocaleString()}</td>`;
    tbody.appendChild(tr);
  }
}

async function loadEvents() {
  const tunnel = $('tunnel').value.trim();
  if (!tunnel) return setStatus('Укажите tunnel_id', 'error');
  setStatus('Загрузка…');
  try {
    const data = await api(`/api/v1/tunnels/${encodeURIComponent(tunnel)}/events?limit=50`);
    renderRows(data.events || []);
    setStatus(`Событий: ${data.count ?? 0}`, 'ok');
    selectedId = '';
    $('replay').disabled = true;
    $('details').textContent = 'Выберите событие в таблице';
    renderDeliveries([]);
  } catch (e) {
    setStatus(e.message, 'error');
  }
}

async function selectEvent(id) {
  selectedId = id;
  $('replay').disabled = false;
  setStatus('Загрузка события…');
  try {
    const ev = await api(`/api/v1/events/${encodeURIComponent(id)}`);
    const { deliveries, latest_delivery, ...rest } = ev;
    $('details').textContent = JSON.stringify(rest, null, 2);
    renderDeliveries(deliveries || (latest_delivery ? [latest_delivery] : []));
    const latest = latest_delivery || (deliveries && deliveries[0]);
    if (latest?.status === 'failed') {
      setStatus(`Выбрано: ${id} — доставка не удалась`, 'error');
    } else {
      setStatus(`Выбрано: ${id}`, 'ok');
    }
  } catch (e) {
    setStatus(e.message, 'error');
  }
}

async function replaySelected() {
  if (!selectedId) return;
  setStatus('Replay…');
  try {
    const result = await api(`/api/v1/events/${encodeURIComponent(selectedId)}/replay`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: '*' }),
    });
    setStatus(`Replay поставлен: delivery ${result.delivery_id}`, 'ok');
    await loadEvents();
    await selectEvent(selectedId);
  } catch (e) {
    setStatus(e.message, 'error');
  }
}

$('refresh').onclick = loadEvents;
$('replay').onclick = replaySelected;
loadEvents();
