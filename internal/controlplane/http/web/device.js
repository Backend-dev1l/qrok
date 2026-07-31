const $ = (id) => document.getElementById(id);

function setStatus(msg, cls = '') {
  const el = $('status');
  el.textContent = msg;
  el.className = 'status ' + cls;
}

$('approve').onclick = async () => {
  const userCode = $('user-code').value.trim();
  const projectID = $('project-id').value.trim();
  if (!userCode || !projectID) {
    return setStatus('Укажите user code и project_id', 'error');
  }

  setStatus('Отправка…');
  try {
    const res = await fetch('/api/v1/oauth/device/approve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ user_code: userCode, project_id: projectID }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      const msg = body?.error?.message || body?.error_description || res.statusText;
      throw new Error(msg);
    }
    setStatus('Устройство подтверждено. Вернитесь в терминал с qrok login.', 'ok');
  } catch (e) {
    setStatus(e.message, 'error');
  }
};

// Автоподстановка user_code из query ?code=
const params = new URLSearchParams(window.location.search);
if (params.get('code')) {
  $('user-code').value = params.get('code');
}
