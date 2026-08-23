const $ = (id) => document.getElementById(id);
const tokenStorageKey = 'qrok_access_token';

$('token').value = sessionStorage.getItem(tokenStorageKey) || '';

function setStatus(msg, cls = '') {
  const el = $('status');
  el.textContent = msg;
  el.className = 'status ' + cls;
}

$('approve').onclick = async () => {
  const userCode = $('user-code').value.trim();
  const projectID = $('project-id').value.trim();
  const token = $('token').value.trim();
  if (!userCode) {
    return setStatus('Укажите user code', 'error');
  }
  if (token) {
    sessionStorage.setItem(tokenStorageKey, token);
  }

  setStatus('Отправка…');
  try {
    const res = await fetch('/api/v1/oauth/device/approve', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
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
