// When a live stream marked data-reload-on-401 fails, ask the stream URL once whether the session
// is gone (the server answers 401 instead of redirecting a stream to /login). If it is, reload so
// the operator lands on the login page instead of a page stuck in "Reconnecting".
(function () {
  function probe(evt) {
    var el = evt.target && evt.target.closest ? evt.target.closest('[data-reload-on-401]') : null;
    if (!el || el.dataset.authProbe === '1') return;
    var url = el.getAttribute('sse-connect');
    if (!url) return;
    el.dataset.authProbe = '1';
    var ctl = new AbortController();
    fetch(url, {
      headers: { Accept: 'text/event-stream' },
      credentials: 'same-origin',
      redirect: 'manual',
      signal: ctl.signal
    }).then(function (r) {
      ctl.abort(); // headers are enough; never keep a second stream open
      if (r.status === 401) window.location.reload();
    }).catch(function () {
      // server down: htmx keeps reconnecting and the banner stays
    }).finally(function () {
      el.dataset.authProbe = '';
    });
  }
  document.addEventListener('htmx:sseError', probe);
})();
