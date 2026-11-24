/* Deloittimeter - live polls, quizzes and Q&A. Created by Deloitte HC, OWT, Israel. */
(() => {
'use strict';
const TEAM = 'Deloitte HC, OWT, Israel';
const BROKERS = ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt', 'wss://mqtt-dashboard.com:8884/mqtt', 'wss://test.mosquitto.org:8081'];
/* Some mobile carriers and Wi-Fi networks block the non-standard WebSocket ports above, so a plain HTTPS (port 443) relay runs alongside them. */
const NTFY = 'https://ntfy.sh/';
const NS = 'deloittimeter/v1/';
const COL = ['#86BC25', '#0097A9', '#046A38', '#00A3E0', '#012169', '#26890D', '#53565A', '#75787B'];
const TYPES = ['mc', 'quiz', 'cloud', 'open', 'scale', 'rank', 'points', 'qa', 'content'];
const OPT_TYPES = ['mc', 'quiz', 'scale', 'rank', 'points'];

/* ---------- helpers ---------- */
const store = {
  get(k) { try { return JSON.parse(localStorage.getItem(k)); } catch (e) { return null; } },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} },
  del(k) { try { localStorage.removeItem(k); } catch (e) {} }
};
function h(tag, props, ...kids) {
  const e = document.createElement(tag);
  for (const k in (props || {})) {
    const v = props[k];
    if (v == null || v === false) continue;
    if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
    else if (k === 'class') e.className = v;
    else if (k === 'text') e.textContent = v;
    else if (k === 'value') e.value = v;
    else e.setAttribute(k, v === true ? '' : v);
  }
  for (const c of kids.flat(3)) { if (c == null || c === false) continue; e.append(c.nodeType ? c : document.createTextNode(String(c))); }
  return e;
}
const _rc = Element.prototype.replaceChildren;
Element.prototype.replaceChildren = function (...k) { _rc.apply(this, k.flat(3).filter(x => x != null && x !== false)); };
const $ = (s, r) => (r || document).querySelector(s);
function hex(n) { const a = new Uint8Array(n); crypto.getRandomValues(a); return [...a].map(x => (x % 16).toString(16)).join(''); }
function digits(n) { const a = new Uint8Array(n); crypto.getRandomValues(a); return [...a].map((x, i) => (i === 0 ? 1 + (x % 9) : x % 10)).join(''); }
const clamp = (x, a, b) => Math.min(b, Math.max(a, x));

let lang = store.get('dm_lang');
if (!lang || !LANGS[lang]) { const n = (navigator.language || 'en').slice(0, 2).toLowerCase(); lang = LANGS[n] ? n : 'en'; }
function t(k, v) {
  let s = (I18N[lang] && I18N[lang][k]) || I18N.en[k] || k;
  if (v) for (const x in v) s = s.replace('{' + x + '}', v[x]);
  return s;
}
function tnode(k, map) { // string with {team} replaced by a node
  const s = t(k); const out = []; const re = /\{(\w+)\}/g; let last = 0, m;
  while ((m = re.exec(s))) { out.push(s.slice(last, m.index)); out.push(map[m[1]]); last = m.index + m[0].length; }
  out.push(s.slice(last)); return out;
}
const dir = () => LANGS[lang].dir;
function toast(msg) { const e = h('div', { class: 'toast', role: 'status' }, msg); document.body.append(e); setTimeout(() => e.remove(), 3500); }

/* ---------- networking (public MQTT relays, dual-connected, de-duplicated) ---------- */
function Net(room, side, onMsg, onStatus) {
  const clients = []; const seen = new Set(); let up = 0;
  const sub = NS + room + '/' + (side === 'host' ? 'i' : 's');
  const status = () => { up = clients.filter(c => c.connected).length; onStatus && onStatus(up); };
  const tname = r => 'deloittimeter-v1-' + room + '-' + r;
  const deliver = txt => {
    let m; try { m = JSON.parse(txt); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.m) { if (seen.has(m.m)) return; seen.add(m.m); if (seen.size > 1500) seen.delete(seen.values().next().value); }
    try { onMsg(m); } catch (e) { console.error(e); }
  };
  let nt = null;
  if (typeof EventSource !== 'undefined' && typeof fetch !== 'undefined') {
    const es = new EventSource(NTFY + tname(side === 'host' ? 'i' : 's') + '/sse' + (side === 'host' ? '' : '?since=10m'));
    nt = { connected: false, es, end() { es.close(); } };
    es.onopen = () => { nt.connected = true; status(); onStatus && onStatus(up, true); };
    es.onerror = () => { nt.connected = false; status(); };
    es.onmessage = ev => { let d; try { d = JSON.parse(ev.data); } catch (e) { return; } if (d && d.event === 'message' && d.message) deliver(d.message); };
    clients.push(nt);
  }
  BROKERS.forEach(url => {
    let c;
    try { c = mqtt.connect(url, { clientId: 'dm_' + hex(14), connectTimeout: 10000, reconnectPeriod: 3000, clean: true, keepalive: 30 }); } catch (e) { return; }
    clients.push(c);
    c.on('connect', () => { c.subscribe(sub, { qos: 0 }, () => {}); status(); onStatus && onStatus(up, true); });
    c.on('close', status); c.on('offline', status); c.on('error', () => {});
    c.on('message', (topic, buf) => {
      deliver(buf.toString());
    });
  });
  return {
    pub(obj, retain) {
      if (!obj.m) obj.m = hex(12);
      const topic = NS + room + '/' + (side === 'host' ? 's' : 'i');
      const s = JSON.stringify(obj);
      clients.forEach(c => { try { if (c === nt) return; c.publish(topic, s, { qos: 0, retain: !!retain }); } catch (e) {} });
      if (nt && s.length < 3800) { try { fetch(NTFY + tname(side === 'host' ? 's' : 'i'), { method: 'POST', body: s }).catch(() => {}); } catch (e) {} }
    },
    clear() { clients.forEach(c => { try { if (c !== nt) c.publish(NS + room + '/s', '', { qos: 0, retain: true }); } catch (e) {} }); },
    close() { clients.forEach(c => { try { c === nt ? c.end() : c.end(true); } catch (e) {} }); },
    up: () => up
  };
}

/* ---------- deck model ---------- */
function newSlide(type) {
  const s = { id: hex(6), type, q: '', opts: [], correct: 0, multi: false, min: 1, max: 5, minL: '', maxL: '', words: 3, body: '' };
  if (OPT_TYPES.includes(type)) s.opts = ['', ''];
  if (type === 'scale') s.opts = [''];
  return s;
}
let deck = store.get('dm_deck') || { title: '', slides: [newSlide('mc')] };
const saveDeck = () => store.set('dm_deck', deck);
function cleanSlides(slides) {
  const out = [];
  slides.forEach(s => {
    const c = JSON.parse(JSON.stringify(s));
    c.q = (c.q || '').trim();
    if (OPT_TYPES.includes(c.type)) {
      const keep = []; let nc = 0;
      c.opts.forEach((o, i) => { if ((o || '').trim()) { if (i === c.correct) nc = keep.length; keep.push(o.trim()); } });
      c.opts = keep; c.correct = nc;
      if (c.type === 'scale' ? keep.length < 1 : keep.length < 2) return;
    }
    if (c.type === 'scale') { c.min = Number(c.min) || 1; c.max = Number(c.max) || 5; if (c.max <= c.min) c.max = c.min + 1; if (c.max - c.min > 20) c.max = c.min + 20; }
    c.words = clamp(parseInt(c.words) || 3, 1, 10);
    if (c.type !== 'content' && !c.q) return;
    if (c.type === 'content' && !c.q && !(c.body || '').trim()) return;
    out.push(c);
  });
  return out;
}

/* ---------- host session ---------- */
let S = null, NET = null, connUp = 0, pubTimer = null, saveTimer = null;
function persistSession() { clearTimeout(saveTimer); saveTimer = setTimeout(() => S && store.set('dm_sess', S), 250); }
function startSession(slides) {
  S = { room: digits(8), slides, cur: 0, open: true, reveal: false, resp: {}, qa: {}, peers: {}, created: Date.now() };
  connectHost(); persistSession(); location.hash = '#/present';
}
function connectHost() {
  if (NET) NET.close();
  NET = Net(S.room, 'host', onHostMsg, (n, fresh) => { connUp = n; if (fresh) publishState(); updateConn(); });
  publishState();
}
function curSlide() { return S && S.slides[S.cur]; }
function sanitize(sl, d) {
  if (!d || typeof d !== 'object') return null;
  const nOpt = (sl.opts || []).length;
  switch (sl.type) {
    case 'mc': { let a = Array.isArray(d.s) ? d.s.map(Number).filter(i => Number.isInteger(i) && i >= 0 && i < nOpt) : []; a = [...new Set(a)]; if (!sl.multi) a = a.slice(0, 1); return a.length ? { s: a } : null; }
    case 'quiz': { const i = Number(d.s); return Number.isInteger(i) && i >= 0 && i < nOpt ? { s: i } : null; }
    case 'cloud': { const w = (Array.isArray(d.w) ? d.w : []).map(x => String(x).trim().slice(0, 30)).filter(Boolean).slice(0, sl.words || 3); return w.length ? { w } : null; }
    case 'open': { const x = String(d.x || '').trim().slice(0, 300); return x ? { x } : null; }
    case 'scale': { const v = (Array.isArray(d.v) ? d.v : []).map(Number); if (v.length !== nOpt || v.some(n => !Number.isFinite(n))) return null; return { v: v.map(n => clamp(Math.round(n), sl.min, sl.max)) }; }
    case 'rank': { const r = (Array.isArray(d.r) ? d.r : []).map(Number); if (r.length !== nOpt || new Set(r).size !== nOpt || r.some(i => !Number.isInteger(i) || i < 0 || i >= nOpt)) return null; return { r }; }
    case 'points': { const p = (Array.isArray(d.p) ? d.p : []).map(n => Math.max(0, Math.floor(Number(n) || 0))); if (p.length !== nOpt || p.reduce((a, b) => a + b, 0) > 100) return null; return { p }; }
  }
  return null;
}
function onHostMsg(m) {
  if (!S || !m.pid) return;
  const pid = String(m.pid).slice(0, 12), name = String(m.name || '').slice(0, 30);
  if (!(pid in S.peers) || (name && S.peers[pid] !== name)) S.peers[pid] = name || S.peers[pid] || '';
  const sl = curSlide();
  if (m.t === 'vote' && sl && S.open && m.sid === sl.id) {
    const d = sanitize(sl, m.d); if (!d) return;
    const r = S.resp[sl.id] || (S.resp[sl.id] = {});
    if (sl.type === 'open') { const a = r[pid] ? r[pid].items : []; if (a.length < 3) a.push(d.x); r[pid] = { items: a }; }
    else if (sl.type === 'quiz') { if (!r[pid]) r[pid] = d; }
    else r[pid] = d;
  } else if (m.t === 'q' && sl && sl.type === 'qa' && m.sid === sl.id) {
    const text = String(m.x || '').trim().slice(0, 300); if (!text) return;
    const l = S.qa[sl.id] || (S.qa[sl.id] = []);
    if (l.length < 300 && !l.some(q => q.id === m.qid)) l.push({ id: String(m.qid || hex(6)).slice(0, 12), text, votes: [], done: false });
  } else if (m.t === 'qup' && sl && sl.type === 'qa') {
    const q = (S.qa[sl.id] || []).find(q => q.id === m.qid); if (q && !q.votes.includes(pid)) q.votes.push(pid);
  }
  persistSession(); schedulePublish(); if (typeof refreshPresenter === 'function') refreshPresenter();
}
function qaList(sid) {
  return (S.qa[sid] || []).map(q => ({ id: q.id, text: q.text, v: q.votes.length, done: q.done })).sort((a, b) => (a.done - b.done) || (b.v - a.v)).slice(0, 80);
}
function leaderboard() {
  const sc = {};
  S.slides.forEach((sl, i) => {
    if (sl.type !== 'quiz' || i > S.cur || (i === S.cur && !S.reveal)) return;
    const r = S.resp[sl.id] || {};
    Object.keys(r).forEach(pid => { sc[pid] = (sc[pid] || 0) + (r[pid].s === sl.correct ? 100 : 0); });
  });
  return Object.keys(sc).map(pid => ({ name: S.peers[pid] || '#' + pid.slice(-3), score: sc[pid] })).sort((a, b) => b.score - a.score);
}
function stateMsg() {
  const sl = curSlide();
  const o = { v: Date.now(), n: Object.keys(S.peers).length, total: S.slides.length, cur: S.cur, open: S.open, reveal: S.reveal, title: deck.title || '' };
  if (sl) {
    o.slide = { id: sl.id, type: sl.type, q: sl.q, opts: sl.opts, multi: sl.multi, min: sl.min, max: sl.max, minL: sl.minL, maxL: sl.maxL, words: sl.words, body: sl.body };
    o.ans = Object.keys(S.resp[sl.id] || {});
    if (sl.type === 'quiz' && S.reveal) { o.correct = sl.correct; o.lb = leaderboard().slice(0, 10); }
    if (sl.type === 'qa') o.qa = qaList(sl.id);
  }
  return o;
}
function publishState() { if (NET && S) NET.pub(stateMsg(), true); }
function schedulePublish() { clearTimeout(pubTimer); pubTimer = setTimeout(publishState, 350); }
function endSession() {
  if (NET) { NET.pub({ ended: true, v: Date.now() }, false); NET.clear(); setTimeout(() => { NET && NET.close(); NET = null; }, 800); }
  S = null; store.del('dm_sess'); location.hash = '#/';
}

/* ---------- aggregation ---------- */
function agg(sl) {
  const R = Object.values(S.resp[sl.id] || {}); const n = R.length; const k = (sl.opts || []).length;
  if (sl.type === 'mc' || sl.type === 'quiz') {
    const c = new Array(k).fill(0);
    R.forEach(r => (sl.type === 'mc' ? r.s : [r.s]).forEach(i => c[i]++));
    return { n, counts: c };
  }
  if (sl.type === 'cloud') {
    const m = new Map();
    R.forEach(r => r.w.forEach(w => { const key = w.toLowerCase(); const e = m.get(key); if (e) e.c++; else m.set(key, { w, c: 1 }); }));
    return { n, words: [...m.values()].sort((a, b) => b.c - a.c).slice(0, 80) };
  }
  if (sl.type === 'open') return { n, items: R.flatMap(r => r.items).reverse() };
  if (sl.type === 'scale') return { n, avg: sl.opts.map((_, i) => n ? R.reduce((a, r) => a + r.v[i], 0) / n : 0) };
  if (sl.type === 'rank') return { n, score: sl.opts.map((_, i) => n ? R.reduce((a, r) => a + (k - r.r.indexOf(i)), 0) / n : 0) };
  if (sl.type === 'points') return { n, avg: sl.opts.map((_, i) => n ? R.reduce((a, r) => a + r.p[i], 0) / n : 0) };
  return { n };
}
const fmt = x => (Math.round(x * 10) / 10).toString();

/* ---------- chrome (header/footer) ---------- */
function renderChrome() {
  document.documentElement.lang = lang; document.documentElement.dir = dir();
  $('#skip').textContent = '↓'; $('#skip').setAttribute('aria-label', t('home'));
  const sel = h('select', { id: 'langsel', 'aria-label': t('lang'), onchange: e => { lang = e.target.value; store.set('dm_lang', lang); renderChrome(); route(); } },
    Object.keys(LANGS).map(k => h('option', { value: k, selected: k === lang }, LANGS[k].n)));
  const top = $('#top'); top.replaceChildren(
    h('a', { class: 'brand', href: '#/', 'aria-label': 'Deloittimeter - ' + t('home') },
      h('img', { class: 'logo', src: 'deloitte.svg', alt: 'Deloitte' }), h('span', { class: 'sep', 'aria-hidden': 'true' }), h('span', { dir: 'ltr' }, 'Deloittimeter')),
    h('span', { class: 'sp' }), h('label', {}, h('span', {}, t('lang')), sel));
  $('#foot').replaceChildren(h('small', {}, tnode('credit', { team: h('b', { dir: 'ltr', style: 'unicode-bidi:isolate' }, TEAM) })));
  document.title = 'Deloittimeter';
}

/* ---------- routing ---------- */
let refreshPresenter = null, partCleanup = null;
function route() {
  if (partCleanup) { partCleanup(); partCleanup = null; }
  refreshPresenter = null;
  const app = $('#app'); app.className = '';
  const hash = location.hash || '#/';
  let m;
  if ((m = hash.match(/^#\/j\/(\d{8})$/))) return viewJoin(app, m[1]);
  if (hash === '#/edit') return viewEditor(app);
  if (hash === '#/present') return S ? viewPresenter(app) : (location.hash = '#/');
  return viewHome(app);
}

/* ---------- home ---------- */
function viewHome(app) {
  const code = h('input', { type: 'text', inputmode: 'numeric', autocomplete: 'off', maxlength: '8', class: 'code', id: 'jc', 'aria-label': t('code'), placeholder: '12345678' });
  const nm = h('input', { type: 'text', id: 'jn', maxlength: '30', autocomplete: 'nickname', value: store.get('dm_name') || '', dir: 'auto' });
  const go = () => {
    const c = code.value.replace(/\D/g, '');
    if (c.length !== 8) { toast(t('p_bad_code')); code.focus(); return; }
    store.set('dm_name', nm.value.trim()); location.hash = '#/j/' + c;
  };
  const saved = store.get('dm_sess');
  app.replaceChildren(
    h('section', { class: 'hero' }, h('h1', {}, h('span', { dir: 'ltr', style: 'unicode-bidi:isolate;display:inline-block' }, 'Deloitt', h('i', {}, 'i'), 'meter')), h('p', {}, t('tagline'))),
    saved ? h('div', { class: 'card', style: 'margin-block-end:20px' }, h('div', { class: 'row' }, h('div', { style: 'flex:1' }, h('b', {}, t('resume')), h('div', { class: 'mut small' }, t('resume_desc'))),
      h('button', { class: 'btn pri', onclick: () => { S = saved; connectHost(); location.hash = '#/present'; } }, t('resume')))) : null,
    h('div', { class: 'grid2' },
      h('section', { class: 'card' }, h('h2', {}, t('create')), h('p', { class: 'mut' }, t('create_desc')), h('a', { class: 'btn pri', href: '#/edit' }, t('create'), ' ', h('span', { class: 'flip', 'aria-hidden': 'true' }, '→'))),
      h('section', { class: 'card' }, h('h2', {}, t('join')), h('p', { class: 'mut' }, t('join_desc')),
        h('form', { onsubmit: e => { e.preventDefault(); go(); } },
          h('label', { class: 'l', for: 'jc' }, t('code')), code,
          h('label', { class: 'l', for: 'jn' }, t('name_opt')), nm,
          h('p', {}, h('button', { class: 'btn dark', type: 'submit' }, t('go')))))),
    h('p', { class: 'note' }, t('privacy_note')));
}

/* ---------- editor ---------- */
let sel = 0;
function viewEditor(app) {
  if (sel >= deck.slides.length) sel = deck.slides.length - 1;
  const list = h('ul', { class: 'slist' });
  const main = h('div', { class: 'card' });
  function typeLabel(ty) { return t('type_' + ty); }
  function renderList() {
    list.replaceChildren(...deck.slides.map((s, i) => h('li', {}, h('button', { class: 'it', 'aria-current': i === sel ? 'true' : 'false', onclick: () => { sel = i; renderList(); renderMain(); } },
      h('span', { class: 'no' }, i + 1), h('span', { class: 'tt' }, h('b', { dir: 'auto' }, s.q || t('slide_no_q')), h('small', {}, typeLabel(s.type)))))));
  }
  function save() { saveDeck(); }
  function add(type) { deck.slides.splice(sel + 1, 0, newSlide(type)); sel = sel + 1; save(); renderList(); renderMain(); }
  function optEditor(s, labelKey, addKey, withCorrect) {
    const box = h('div');
    const draw = () => {
      box.replaceChildren(...s.opts.map((o, i) => h('div', { class: 'optrow' },
        withCorrect ? h('input', { type: 'radio', name: 'corr', checked: s.correct === i, 'aria-label': t('correct'), onchange: () => { s.correct = i; save(); } }) : null,
        h('input', { type: 'text', dir: 'auto', maxlength: '120', value: o, 'aria-label': t(labelKey) + ' ' + (i + 1), placeholder: t(labelKey) + ' ' + (i + 1), oninput: e => { s.opts[i] = e.target.value; save(); } }),
        h('button', { class: 'btn sm ico', 'aria-label': t('remove'), title: t('remove'), disabled: s.opts.length <= (s.type === 'scale' ? 1 : 2), onclick: () => { s.opts.splice(i, 1); if (s.correct >= s.opts.length) s.correct = 0; save(); draw(); } }, '✕'))),
        s.opts.length < 12 ? h('button', { class: 'btn sm', onclick: () => { s.opts.push(''); save(); draw(); } }, '+ ' + t(addKey)) : null);
    };
    draw(); return box;
  }
  function renderMain() {
    const s = deck.slides[sel]; if (!s) return;
    const move = d => { const j = sel + d; if (j < 0 || j >= deck.slides.length) return; [deck.slides[sel], deck.slides[j]] = [deck.slides[j], deck.slides[sel]]; sel = j; save(); renderList(); renderMain(); };
    const tb = h('div', { class: 'row', style: 'margin-block-end:8px' },
      h('b', {}, typeLabel(s.type)), h('span', { style: 'flex:1' }),
      h('button', { class: 'btn sm ico', disabled: sel === 0, 'aria-label': t('move_up'), title: t('move_up'), onclick: () => move(-1) }, '↑'),
      h('button', { class: 'btn sm ico', disabled: sel === deck.slides.length - 1, 'aria-label': t('move_down'), title: t('move_down'), onclick: () => move(1) }, '↓'),
      h('button', { class: 'btn sm', onclick: () => { const c = JSON.parse(JSON.stringify(s)); c.id = hex(6); deck.slides.splice(sel + 1, 0, c); sel++; save(); renderList(); renderMain(); } }, t('duplicate')),
      h('button', { class: 'btn sm danger', onclick: () => { deck.slides.splice(sel, 1); if (!deck.slides.length) deck.slides.push(newSlide('mc')); sel = clamp(sel, 0, deck.slides.length - 1); save(); renderList(); renderMain(); } }, t('delete')));
    const q = h('input', { type: 'text', id: 'eq', dir: 'auto', maxlength: '200', value: s.q, oninput: e => { s.q = e.target.value; save(); renderList(); } });
    const parts = [tb, h('label', { class: 'l', for: 'eq' }, s.type === 'content' ? t('question') : t('question')), q];
    if (s.type === 'mc') parts.push(h('label', { class: 'l' }, t('option')), optEditor(s, 'option', 'add_option'), h('label', { class: 'row' }, h('input', { type: 'checkbox', checked: s.multi, onchange: e => { s.multi = e.target.checked; save(); } }), t('multi')));
    if (s.type === 'quiz') parts.push(h('label', { class: 'l' }, t('option') + ' / ' + t('correct')), optEditor(s, 'option', 'add_option', true));
    if (s.type === 'rank' || s.type === 'points') parts.push(h('label', { class: 'l' }, t('option')), optEditor(s, 'option', 'add_option'));
    if (s.type === 'scale') parts.push(h('label', { class: 'l' }, t('statement')), optEditor(s, 'statement', 'add_statement'),
      h('div', { class: 'row' }, h('label', {}, t('scale_min') + ' '), h('input', { type: 'number', style: 'width:90px', value: s.min, 'aria-label': t('scale_min'), oninput: e => { s.min = parseInt(e.target.value) || 1; save(); } }),
        h('label', {}, t('scale_max') + ' '), h('input', { type: 'number', style: 'width:90px', value: s.max, 'aria-label': t('scale_max'), oninput: e => { s.max = parseInt(e.target.value) || 5; save(); } })),
      h('label', { class: 'l' }, t('label_min')), h('input', { type: 'text', dir: 'auto', maxlength: '40', value: s.minL, oninput: e => { s.minL = e.target.value; save(); } }),
      h('label', { class: 'l' }, t('label_max')), h('input', { type: 'text', dir: 'auto', maxlength: '40', value: s.maxL, oninput: e => { s.maxL = e.target.value; save(); } }));
    if (s.type === 'cloud') parts.push(h('label', { class: 'l', for: 'ew' }, t('max_words')), h('input', { type: 'number', id: 'ew', min: '1', max: '10', style: 'width:90px', value: s.words, oninput: e => { s.words = clamp(parseInt(e.target.value) || 3, 1, 10); save(); } }));
    if (s.type === 'content') parts.push(h('label', { class: 'l', for: 'eb' }, t('body')), h('textarea', { id: 'eb', dir: 'auto', rows: '6', oninput: e => { s.body = e.target.value; save(); } }, s.body));
    main.replaceChildren(...parts);
  }
  const ttl = h('input', { type: 'text', id: 'dt', dir: 'auto', maxlength: '100', value: deck.title, placeholder: t('untitled'), oninput: e => { deck.title = e.target.value; save(); } });
  const file = h('input', { type: 'file', accept: '.json,application/json', style: 'display:none', onchange: async e => {
    try { const d = JSON.parse(await e.target.files[0].text()); if (!d || !Array.isArray(d.slides)) throw 0; deck = { title: String(d.title || ''), slides: d.slides.filter(s => TYPES.includes(s.type)).map(s => Object.assign(newSlide(s.type), s)) }; if (!deck.slides.length) deck.slides = [newSlide('mc')]; sel = 0; save(); viewEditor(app); } catch (err) { toast(t('bad_file')); }
  } });
  const typeBtns = h('div', { class: 'typegrid', id: 'types', hidden: true }, TYPES.map(ty => h('button', { class: 'btn sm', onclick: () => { typeBtns.hidden = true; add(ty); } }, typeLabel(ty))));
  app.replaceChildren(
    h('div', { class: 'row', style: 'margin-block-end:16px' }, h('div', { style: 'flex:1;min-width:240px' }, h('label', { class: 'l', for: 'dt', style: 'margin-top:0' }, t('editor_title')), ttl),
      h('button', { class: 'btn pri', onclick: () => { const sl = cleanSlides(deck.slides); if (!sl.length) { toast(t('need_one_slide')); return; } startSession(sl); } }, '▶ ' + t('start'))),
    h('div', { class: 'ed' },
      h('aside', {}, h('h2', {}, t('slides')), list, h('p', {}, h('button', { class: 'btn', onclick: () => { typeBtns.hidden = !typeBtns.hidden; } }, '+ ' + t('add_slide')), typeBtns),
        h('div', { class: 'row' }, h('button', { class: 'btn sm', onclick: () => { const b = new Blob([JSON.stringify(deck, null, 1)], { type: 'application/json' }); const a = h('a', { href: URL.createObjectURL(b), download: 'deloittimeter.json' }); document.body.append(a); a.click(); a.remove(); } }, t('export_deck')),
          h('button', { class: 'btn sm', onclick: () => file.click() }, t('import_deck')), file)),
      main));
  renderList(); renderMain();
}

/* ---------- presenter ---------- */
function joinUrl() { return location.origin + location.pathname + '#/j/' + S.room; }
function qrSvg(text) { const q = qrcode(0, 'M'); q.addData(text); q.make(); return q.createSvgTag({ cellSize: 4, margin: 2, scalable: true }); }
function updateConn() { const e = $('#conn'); if (e) e.replaceChildren(h('span', { class: 'dot' + (connUp ? ' ok' : ''), 'aria-hidden': 'true' }), connUp ? t('conn_ok') : t('conn_bad')); }
function renderResults(sl, box) {
  const a = agg(sl); box.replaceChildren();
  if (sl.type === 'content') { box.append(h('div', { class: 'bodytxt', dir: 'auto' }, sl.body)); return; }
  if (sl.type === 'qa') {
    const l = (S.qa[sl.id] || []).slice().sort((x, y) => (x.done - y.done) || (y.votes.length - x.votes.length));
    if (!l.length) { box.append(h('p', { class: 'mut' }, t('qa_empty'))); return; }
    l.forEach(q => box.append(h('div', { class: 'qa-item' + (q.done ? ' done' : '') }, h('div', { class: 'up' }, h('b', {}, q.votes.length), h('small', {}, t('p_votes'))),
      h('div', { class: 'tx', dir: 'auto' }, q.text, q.done ? h('div', {}, h('span', { class: 'pill on' }, t('answered_tag'))) : null),
      h('div', { class: 'row' }, h('button', { class: 'btn sm', onclick: () => { q.done = !q.done; persistSession(); publishState(); renderResults(sl, box); } }, t('mark_answered')),
        h('button', { class: 'btn sm ico danger', 'aria-label': t('delete'), onclick: () => { S.qa[sl.id] = S.qa[sl.id].filter(x => x !== q); persistSession(); publishState(); renderResults(sl, box); } }, '✕')))));
    return;
  }
  if (!a.n) { box.append(h('p', { class: 'mut', style: 'font-size:1.2rem' }, t('no_responses'))); return; }
  if (sl.type === 'mc' || sl.type === 'quiz' || sl.type === 'rank' || sl.type === 'points') {
    const vals = sl.type === 'rank' ? a.score : sl.type === 'points' ? a.avg : a.counts;
    const mx = Math.max(...vals, 0.0001); const tot = sl.type === 'mc' || sl.type === 'quiz' ? a.counts.reduce((x, y) => x + y, 0) : 0;
    box.append(h('div', { class: 'bars' }, sl.opts.map((o, i) => h('div', { class: 'bar' + (sl.type === 'quiz' && S.reveal && i === sl.correct ? ' corr' : '') },
      h('div', { class: 'lb', dir: 'auto' }, o), h('div', { class: 'tr' }, h('div', { class: 'fl', style: 'width:' + (vals[i] / mx * 100) + '%;background:' + COL[i % COL.length] })),
      h('div', { class: 'vl', dir: 'ltr' }, (sl.type === 'mc' || sl.type === 'quiz') ? vals[i] + (tot ? ' · ' + Math.round(vals[i] / tot * 100) + '%' : '') : fmt(vals[i]))))));
  } else if (sl.type === 'cloud') {
    const mx = a.words[0] ? a.words[0].c : 1;
    box.append(h('div', { class: 'cloud', dir: 'auto' }, a.words.map((w, i) => h('span', { dir: 'auto', style: 'font-size:' + (1 + 3.4 * Math.pow(w.c / mx, .8)) + 'rem;color:' + COL[i % COL.length] }, w.w))));
  } else if (sl.type === 'open') {
    box.append(h('div', { class: 'cards' }, a.items.map(x => h('div', { class: 'c', dir: 'auto' }, x))));
  } else if (sl.type === 'scale') {
    sl.opts.forEach((o, i) => box.append(h('div', { class: 'sc' }, h('div', { class: 'bar', style: 'grid-template-columns:1fr auto' }, h('div', { class: 'lb', dir: 'auto' }, o), h('div', { class: 'vl', dir: 'ltr' }, t('avg') + ' ' + fmt(a.avg[i]))),
      h('div', { class: 'tr' }, h('div', { class: 'mk', style: 'inset-inline-start:' + ((a.avg[i] - sl.min) / (sl.max - sl.min) * 100) + '%', dir: 'ltr' }, fmt(a.avg[i]))),
      h('div', { class: 'ends' }, h('span', { dir: 'auto' }, sl.min + (sl.minL ? ' · ' + sl.minL : '')), h('span', { dir: 'auto' }, (sl.maxL ? sl.maxL + ' · ' : '') + sl.max)))));
  }
}
function csv() {
  const rows = [['slide', 'type', 'question', 'item', 'value']];
  S.slides.forEach((sl, i) => {
    const a = agg(sl);
    if (sl.type === 'mc' || sl.type === 'quiz') sl.opts.forEach((o, j) => rows.push([i + 1, sl.type, sl.q, o, a.counts[j]]));
    else if (sl.type === 'rank') sl.opts.forEach((o, j) => rows.push([i + 1, sl.type, sl.q, o, fmt(a.score[j])]));
    else if (sl.type === 'points' || sl.type === 'scale') sl.opts.forEach((o, j) => rows.push([i + 1, sl.type, sl.q, o, fmt(a.avg[j])]));
    else if (sl.type === 'cloud') a.words.forEach(w => rows.push([i + 1, sl.type, sl.q, w.w, w.c]));
    else if (sl.type === 'open') a.items.forEach(x => rows.push([i + 1, sl.type, sl.q, x, 1]));
    else if (sl.type === 'qa') (S.qa[sl.id] || []).forEach(q => rows.push([i + 1, sl.type, sl.q, q.text, q.votes.length]));
  });
  const esc = v => { v = String(v); if (/^[=+\-@]/.test(v)) v = "'" + v; return '"' + v.replace(/"/g, '""') + '"'; };
  const b = new Blob(['\ufeff' + rows.map(r => r.map(esc).join(',')).join('\n')], { type: 'text/csv;charset=utf-8' });
  const a = h('a', { href: URL.createObjectURL(b), download: 'deloittimeter-results.csv' }); document.body.append(a); a.click(); a.remove();
}
function viewPresenter(app) {
  app.className = 'wide';
  const url = joinUrl(); const shown = url.replace(/^https?:\/\//, '').replace(/#.*/, '');
  const qr = h('div', { class: 'qr', role: 'img', 'aria-label': 'QR', tabindex: '0', onclick: e => e.currentTarget.classList.toggle('big'), onkeydown: e => { if (e.key === 'Enter') e.currentTarget.classList.toggle('big'); } }); qr.innerHTML = qrSvg(url);
  const stage = h('div', { class: 'pstage' }); const bar = h('div', { class: 'pbar' });
  const go = d => { const j = S.cur + d; if (j < 0 || j >= S.slides.length) return; S.cur = j; S.open = true; S.reveal = false; persistSession(); publishState(); draw(); };
  const nextDir = dir() === 'rtl' ? -1 : 1;
  function draw() {
    const sl = curSlide(); const box = h('div', { 'aria-live': 'polite' });
    const a = sl ? agg(sl) : { n: 0 };
    stage.replaceChildren(
      h('div', { class: 'pmeta' }, h('span', {}, t('slide_of', { a: S.cur + 1, b: S.slides.length })), h('span', {}, '·'), h('span', {}, t('type_' + sl.type)),
        sl.type !== 'content' ? h('span', { class: 'pill ' + (S.open ? 'on' : 'off') }, S.open ? t('voting_open') : t('voting_closed')) : null,
        sl.type !== 'content' && sl.type !== 'qa' ? h('span', { id: 'cnt' }, a.n + ' ' + t('responses')) : null, h('span', { id: 'ppl' }, Object.keys(S.peers).length + ' ' + t('participants')), h('span', { id: 'conn' })),
      sl.type === 'content' ? h('h1', { class: 'pq', dir: 'auto' }, sl.q) : h('h1', { class: 'pq', dir: 'auto' }, sl.q), box);
    renderResults(sl, box);
    if (sl.type === 'quiz' && S.reveal) { const lb = leaderboard().slice(0, 5); if (lb.length) box.append(h('h2', { style: 'margin-top:24px' }, t('p_leaderboard')), h('div', { class: 'lbd' }, lb.map((r, i) => h('div', { class: 'r' }, h('span', {}, i + 1 + '.'), h('b', { dir: 'auto' }, r.name), h('span', { dir: 'ltr' }, r.score + ' ' + t('p_points')))))); }
    updateConn();
    bar.replaceChildren(
      h('button', { class: 'btn', disabled: S.cur === 0, onclick: () => go(-1) }, h('span', { class: 'flip', 'aria-hidden': 'true' }, '←'), ' ' + t('prev')),
      h('button', { class: 'btn pri', disabled: S.cur >= S.slides.length - 1, onclick: () => go(1) }, t('next') + ' ', h('span', { class: 'flip', 'aria-hidden': 'true' }, '→')),
      sl.type !== 'content' ? h('button', { class: 'btn', onclick: () => { S.open = !S.open; persistSession(); publishState(); draw(); } }, S.open ? t('close_voting') : t('open_voting')) : null,
      sl.type === 'quiz' ? h('button', { class: 'btn', onclick: () => { S.reveal = !S.reveal; persistSession(); publishState(); draw(); } }, S.reveal ? t('hide') : t('reveal')) : null,
      sl.type !== 'content' ? h('button', { class: 'btn', onclick: () => { if (confirm(t('reset_confirm'))) { delete S.resp[sl.id]; if (sl.type === 'qa') delete S.qa[sl.id]; persistSession(); publishState(); draw(); } } }, t('reset')) : null,
      h('span', { class: 'sp' }),
      h('button', { class: 'btn sm', onclick: csv }, t('export_csv')),
      h('button', { class: 'btn sm', onclick: () => { document.fullscreenElement ? document.exitFullscreen() : document.documentElement.requestFullscreen && document.documentElement.requestFullscreen(); } }, t('fullscreen')),
      h('button', { class: 'btn sm danger', onclick: () => { if (confirm(t('end_confirm'))) endSession(); } }, t('end')));
  }
  app.replaceChildren(h('div', { class: 'pres' },
    h('div', { class: 'pjoin' }, h('div', { class: 't' }, t('present_join_at') + ' ', h('span', { class: 'ltr' }, shown), ' ' + t('present_code') + ' ', h('b', { class: 'ltr' }, S.room)), qr), stage, bar));
  refreshPresenter = () => { const sl = curSlide(); if (!sl) return; const box = $('#app .pstage [aria-live]'); if (!box) return; renderResults(sl, box); const a = agg(sl); const c = $('#cnt'); if (c) c.textContent = a.n + ' ' + t('responses'); const p = $('#ppl'); if (p) p.textContent = Object.keys(S.peers).length + ' ' + t('participants'); };
  draw();
  const key = e => { if (/INPUT|TEXTAREA|SELECT/.test(e.target.tagName)) return; if (e.key === 'ArrowRight') go(nextDir); else if (e.key === 'ArrowLeft') go(-nextDir); else if (e.key === 'PageDown') go(1); else if (e.key === 'PageUp') go(-1); };
  document.addEventListener('keydown', key); partCleanup = () => document.removeEventListener('keydown', key);
}

/* ---------- participant ---------- */
function viewJoin(app, room) {
  const name = h('input', { type: 'text', id: 'pn', maxlength: '30', dir: 'auto', autocomplete: 'nickname', value: store.get('dm_name') || '' });
  const start = () => { store.set('dm_name', name.value.trim()); participant(app, room, name.value.trim()); };
  if (store.get('dm_joined_' + room)) return start();
  app.replaceChildren(h('div', { class: 'pwrap' }, h('div', { class: 'card' }, h('h2', {}, t('join')), h('p', { class: 'mut' }, h('span', { class: 'ltr code' }, room)),
    h('form', { onsubmit: e => { e.preventDefault(); store.set('dm_joined_' + room, 1); start(); } }, h('label', { class: 'l', for: 'pn' }, t('name_opt')), name, h('p', {}, h('button', { class: 'btn pri', type: 'submit' }, t('go')))))));
  name.focus();
}
function participant(app, room, name) {
  let pid = store.get('dm_pid'); if (!pid) { pid = hex(8); store.set('dm_pid', pid); }
  let st = null, upN = 0, lastKey = '', seenAny = false; const local = {}; const pending = {}; const ups = new Set(store.get('dm_ups_' + room) || []);
  const view = h('div', { class: 'pwrap', 'aria-live': 'polite' });
  app.replaceChildren(view);
  let net;
  const hi = () => net && net.pub({ t: 'hi', pid, name });
  net = Net(room, 'part', m => { if (m.ended) { st = { ended: true }; seenAny = true; update(); return; } if (!m.slide && m.total == null) return; if (st && st.v && m.v < st.v) return; st = m; if (!seenAny) { seenAny = true; hi(); } update(); },
    (n, fresh) => { upN = n; if (fresh) hi(); update(); });
  const timer = setInterval(() => {
    Object.keys(pending).forEach(sid => { const p = pending[sid]; const answered = st && st.slide && st.slide.id === sid && st.ans && st.ans.includes(pid); if (answered || p.tries > 6) { delete pending[sid]; update(true); return; } p.tries++; net.pub(p.msg); });
    if (!seenAny) update();
  }, 3000);
  partCleanup = () => { clearInterval(timer); net.close(); };
  function send(sl, d) { const msg = { t: 'vote', pid, name, sid: sl.id, d, m: hex(12) }; pending[sl.id] = { msg, tries: 0 }; local[sl.id] = d; net.pub(msg); update(true); }
  function update(force) {
    const sl = st && st.slide; const answered = !!(sl && ((st.ans || []).includes(pid)));
    const key = JSON.stringify([!!seenAny, upN > 0, st && st.ended, sl && sl.id, st && st.open, st && st.reveal, answered, sl && !!pending[sl.id], sl && !!local[sl.id]]);
    if (key !== lastKey || force) { lastKey = key; draw(); }
    else if (sl && sl.type === 'qa') drawQa(); else if (st && st.reveal && st.lb) drawLb();
  }
  let qaBox = null, lbBox = null;
  function drawQa() { if (!qaBox || !st.qa) return; qaBox.replaceChildren(...st.qa.map(q => h('div', { class: 'qa-item' + (q.done ? ' done' : '') }, h('div', { class: 'tx', dir: 'auto' }, q.text, q.done ? h('div', {}, h('span', { class: 'pill on' }, t('answered_tag'))) : null),
    h('div', { class: 'up' }, h('button', { class: 'btn sm ico', disabled: ups.has(q.id), 'aria-label': t('p_upvote'), onclick: () => { ups.add(q.id); store.set('dm_ups_' + room, [...ups]); net.pub({ t: 'qup', pid, name, qid: q.id }); drawQa(); } }, '▲'), h('b', {}, q.v))))); }
  function drawLb() { if (!lbBox || !st.lb) return; lbBox.replaceChildren(h('h2', {}, t('p_leaderboard')), h('div', { class: 'lbd' }, st.lb.map((r, i) => h('div', { class: 'r' }, h('span', {}, i + 1 + '.'), h('b', { dir: 'auto' }, r.name), h('span', { dir: 'ltr' }, r.score))))); }
  function draw() {
    qaBox = null; lbBox = null;
    if (st && st.ended) return view.replaceChildren(h('div', { class: 'card center' }, h('div', { class: 'big' }, '👋'), h('h2', {}, t('p_ended')), h('a', { class: 'btn', href: '#/' }, t('home'))));
    if (!seenAny) return view.replaceChildren(h('div', { class: 'card center' }, h('p', {}, upN ? t('p_wait') : t('p_connecting')), h('p', { class: 'mut small' }, h('span', { class: 'ltr' }, room))));
    const sl = st.slide; if (!sl) return view.replaceChildren(h('div', { class: 'card center' }, h('p', {}, t('p_wait'))));
    const open = st.open; const done = (st.ans || []).includes(pid) || !!pending[sl.id]; const sending = !!pending[sl.id] && !(st.ans || []).includes(pid);
    const head = h('h1', { class: 'q', dir: 'auto' }, sl.q);
    const thanks = () => h('div', { class: 'card center' }, h('div', { class: 'big' }, sending ? '⏳' : '✓'), h('h2', {}, sending ? t('p_sending') : t('p_thanks')), !sending ? h('p', { class: 'mut' }, t('p_received')) : null);
    if (sl.type === 'content') return view.replaceChildren(head, h('div', { class: 'bodytxt', dir: 'auto' }, sl.body));
    if (sl.type === 'qa') {
      const ta = h('textarea', { dir: 'auto', maxlength: '300', placeholder: t('p_ask_ph'), 'aria-label': t('p_ask') });
      qaBox = h('div', { style: 'margin-top:16px' });
      view.replaceChildren(head, open ? h('div', {}, ta, h('p', {}, h('button', { class: 'btn pri', onclick: () => { const x = ta.value.trim(); if (!x) return; net.pub({ t: 'q', pid, name, sid: sl.id, qid: hex(8), x }); ta.value = ''; toast(t('p_thanks')); } }, t('p_submit')))) : h('p', { class: 'mut' }, t('p_closed')), qaBox); drawQa(); return;
    }
    if (!open && !done) return view.replaceChildren(head, h('div', { class: 'card center' }, h('p', {}, t('p_closed'))));
    if (sl.type === 'quiz' && st.reveal) {
      const mine = local[sl.id] ? local[sl.id].s : null; lbBox = h('div', { style: 'margin-top:16px' });
      view.replaceChildren(head, h('div', { class: 'card center' }, mine == null ? h('p', {}, t('correct_is') + ': ', h('b', { dir: 'auto' }, sl.opts[st.correct])) : h('div', {}, h('div', { class: 'big' }, mine === st.correct ? '🎉' : '🤔'), h('h2', { class: mine === st.correct ? 'ok' : 'bad' }, mine === st.correct ? t('p_correct') : t('p_wrong')), h('p', {}, t('correct_is') + ': ', h('b', { dir: 'auto' }, sl.opts[st.correct])))), lbBox); drawLb(); return;
    }
    if (done && !(open && sl.type !== 'quiz' && local[sl.id] === 'edit')) {
      return view.replaceChildren(head, thanks(), open && sl.type !== 'quiz' ? h('p', { class: 'center' }, h('button', { class: 'btn', onclick: () => { local[sl.id] = 'edit'; delete pending[sl.id]; draw(); } }, t('p_again'))) : null);
    }
    const prev = local[sl.id] && typeof local[sl.id] === 'object' ? local[sl.id] : null;
    const submit = (fn) => h('p', {}, h('button', { class: 'btn pri', id: 'sub', onclick: fn }, t('p_submit')));
    if (sl.type === 'mc' || sl.type === 'quiz') {
      const sel = new Set(prev && prev.s != null ? [].concat(prev.s) : []); const btns = [];
      const paint = () => btns.forEach((b, i) => b.setAttribute('aria-pressed', sel.has(i) ? 'true' : 'false'));
      sl.opts.forEach((o, i) => btns.push(h('button', { class: 'choice' + (sl.multi && sl.type === 'mc' ? ' m' : ''), 'aria-pressed': 'false', onclick: () => { if (sl.multi && sl.type === 'mc') { sel.has(i) ? sel.delete(i) : sel.add(i); } else { sel.clear(); sel.add(i); } paint(); } }, h('span', { class: 'box' }), h('span', { dir: 'auto' }, o))));
      paint(); return view.replaceChildren(head, ...btns, submit(() => { if (!sel.size) return; send(sl, sl.type === 'quiz' ? { s: [...sel][0] } : { s: [...sel] }); }));
    }
    if (sl.type === 'cloud') {
      const ins = []; for (let i = 0; i < (sl.words || 3); i++) ins.push(h('input', { type: 'text', maxlength: '30', dir: 'auto', style: 'margin-bottom:8px', 'aria-label': (i + 1) + '', value: prev && prev.w && prev.w[i] || '' }));
      return view.replaceChildren(head, h('p', { class: 'mut' }, t('p_cloud_hint', { n: sl.words || 3 })), ...ins, submit(() => { const w = ins.map(i => i.value.trim()).filter(Boolean); if (w.length) send(sl, { w }); }));
    }
    if (sl.type === 'open') {
      const ta = h('textarea', { dir: 'auto', maxlength: '300', placeholder: t('p_open_ph'), 'aria-label': t('p_open_ph') });
      return view.replaceChildren(head, ta, submit(() => { const x = ta.value.trim(); if (x) { delete local[sl.id]; send(sl, { x }); } }));
    }
    if (sl.type === 'scale') {
      const vals = sl.opts.map((_, i) => prev && prev.v ? prev.v[i] : Math.round((sl.min + sl.max) / 2));
      return view.replaceChildren(head, ...sl.opts.map((o, i) => { const out = h('b', { dir: 'ltr' }, vals[i]); return h('div', { class: 'scalerow' }, h('div', { class: 'row' }, h('span', { dir: 'auto', style: 'flex:1' }, o), out),
        h('input', { type: 'range', min: sl.min, max: sl.max, step: 1, value: vals[i], 'aria-label': o, oninput: e => { vals[i] = Number(e.target.value); out.textContent = vals[i]; } }),
        h('div', { class: 'ends' }, h('span', { dir: 'auto' }, sl.min + (sl.minL ? ' · ' + sl.minL : '')), h('span', { dir: 'auto' }, (sl.maxL ? sl.maxL + ' · ' : '') + sl.max))); }), submit(() => send(sl, { v: vals })));
    }
    if (sl.type === 'rank') {
      let order = prev && prev.r ? prev.r.slice() : sl.opts.map((_, i) => i); const lst = h('div');
      const paint = () => lst.replaceChildren(...order.map((idx, pos) => h('div', { class: 'rankrow' }, h('span', { class: 'no' }, pos + 1), h('span', { class: 'tx', dir: 'auto' }, sl.opts[idx]),
        h('button', { class: 'btn sm ico', disabled: pos === 0, 'aria-label': t('move_up'), onclick: () => { [order[pos - 1], order[pos]] = [order[pos], order[pos - 1]]; paint(); } }, '▲'),
        h('button', { class: 'btn sm ico', disabled: pos === order.length - 1, 'aria-label': t('move_down'), onclick: () => { [order[pos + 1], order[pos]] = [order[pos], order[pos + 1]]; paint(); } }, '▼'))));
      paint(); return view.replaceChildren(head, h('p', { class: 'mut' }, t('p_rank_hint')), lst, submit(() => send(sl, { r: order })));
    }
    if (sl.type === 'points') {
      const vals = sl.opts.map((_, i) => prev && prev.p ? prev.p[i] : 0); const left = h('p', { class: 'ok' });
      const calc = () => { const s = vals.reduce((a, b) => a + b, 0); left.textContent = t('p_points_left', { n: 100 - s }); left.className = s > 100 ? 'bad' : 'ok'; const b = $('#sub'); if (b) b.disabled = s > 100 || s === 0; };
      const rows = sl.opts.map((o, i) => h('div', { class: 'row', style: 'margin-bottom:8px' }, h('label', { style: 'flex:1', dir: 'auto' }, o), h('input', { type: 'number', min: '0', max: '100', inputmode: 'numeric', style: 'width:96px', dir: 'ltr', value: vals[i], 'aria-label': o, oninput: e => { vals[i] = Math.max(0, parseInt(e.target.value) || 0); calc(); } })));
      view.replaceChildren(head, left, ...rows, submit(() => send(sl, { p: vals }))); calc(); return;
    }
  }
  draw();
  setTimeout(() => { if (!seenAny && !net.up()) { view.replaceChildren(h('div', { class: 'card center' }, h('p', {}, t('p_conn_fail')), h('p', {}, h('a', { class: 'btn', href: '#/' }, t('home'))))); } }, 15000);
}

/* ---------- boot ---------- */
window.addEventListener('hashchange', route);
S = null;
renderChrome(); route();
})();
