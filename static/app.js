/* Lifestudio ERP アナライザ - フロントエンド */

let sseSource = null;
const charts = {};

// ─── 認証 ─────────────────────────────────────────────
async function doLogout() {
  await fetch("/api/auth/logout", { method: "POST" });
  window.location.href = "/login";
}

async function changePassword() {
  const current = document.getElementById("pw-current").value;
  const newPw = document.getElementById("pw-new").value;
  const confirm = document.getElementById("pw-confirm").value;
  const msg = document.getElementById("pw-msg");
  if (newPw !== confirm) { msg.textContent = "❌ 新しいパスワードが一致しません"; msg.className = "small text-danger"; return; }
  if (newPw.length < 4) { msg.textContent = "❌ 4文字以上で設定してください"; msg.className = "small text-danger"; return; }
  try {
    const res = await fetch("/api/auth/change-password", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ current, new_password: newPw }),
    });
    const data = await res.json();
    msg.textContent = data.ok ? "✅ " + data.message : "❌ " + data.message;
    msg.className = `small ${data.ok ? "text-success" : "text-danger"}`;
    if (data.ok) {
      setTimeout(() => {
        document.getElementById("pw-current").value = "";
        document.getElementById("pw-new").value = "";
        document.getElementById("pw-confirm").value = "";
        msg.textContent = "";
        bootstrap.Modal.getInstance(document.getElementById("modal-pw")).hide();
      }, 1500);
    }
  } catch { msg.textContent = "❌ 通信エラー"; msg.className = "small text-danger"; }
}

// ─── 初期化 ─────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  refreshStatus();
  setInterval(refreshStatus, 5000);
  document.getElementById("tab-dashboard-link").addEventListener("shown.bs.tab", loadDashboard);
  document.getElementById("tab-instagram-link").addEventListener("shown.bs.tab", () => {
    refreshIgStatus();
    loadIgDashboard();
  });
  setInterval(refreshIgStatus, 10000);
});

let igSseSource = null;

// ─── 接続テスト ─────────────────────────────────────────
async function testCookie() {
  const cookie = document.getElementById("cookie-input").value.trim();
  const el = document.getElementById("test-result");
  el.textContent = "テスト中...";
  el.className = "fw-semibold small text-secondary";
  try {
    const res = await fetch("/api/test-cookie", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cookie }),
    });
    const data = await res.json();
    el.textContent = data.message;
    el.className = `fw-semibold small ${data.ok ? "text-success" : "text-danger"}`;
  } catch {
    el.textContent = "❌ 通信エラー";
    el.className = "fw-semibold small text-danger";
  }
}

// ─── 収集開始 ─────────────────────────────────────────────
async function startCollection() {
  const cookie = document.getElementById("cookie-input").value.trim();
  if (!cookie) { alert("Cookieを入力してください"); return; }
  const res = await fetch("/api/collect/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ cookie, b_seq: 49 }),
  });
  const data = await res.json();
  if (!data.ok) { alert(data.message); return; }
  setCollecting(true);
  clearLog();
  addLog("収集を開始しました...", "info");
  connectSSE();
}

async function stopCollection() {
  await fetch("/api/collect/stop", { method: "POST" });
  addLog("中断リクエストを送信しました...", "warning");
}

function exportExcel() {
  window.location.href = "/api/export";
}

// ─── SSE ─────────────────────────────────────────────────
function connectSSE() {
  if (sseSource) sseSource.close();
  sseSource = new EventSource("/api/collect/stream");
  sseSource.onmessage = (e) => handleProgressEvent(JSON.parse(e.data));
  sseSource.onerror = () => {
    sseSource.close(); sseSource = null; setCollecting(false);
  };
}

function handleProgressEvent(msg) {
  if (msg.type === "ping") return;
  const colorMap = { info: "info", phase: "primary", progress: "secondary", error: "danger", done: "success", stopped: "warning" };
  addLog(msg.message, colorMap[msg.type] || "secondary");
  if (msg.type === "progress") updateProgress(msg.current || 0, msg.total || 0, msg.percent || 0, msg.message);
  if (["done", "error", "stopped"].includes(msg.type)) {
    setCollecting(false);
    if (sseSource) { sseSource.close(); sseSource = null; }
    refreshStatus();
    if (msg.type === "done") loadDashboard();
  }
}

// ─── UI ヘルパー ─────────────────────────────────────────
function setCollecting(on) {
  document.getElementById("btn-start").disabled = on;
  document.getElementById("btn-stop").disabled = !on;
  if (on) document.getElementById("progress-section").style.display = "block";
  if (!on) document.getElementById("progress-bar").classList.remove("progress-bar-animated");
}

function updateProgress(current, total, percent, message) {
  const p = percent || (total > 0 ? Math.round(current / total * 100) : 0);
  document.getElementById("progress-bar").style.width = p + "%";
  document.getElementById("progress-pct").textContent = p + "%";
  document.getElementById("progress-label").textContent = message || `${current}/${total}件`;
  document.getElementById("progress-section").style.display = "block";
}

function clearLog() { document.getElementById("log-area").innerHTML = ""; }

function addLog(message, type = "secondary") {
  const colors = { info: "#00bfff", primary: "#7ec8ff", secondary: "#999", danger: "#ff6b6b", success: "#00e5a0", warning: "#ffd700" };
  const now = new Date().toLocaleTimeString("ja-JP");
  const p = document.createElement("p");
  p.innerHTML = `<span style="color:#555">[${now}]</span> <span style="color:${colors[type] || "#999"}">${message}</span>`;
  const la = document.getElementById("log-area");
  la.appendChild(p);
  la.scrollTop = la.scrollHeight;
}

function setText(id, val) { const el = document.getElementById(id); if (el) el.textContent = val ?? "-"; }

// ─── ステータス更新 ──────────────────────────────────────
async function refreshStatus() {
  try {
    const res = await fetch("/api/collect/status");
    const data = await res.json();
    const s = data.stats || {};
    setText("stat-reservations", s.total_reservations);
    setText("stat-customers", s.total_customers);
    setText("stat-fetched", s.fetched_customers);
    setText("stat-children", s.children_with_birthday);
    setText("stat-address", s.customers_with_address);
    document.getElementById("nav-status").textContent =
      `豊川店 | 予約${s.total_reservations || 0}件 / 収集${s.fetched_customers || 0}件`;
    if (data.running) {
      setCollecting(true);
      if (!sseSource) connectSSE();
      if (data.current > 0) updateProgress(data.current, data.total, data.percent, data.last_message);
    }
  } catch (_) {}
}

// ─── ダッシュボード読み込み ──────────────────────────────
async function loadDashboard() {
  try {
    const res = await fetch("/api/stats");
    const { analytics: a, stats: s } = await res.json();

    // サマリー
    setText("d-stat-reservations", s.total_reservations);
    setText("d-stat-customers", s.total_customers);
    setText("d-stat-children", s.children_with_birthday);
    setText("d-stat-address", s.customers_with_address);
    const pct = s.total_customers ? Math.round(s.fetched_customers / s.total_customers * 100) : 0;
    setText("d-stat-fetched-pct", pct + "%");

    // 月別
    renderMonthlyTotalChart(a.monthly_total || []);
    renderMonthlyChart(a.monthly_by_type || []);

    // 年齢・子供
    renderAgeChart(a.age_distribution || {});
    renderSiblingChart(a.sibling_order || {});
    renderCrossTable("cross-table", "no-cross", a.age_type_cross || {}, a.shoot_type_list || [], ["0-1歳","2-3歳","4-5歳","6-7歳","8歳以上"], "年齢層");
    renderSiblingVisitTable(a.sibling_visit_summary || {});

    // 母親
    renderMotherAgeChart(a.mother_age_distribution || {});

    // 来店回数
    renderVisitChart(a.visit_distribution || {});

    // 地域（県＋市）
    renderAreaChart(a.area_ranking || []);
    renderAreaTypeTable(a.area_type || {}, a.area_ranking || []);

    // 連鎖
    renderChainTable("first-chain-table", "no-first-chain", a.first_visit_chain || {}, "初回↓ 次回→");
    renderChainTable("zero-chain-table", "no-zero-chain", a.zero_age_chain || {}, "0歳時↓ 次回→");
    renderFirstVisitMonthlyChart(a.first_visit_monthly || []);

  } catch (e) {
    console.error("ダッシュボード読み込みエラー:", e);
  }
}

// ════════════════════════════════════════════════════
// ─── Instagram Graph API 収集・表示 ─────────────────
// ════════════════════════════════════════════════════

// トークン設定フォームを表示（再設定モード）
function igShowTokenForm() {
  document.getElementById("ig-token-form").style.display = "block";
  document.getElementById("ig-reconfig-area").style.display = "none";
}

// トークンを検証して保存
async function igSetupToken() {
  const token = document.getElementById("ig-token-input").value.trim();
  const el = document.getElementById("ig-setup-result");
  if (!token) { el.textContent = "トークンを入力してください"; el.className = "fw-semibold small text-danger"; return; }
  el.textContent = "検証中..."; el.className = "fw-semibold small text-secondary";
  try {
    const res = await fetch("/api/instagram/graph/setup", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token }),
    });
    const data = await res.json();
    if (data.ok) {
      el.textContent = `✅ @${data.username} に接続しました`;
      el.className = "fw-semibold small text-success";
      document.getElementById("ig-token-input").value = "";
      setTimeout(() => refreshIgStatus(), 200);
    } else {
      el.textContent = "❌ " + data.message;
      el.className = "fw-semibold small text-danger";
    }
  } catch { el.textContent = "❌ 通信エラー"; el.className = "fw-semibold small text-danger"; }
}

// 接続状態を更新してUI表示
async function refreshIgStatus() {
  try {
    const res = await fetch("/api/instagram/graph/status");
    const data = await res.json();

    if (data.connected) {
      document.getElementById("ig-connected-info").style.display = "block";
      document.getElementById("ig-token-form").style.display = "none";
      document.getElementById("ig-reconfig-area").style.display = "block";
      setText("ig-acc-name", "@" + (data.username || ""));
      setText("ig-token-masked", data.token_masked || "");
      setText("ig-acc-followers", "-");
      setText("ig-acc-media", "-");
    } else {
      document.getElementById("ig-connected-info").style.display = "none";
      document.getElementById("ig-token-form").style.display = "block";
      document.getElementById("ig-reconfig-area").style.display = "none";
    }

    // 収集済み統計
    const statsRes = await fetch("/api/instagram/graph/stats");
    const statsData = await statsRes.json();
    const stats = statsData.stats || {};
    setText("ig-stat-total", stats.total ?? "-");
    const monthly = stats.monthly || [];
    const totalLikes = monthly.reduce((s, r) => s + (r.avg_likes || 0) * (r.count || 0), 0);
    const totalPosts = monthly.reduce((s, r) => s + (r.count || 0), 0);
    setText("ig-stat-avg-likes", totalPosts > 0 ? Math.round(totalLikes / totalPosts) : "-");
    const totalComments = monthly.reduce((s, r) => s + (r.avg_comments || 0) * (r.count || 0), 0);
    setText("ig-stat-avg-comments", totalPosts > 0 ? Math.round(totalComments / totalPosts) : "-");

    // 収集中なら SSE を再接続
    if (statsData.running && !igSseSource) {
      document.getElementById("ig-btn-start").disabled = true;
      document.getElementById("ig-btn-stop").disabled = false;
      igConnectGraphSSE();
    }

  } catch(_) {}
}

// 収集開始
async function igGraphStartCollection() {
  const res = await fetch("/api/instagram/graph/collect/start", { method: "POST" });
  const data = await res.json();
  if (!data.ok) { alert(data.message); return; }
  document.getElementById("ig-btn-start").disabled = true;
  document.getElementById("ig-btn-stop").disabled = false;
  document.getElementById("ig-progress-section").style.display = "block";
  igAddLog(data.message, "info");
  igConnectGraphSSE();
}

// 収集停止
async function igGraphStopCollection() {
  await fetch("/api/instagram/graph/collect/stop", { method: "POST" });
  igAddLog("中断リクエストを送信しました...", "warning");
}

// SSE 接続
function igConnectGraphSSE() {
  if (igSseSource) igSseSource.close();
  igSseSource = new EventSource("/api/instagram/graph/collect/stream");
  igSseSource.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === "ping") return;
    const colorMap = { info:"info", phase:"primary", progress:"secondary", error:"danger", done:"success", stopped:"warning" };
    igAddLog(msg.message, colorMap[msg.type] || "secondary");
    if (msg.type === "progress") {
      document.getElementById("ig-progress-label").textContent = msg.message;
      document.getElementById("ig-progress-pct").textContent = (msg.percent || 0) + "%";
      document.getElementById("ig-progress-bar").style.width = (msg.percent || 0) + "%";
    }
    if (["done", "error", "stopped"].includes(msg.type)) {
      document.getElementById("ig-btn-start").disabled = false;
      document.getElementById("ig-btn-stop").disabled = true;
      igSseSource.close(); igSseSource = null;
      refreshIgStatus();
      loadIgDashboard();
    }
  };
  igSseSource.onerror = () => { igSseSource && igSseSource.close(); igSseSource = null; };
}

function igAddLog(message, type = "secondary") {
  const colors = { info:"#00bfff", primary:"#7ec8ff", secondary:"#aaa", danger:"#ff6b6b", success:"#00e5a0", warning:"#ffd700" };
  const now = new Date().toLocaleTimeString("ja-JP");
  const p = document.createElement("p");
  p.innerHTML = `<span style="color:#555">[${now}]</span> <span style="color:${colors[type]||"#aaa"}">${message}</span>`;
  p.style.margin = "1px 0";
  const la = document.getElementById("ig-log-area");
  la.appendChild(p); la.scrollTop = la.scrollHeight;
}

// ダッシュボード全体読み込み
async function loadIgDashboard() {
  try {
    const res = await fetch("/api/instagram/graph/stats");
    const { stats, hashtags } = await res.json();

    renderIgMonthlyChart(stats.monthly || []);
    renderIgHourlyChart(stats.hourly || []);
    renderIgWeekdayChart(stats.weekday || []);
    renderIgTypeChart(stats.media_types || []);
    renderIgLikesChart(stats.monthly || []);
    renderIgTopTable(stats.top_posts || []);
    renderIgHashtagTable(hashtags || []);
  } catch(e) { console.error("IG dashboard error:", e); }
}

// ─── Instagram グラフ描画 ────────────────────────────

function renderIgMonthlyChart(monthly) {
  const hasData = monthly.length > 0;
  document.getElementById("ig-no-monthly").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  destroyChart("ig-monthly");
  charts["ig-monthly"] = new Chart(document.getElementById("ig-chart-monthly"), {
    type: "bar",
    data: {
      labels: monthly.map(r => r.month),
      datasets: [{
        label: "投稿件数",
        data: monthly.map(r => r.count),
        backgroundColor: "rgba(220,53,69,0.7)", borderColor: "#dc3545", borderWidth: 1,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true, title: { display: true, text: "件数" } }, x: { ticks: { maxTicksLimit: 20 } } },
    },
  });
}

function renderIgHourlyChart(hourly) {
  const hasData = hourly.length > 0;
  document.getElementById("ig-no-hourly").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  // 0〜23時の全時間帯を埋める
  const allHours = Array.from({length: 24}, (_, i) => i);
  const countMap = {};
  hourly.forEach(r => { countMap[r.hour] = r.count; });
  destroyChart("ig-hourly");
  charts["ig-hourly"] = new Chart(document.getElementById("ig-chart-hourly"), {
    type: "bar",
    data: {
      labels: allHours.map(h => h + "時"),
      datasets: [{
        label: "投稿数",
        data: allHours.map(h => countMap[h] || 0),
        backgroundColor: allHours.map(h => h >= 7 && h <= 22 ? "rgba(220,53,69,0.6)" : "rgba(108,117,125,0.4)"),
        borderWidth: 0,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true }, x: { ticks: { maxTicksLimit: 24, font: { size: 9 } } } },
    },
  });
}

function renderIgWeekdayChart(weekday) {
  const hasData = weekday.length > 0;
  document.getElementById("ig-no-weekday").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  const order = ["日","月","火","水","木","金","土"];
  const sorted = order.map(d => { const r = weekday.find(w => w.day === d); return r ? r.count : 0; });
  destroyChart("ig-weekday");
  charts["ig-weekday"] = new Chart(document.getElementById("ig-chart-weekday"), {
    type: "bar",
    data: {
      labels: order,
      datasets: [{ label: "投稿数", data: sorted, backgroundColor: COLORS.map(c => c + "bb"), borderWidth: 1 }],
    },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } },
  });
}

function renderIgTypeChart(mediaTypes) {
  const hasData = mediaTypes.length > 0;
  document.getElementById("ig-no-type").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  const typeNames = { 1: "写真", 2: "動画/リール", 8: "カルーセル" };
  destroyChart("ig-type");
  charts["ig-type"] = new Chart(document.getElementById("ig-chart-type"), {
    type: "doughnut",
    data: {
      labels: mediaTypes.map(r => typeNames[r.type] || `type${r.type}`),
      datasets: [{ data: mediaTypes.map(r => r.count), backgroundColor: COLORS.slice(0, mediaTypes.length) }],
    },
    options: { responsive: true, plugins: { legend: { position: "bottom" } } },
  });
}

function renderIgLikesChart(monthly) {
  const hasData = monthly.length > 0;
  document.getElementById("ig-no-likes").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  destroyChart("ig-likes");
  charts["ig-likes"] = new Chart(document.getElementById("ig-chart-likes"), {
    type: "line",
    data: {
      labels: monthly.map(r => r.month),
      datasets: [
        { label: "平均いいね", data: monthly.map(r => r.avg_likes), borderColor: "#dc3545", backgroundColor: "#dc354522", tension: 0.3, fill: true },
        { label: "平均コメント", data: monthly.map(r => r.avg_comments), borderColor: "#0d6efd", backgroundColor: "#0d6efd22", tension: 0.3, fill: true },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "bottom" } },
      scales: { y: { beginAtZero: true }, x: { ticks: { maxTicksLimit: 18 } } },
    },
  });
}

function renderIgTopTable(posts) {
  const table = document.getElementById("ig-top-table");
  const hasData = posts.length > 0;
  document.getElementById("ig-no-top").style.display = hasData ? "none" : "block";
  if (!hasData) { table.querySelector("thead").innerHTML = ""; table.querySelector("tbody").innerHTML = ""; return; }
  const typeNames = { 1: "📷写真", 2: "🎬動画", 8: "🖼カルーセル" };
  table.querySelector("thead").innerHTML =
    "<tr><th>#</th><th>投稿日</th><th>種別</th><th>いいね</th><th>コメント</th><th>キャプション（冒頭）</th><th>リンク</th></tr>";
  let html = "";
  posts.forEach((p, i) => {
    const date = p.taken_at ? new Date(p.taken_at * 1000).toLocaleDateString("ja-JP") : "-";
    const url = p.shortcode ? `https://www.instagram.com/p/${p.shortcode}/` : "#";
    const cap = (p.caption || "").slice(0, 60).replace(/</g, "&lt;");
    html += `<tr>
      <td>${i+1}</td>
      <td>${date}</td>
      <td>${typeNames[p.media_type] || p.media_type}</td>
      <td>❤️ ${p.likes}</td>
      <td>💬 ${p.comments}</td>
      <td style="max-width:280px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${cap}</td>
      <td><a href="${url}" target="_blank" class="btn btn-outline-secondary btn-sm py-0">開く</a></td>
    </tr>`;
  });
  table.querySelector("tbody").innerHTML = html;
}

function renderIgHashtagTable(hashtags) {
  const table = document.getElementById("ig-hashtag-table");
  const hasData = hashtags.length > 0;
  document.getElementById("ig-no-hashtag").style.display = hasData ? "none" : "block";
  if (!hasData) { table.querySelector("tbody").innerHTML = ""; return; }
  let html = "";
  hashtags.forEach((h, i) => {
    html += `<tr><td class="text-muted">${i+1}</td><td><span class="badge bg-light text-dark border">#${h.tag}</span></td><td>${h.count}</td></tr>`;
  });
  table.querySelector("tbody").innerHTML = html;
}

// ─── グラフ描画 ──────────────────────────────────────────

function destroyChart(name) {
  if (charts[name]) { charts[name].destroy(); delete charts[name]; }
}

const COLORS = ["#0d6efd","#198754","#dc3545","#fd7e14","#6610f2","#0dcaf0","#6c757d","#ffc107","#20c997","#d63384"];

// 月別合計来店件数（棒グラフ）
function renderMonthlyTotalChart(monthlyTotal) {
  if (!monthlyTotal.length) return;
  destroyChart("monthly-total");
  charts["monthly-total"] = new Chart(document.getElementById("chart-monthly-total"), {
    type: "bar",
    data: {
      labels: monthlyTotal.map(r => r.month),
      datasets: [{
        label: "来店件数",
        data: monthlyTotal.map(r => r.count),
        backgroundColor: "rgba(13,202,240,0.7)",
        borderColor: "#0dcaf0",
        borderWidth: 1,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, title: { display: true, text: "件数" } },
        x: { ticks: { maxRotation: 45, font: { size: 10 } } },
      },
    },
  });
}

// 撮影種類×月別推移（折れ線）
function renderMonthlyChart(monthlyData) {
  if (!monthlyData.length) return;
  const months = [...new Set(monthlyData.map(r => r.month))].sort();
  const typeTotals = {};
  monthlyData.forEach(r => { typeTotals[r.shoot_type] = (typeTotals[r.shoot_type] || 0) + r.count; });
  const topTypes = Object.entries(typeTotals).sort((a, b) => b[1] - a[1]).slice(0, 6).map(e => e[0]);
  destroyChart("monthly");
  charts["monthly"] = new Chart(document.getElementById("chart-monthly"), {
    type: "line",
    data: {
      labels: months,
      datasets: topTypes.map((type, i) => ({
        label: type,
        data: months.map(m => { const r = monthlyData.find(r => r.month === m && r.shoot_type === type); return r ? r.count : 0; }),
        borderColor: COLORS[i % COLORS.length],
        backgroundColor: COLORS[i % COLORS.length] + "22",
        fill: false, tension: 0.3, pointRadius: 3,
      })),
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "bottom" } },
      scales: {
        y: { beginAtZero: true, title: { display: true, text: "件数" } },
        x: { ticks: { maxTicksLimit: 18 } },
      },
    },
  });
}

// 子供の撮影時年齢分布
function renderAgeChart(ageDist) {
  const keys = Object.keys(ageDist).map(Number).sort((a, b) => a - b);
  const hasData = keys.length > 0;
  document.getElementById("no-age").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  destroyChart("age");
  charts["age"] = new Chart(document.getElementById("chart-age"), {
    type: "bar",
    data: {
      labels: keys.map(k => k + "歳"),
      datasets: [{ label: "人数", data: keys.map(k => ageDist[String(k)]), backgroundColor: "rgba(13,110,253,0.7)", borderColor: "#0d6efd", borderWidth: 1 }],
    },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true }, x: { title: { display: true, text: "撮影時の年齢" } } } },
  });
}

// 来店回数分布
function renderVisitChart(visitDist) {
  const order = ["1","2","3","4","5","6以上"];
  const labels = order.filter(k => k in visitDist);
  destroyChart("visit");
  charts["visit"] = new Chart(document.getElementById("chart-visit"), {
    type: "bar",
    data: {
      labels: labels.map(l => l + "回"),
      datasets: [{ label: "顧客数", data: labels.map(k => visitDist[k] || 0), backgroundColor: "rgba(255,193,7,0.7)", borderColor: "#ffc107", borderWidth: 1 }],
    },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } },
  });
}

// 母親年齢分布
function renderMotherAgeChart(motherAgeDist) {
  const keys = Object.keys(motherAgeDist).sort();
  const hasData = keys.length > 0;
  document.getElementById("no-mother-age").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  destroyChart("mother-age");
  charts["mother-age"] = new Chart(document.getElementById("chart-mother-age"), {
    type: "bar",
    data: {
      labels: keys,
      datasets: [{ label: "人数", data: keys.map(k => motherAgeDist[k]), backgroundColor: "rgba(220,53,69,0.7)", borderColor: "#dc3545", borderWidth: 1 }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true, title: { display: true, text: "人数" } } },
    },
  });
}

// 兄弟順×撮影傾向
function renderSiblingChart(siblingOrder) {
  const labels = ["第1子","第2子","第3子","第4子以上"].filter(l => l in siblingOrder);
  const hasData = labels.length > 0;
  document.getElementById("no-sibling").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  const typeTotals = {};
  labels.forEach(l => Object.entries(siblingOrder[l] || {}).forEach(([t, v]) => { typeTotals[t] = (typeTotals[t] || 0) + v; }));
  const topTypes = Object.entries(typeTotals).sort((a, b) => b[1] - a[1]).slice(0, 5).map(e => e[0]);
  destroyChart("sibling");
  charts["sibling"] = new Chart(document.getElementById("chart-sibling"), {
    type: "bar",
    data: {
      labels,
      datasets: topTypes.map((type, i) => ({
        label: type,
        data: labels.map(l => (siblingOrder[l] || {})[type] || 0),
        backgroundColor: COLORS[i % COLORS.length] + "bb",
        borderColor: COLORS[i % COLORS.length], borderWidth: 1,
      })),
    },
    options: {
      responsive: true,
      plugins: { legend: { position: "bottom" } },
      scales: { x: { stacked: false }, y: { beginAtZero: true, stacked: false } },
    },
  });
}

// 地域ランキング（県＋市）
function renderAreaChart(areaRanking) {
  const hasData = areaRanking.length > 0;
  document.getElementById("no-area").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  const top = areaRanking.slice(0, 15);
  destroyChart("area");
  charts["area"] = new Chart(document.getElementById("chart-area"), {
    type: "bar",
    data: {
      labels: top.map(r => r.city),
      datasets: [
        {
          label: "顧客数",
          data: top.map(r => r.customers),
          backgroundColor: "rgba(25,135,84,0.7)",
          borderColor: "#198754", borderWidth: 1,
        },
        {
          label: "来店件数",
          data: top.map(r => r.visits),
          backgroundColor: "rgba(25,135,84,0.3)",
          borderColor: "#198754", borderWidth: 1,
        },
      ],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      plugins: { legend: { position: "bottom" } },
      scales: { x: { beginAtZero: true } },
    },
  });
}

// 初来店×月別グラフ
function renderFirstVisitMonthlyChart(data) {
  const hasData = data.length > 0;
  document.getElementById("no-first-monthly").style.display = hasData ? "none" : "block";
  if (!hasData) return;
  const months = [...new Set(data.map(r => r.month))].sort();
  const typeTotals = {};
  data.forEach(r => { typeTotals[r.shoot_type] = (typeTotals[r.shoot_type] || 0) + r.count; });
  const topTypes = Object.entries(typeTotals).sort((a, b) => b[1] - a[1]).slice(0, 6).map(e => e[0]);
  destroyChart("first-visit-monthly");
  charts["first-visit-monthly"] = new Chart(document.getElementById("chart-first-visit-monthly"), {
    type: "bar",
    data: {
      labels: months,
      datasets: topTypes.map((type, i) => ({
        label: type,
        data: months.map(m => { const r = data.find(r => r.month === m && r.shoot_type === type); return r ? r.count : 0; }),
        backgroundColor: COLORS[i % COLORS.length] + "bb",
        borderColor: COLORS[i % COLORS.length], borderWidth: 1,
      })),
    },
    options: {
      responsive: true,
      plugins: { legend: { position: "bottom" } },
      scales: {
        x: { stacked: true, ticks: { maxTicksLimit: 20 } },
        y: { stacked: true, beginAtZero: true, title: { display: true, text: "初来店件数" } },
      },
    },
  });
}

// ─── テーブル描画 ────────────────────────────────────────

function renderCrossTable(tableId, noDataId, crossData, shootTypes, buckets, rowHeader) {
  const hasData = Object.keys(crossData).length > 0;
  document.getElementById(noDataId).style.display = hasData ? "none" : "block";
  const table = document.getElementById(tableId);
  if (!hasData) { table.querySelector("thead").innerHTML = ""; table.querySelector("tbody").innerHTML = ""; return; }
  const topTypes = shootTypes.slice(0, 8);
  table.querySelector("thead").innerHTML =
    `<tr><th>${rowHeader}</th>${topTypes.map(t => `<th>${t}</th>`).join("")}<th>合計</th></tr>`;
  let html = "";
  const colTotals = {};
  let grand = 0;
  for (const bucket of buckets) {
    const row = crossData[bucket] || {};
    const rowTotal = topTypes.reduce((s, t) => s + (row[t] || 0), 0);
    grand += rowTotal;
    topTypes.forEach(t => { colTotals[t] = (colTotals[t] || 0) + (row[t] || 0); });
    html += `<tr><td><strong>${bucket}</strong></td>${topTypes.map(t => `<td>${row[t] || 0}</td>`).join("")}<td><strong>${rowTotal}</strong></td></tr>`;
  }
  html += `<tr class="table-secondary"><td><strong>合計</strong></td>${topTypes.map(t => `<td><strong>${colTotals[t] || 0}</strong></td>`).join("")}<td><strong>${grand}</strong></td></tr>`;
  table.querySelector("tbody").innerHTML = html;
}

function renderSiblingVisitTable(summary) {
  const table = document.getElementById("sibling-visit-table");
  const hasData = Object.keys(summary).length > 0;
  document.getElementById("no-sibling-visit").style.display = hasData ? "none" : "block";
  if (!hasData) { table.querySelector("thead").innerHTML = ""; table.querySelector("tbody").innerHTML = ""; return; }
  table.querySelector("thead").innerHTML =
    "<tr><th>子供の人数</th><th>顧客数</th><th>平均来店回数</th><th>来店回数合計</th></tr>";
  const order = ["1","2","3","4以上"];
  let html = "";
  for (const k of order) {
    if (!summary[k]) continue;
    const s = summary[k];
    html += `<tr><td><strong>${k}人</strong></td><td>${s.count}</td><td>${s.avg}</td><td>${s.total_visits}</td></tr>`;
  }
  table.querySelector("tbody").innerHTML = html;
}

function renderAreaTypeTable(areaType, areaRanking) {
  const table = document.getElementById("area-type-table");
  const hasData = Object.keys(areaType).length > 0;
  document.getElementById("no-area-type").style.display = hasData ? "none" : "block";
  if (!hasData) { table.querySelector("thead").innerHTML = ""; table.querySelector("tbody").innerHTML = ""; return; }

  // areaTypeのキーは「愛知県豊川市」のように prefecture+city
  const topCityKeys = areaRanking.slice(0, 10).map(r => (r.prefecture || "") + r.city);
  const allTypes = [...new Set(Object.values(areaType).flatMap(d => Object.keys(d)))].slice(0, 6);

  table.querySelector("thead").innerHTML =
    `<tr><th>県・市</th>${allTypes.map(t => `<th>${t}</th>`).join("")}</tr>`;
  let html = "";
  for (const key of topCityKeys) {
    const row = areaType[key] || {};
    html += `<tr><td><strong>${key}</strong></td>${allTypes.map(t => `<td>${row[t] || 0}</td>`).join("")}</tr>`;
  }
  table.querySelector("tbody").innerHTML = html;
}

// 汎用：連鎖テーブル（初来店連鎖・0歳連鎖共用）
function renderChainTable(tableId, noDataId, chain, headerLabel) {
  const table = document.getElementById(tableId);
  const hasData = Object.keys(chain).length > 0;
  document.getElementById(noDataId).style.display = hasData ? "none" : "block";
  if (!hasData) { table.querySelector("thead").innerHTML = ""; table.querySelector("tbody").innerHTML = ""; return; }

  // 遷移先のうち上位を列に
  const toTotals = {};
  Object.values(chain).forEach(toMap => {
    Object.entries(toMap).forEach(([to, cnt]) => { toTotals[to] = (toTotals[to] || 0) + cnt; });
  });
  const topTo = Object.entries(toTotals).sort((a, b) => b[1] - a[1]).slice(0, 6).map(e => e[0]);

  // 遷移元を件数順に並べる
  const fromTotals = {};
  Object.entries(chain).forEach(([from, toMap]) => {
    fromTotals[from] = Object.values(toMap).reduce((s, v) => s + v, 0);
  });
  const topFrom = Object.entries(fromTotals).sort((a, b) => b[1] - a[1]).slice(0, 8).map(e => e[0]);

  table.querySelector("thead").innerHTML =
    `<tr><th>${headerLabel}</th>${topTo.map(t => `<th>${t}</th>`).join("")}<th>合計</th></tr>`;
  let html = "";
  for (const from of topFrom) {
    const row = chain[from] || {};
    const rowTotal = topTo.reduce((s, t) => s + (row[t] || 0), 0);
    html += `<tr><td><strong>${from}</strong></td>${topTo.map(t => `<td>${row[t] || 0}</td>`).join("")}<td><strong>${rowTotal}</strong></td></tr>`;
  }
  table.querySelector("tbody").innerHTML = html;
}
