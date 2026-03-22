"""
Lifestudio ERP データ収集・分析 Webアプリ
起動: python3 app.py
"""
import asyncio
import json
import webbrowser
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse, RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from auth import (
    verify_credentials, create_session_token, verify_session_token,
    change_password, get_username, COOKIE_NAME, SESSION_MAX_AGE
)
from crawler import CollectionJob, make_session, test_session
from db import Database
from excel_export import build_excel
from instagram_crawler import IGCollectionJob, make_ig_session, test_ig_session
from instagram_graph import IGGraphCollectionJob, validate_token, get_ig_account

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "lifeerp.db"
EXCEL_PATH = DATA_DIR / "豊川店_顧客年齢分析.xlsx"

db = Database(DB_PATH)
_job = None            # type: CollectionJob
_ig_job = None         # type: IGCollectionJob
_ig_graph_job = None   # type: IGGraphCollectionJob


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        db.init()
    except Exception as e:
        print(f"[WARN] DB初期化エラー（リセットして再試行）: {e}")
        # 既存の接続を閉じる
        db.close()
        # DBファイルが壊れている場合はリネームして新規作成
        import shutil
        recovered = False
        # まずリネーム・削除を試みる
        backup = Path(str(DB_PATH) + ".bak")
        try:
            if DB_PATH.exists():
                DB_PATH.rename(backup)
                print(f"[INFO] 壊れたDBをリネーム: {backup}")
            for ext in ["-journal", "-wal", "-shm"]:
                p = Path(str(DB_PATH) + ext)
                if p.exists():
                    try:
                        p.unlink()
                    except OSError:
                        with open(p, "wb") as f:
                            f.truncate(0)
            db.init()
            recovered = True
            print(f"[INFO] DBを新規作成しました")
        except OSError as oe:
            print(f"[WARN] DBリネーム/削除失敗: {oe}")

        if not recovered:
            # リネームも削除もできない場合は別パスに新規DB作成
            alt_path = BASE_DIR / "lifeerp_new.db"
            print(f"[INFO] 別パスでDB作成: {alt_path}")
            # 古いジャーナルをクリア
            for ext in ["-journal", "-wal", "-shm"]:
                p = Path(str(DB_PATH) + ext)
                if p.exists():
                    try:
                        with open(p, "wb") as f:
                            f.truncate(0)
                    except OSError:
                        pass
            db.db_path = alt_path
            db.close()
            db.init()
            print(f"[INFO] 新DBで起動しました: {alt_path}")

    records_path = DATA_DIR / "parsed_records.json"
    if records_path.exists():
        with open(records_path, encoding="utf-8") as f:
            records = json.load(f)
        imported = db.import_reservations(records)
        print(f"[INFO] 予約データ読み込み: {imported}件")
    else:
        print(f"[WARN] {records_path} が見つかりません")
    yield


app = FastAPI(title="Lifestudio ERP アナライザ", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# ─── 認証ヘルパー ────────────────────────────────────

def _is_authenticated(request: Request) -> bool:
    token = request.cookies.get(COOKIE_NAME, "")
    return verify_session_token(token)

# 認証が不要なパス
_PUBLIC_PATHS = {"/login", "/api/auth/login", "/favicon.ico"}


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """未認証リクエストを /login にリダイレクト"""
    path = request.url.path
    # 静的ファイルとパブリックAPIは通す
    if path.startswith("/static") or path in _PUBLIC_PATHS:
        return await call_next(request)
    if not _is_authenticated(request):
        if path.startswith("/api/"):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return RedirectResponse(url="/login")
    return await call_next(request)


# ─── ログイン画面・API ────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return HTMLResponse(content=_LOGIN_HTML)


@app.post("/api/auth/login")
async def api_login(request: Request, response: Response):
    body = await request.json()
    username = body.get("username", "").strip()
    password = body.get("password", "").strip()

    if not verify_credentials(username, password):
        return JSONResponse({"ok": False, "message": "ユーザー名またはパスワードが違います"}, status_code=401)

    token = create_session_token()
    resp = JSONResponse({"ok": True})
    resp.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="strict",
        max_age=SESSION_MAX_AGE,
        path="/",
    )
    return resp


@app.post("/api/auth/logout")
async def api_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(key=COOKIE_NAME, path="/")
    return resp


@app.post("/api/auth/change-password")
async def api_change_password(request: Request):
    body = await request.json()
    current = body.get("current", "").strip()
    new_pw = body.get("new_password", "").strip()
    if not verify_credentials(get_username(), current):
        return JSONResponse({"ok": False, "message": "現在のパスワードが違います"}, status_code=401)
    if len(new_pw) < 4:
        return JSONResponse({"ok": False, "message": "パスワードは4文字以上にしてください"}, status_code=400)
    change_password(new_pw)
    return {"ok": True, "message": "パスワードを変更しました"}


# ─── ログインHTML ─────────────────────────────────────
_LOGIN_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ログイン | Lifestudio ERP</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
  body { background: #f4f6fa; display: flex; align-items: center; justify-content: center; min-height: 100vh; }
  .login-card { width: 360px; border-radius: 16px; box-shadow: 0 4px 24px rgba(0,0,0,.12); border: none; }
  .login-header { background: #0d6efd; color: #fff; border-radius: 16px 16px 0 0; padding: 28px 24px 20px; text-align: center; }
  .login-header h4 { margin: 0; font-weight: 700; font-size: 1.1rem; letter-spacing: .02em; }
  .login-header p { margin: 4px 0 0; font-size: 0.82rem; opacity: .8; }
  .login-body { padding: 28px 24px 24px; }
  .form-control:focus { box-shadow: 0 0 0 3px rgba(13,110,253,.2); }
  #err-msg { font-size: 0.84rem; }
</style>
</head>
<body>
<div class="card login-card">
  <div class="login-header">
    <h4>📷 Lifestudio ERP アナライザ</h4>
    <p>豊川店 スタッフ専用</p>
  </div>
  <div class="login-body">
    <div class="mb-3">
      <label class="form-label small fw-semibold">ユーザー名</label>
      <input type="text" id="username" class="form-control" placeholder="admin" autocomplete="username">
    </div>
    <div class="mb-3">
      <label class="form-label small fw-semibold">パスワード</label>
      <input type="password" id="password" class="form-control" placeholder="パスワード" autocomplete="current-password">
    </div>
    <div id="err-msg" class="text-danger mb-2" style="min-height:20px;"></div>
    <button id="login-btn" class="btn btn-primary w-100" onclick="doLogin()">ログイン</button>
    <p class="text-muted text-center mt-3 mb-0" style="font-size:0.78rem;">
      初期パスワード: <code>lifestudio</code>（ログイン後に変更できます）
    </p>
  </div>
</div>
<script>
document.addEventListener("keydown", e => { if (e.key === "Enter") doLogin(); });
async function doLogin() {
  const btn = document.getElementById("login-btn");
  const err = document.getElementById("err-msg");
  const username = document.getElementById("username").value.trim();
  const password = document.getElementById("password").value;
  if (!username || !password) { err.textContent = "ユーザー名とパスワードを入力してください"; return; }
  btn.disabled = true; btn.textContent = "確認中...";
  try {
    const res = await fetch("/api/auth/login", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password })
    });
    const data = await res.json();
    if (data.ok) { window.location.href = "/"; }
    else { err.textContent = data.message; btn.disabled = false; btn.textContent = "ログイン"; }
  } catch { err.textContent = "通信エラーが発生しました"; btn.disabled = false; btn.textContent = "ログイン"; }
}
</script>
</body>
</html>"""


@app.get("/", response_class=FileResponse)
async def index():
    return FileResponse(BASE_DIR / "static" / "index.html")


# ─── Cookie テスト ───────────────────────────────────────

@app.post("/api/test-cookie")
async def api_test_cookie(request: Request):
    body = await request.json()
    cookie_str = body.get("cookie", "").strip()
    if not cookie_str:
        return {"ok": False, "message": "Cookieが入力されていません"}

    def _test():
        session = make_session(cookie_str)
        return test_session(session)

    ok = await asyncio.to_thread(_test)
    return {"ok": ok, "message": "接続OK ✅" if ok else "接続失敗 ❌ Cookieを確認してください"}


# ─── 収集開始 / 停止 / ステータス ───────────────────────

@app.post("/api/collect/start")
async def api_collect_start(request: Request):
    global _job
    body = await request.json()
    cookie_str = body.get("cookie", "").strip()
    b_seq = int(body.get("b_seq", 49))

    if not cookie_str:
        return JSONResponse({"ok": False, "message": "Cookieが未入力です"}, status_code=400)
    if _job and _job.is_running():
        return JSONResponse({"ok": False, "message": "収集中です。先に中断してください"}, status_code=400)

    _job = CollectionJob(db, cookie_str, b_seq)
    _job.start()
    return {"ok": True, "message": "収集を開始しました"}


@app.post("/api/collect/stop")
async def api_collect_stop():
    if _job:
        _job.stop()
    return {"ok": True}


@app.get("/api/collect/status")
async def api_collect_status():
    running = _job.is_running() if _job else False
    last_msg = ""
    current = 0
    total = 0
    percent = 0

    if _job and _job.progress_log:
        last = _job.progress_log[-1]
        last_msg = last.get("message", "")
        current = last.get("current", 0)
        total = last.get("total", 0)
        percent = last.get("percent", 0)

    stats = await asyncio.to_thread(db.get_collection_stats)
    return {
        "running": running,
        "last_message": last_msg,
        "current": current,
        "total": total,
        "percent": percent,
        "stats": stats,
    }


# ─── SSE 進捗ストリーム ─────────────────────────────────

@app.get("/api/collect/stream")
async def api_collect_stream():
    async def generator():
        last_idx = 0
        idle_count = 0
        while True:
            new_entries = (_job.progress_log[last_idx:] if _job else [])
            if new_entries:
                idle_count = 0
                for entry in new_entries:
                    last_idx += 1
                    yield f"data: {json.dumps(entry, ensure_ascii=False)}\n\n"
                    if entry.get("type") in ("done", "error", "stopped"):
                        return
            else:
                idle_count += 1
                if idle_count % 4 == 0:
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── 分析データ ─────────────────────────────────────────

@app.get("/api/stats")
async def api_stats(branch_id: int = 49):
    analytics = await asyncio.to_thread(db.get_analytics, branch_id)
    stats = await asyncio.to_thread(db.get_collection_stats, branch_id)
    return {"analytics": analytics, "stats": stats}


# ─── Excel エクスポート ─────────────────────────────────

@app.get("/api/export")
async def api_export(branch_id: int = 49):
    def _build():
        reservations = db.get_reservations(branch_id)
        customer_data = db.get_all_customer_data(branch_id)
        build_excel(reservations, customer_data, EXCEL_PATH)

    await asyncio.to_thread(_build)

    if not EXCEL_PATH.exists():
        return JSONResponse({"error": "Excel生成に失敗しました"}, status_code=500)

    return FileResponse(
        path=str(EXCEL_PATH),
        filename="豊川店_顧客年齢分析.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ─── Instagram API ──────────────────────────────────────

@app.post("/api/instagram/test-cookie")
async def ig_test_cookie(request: Request):
    body = await request.json()
    cookie_str = body.get("cookie", "").strip()
    if not cookie_str:
        return {"ok": False, "message": "Cookieが入力されていません"}
    def _test():
        session = make_ig_session(cookie_str)
        return test_ig_session(session)
    ok = await asyncio.to_thread(_test)
    return {"ok": ok, "message": "接続OK ✅" if ok else "接続失敗 ❌ Cookieを確認してください"}


@app.post("/api/instagram/collect/start")
async def ig_collect_start(request: Request):
    global _ig_job
    body = await request.json()
    cookie_str = body.get("cookie", "").strip()
    usernames = body.get("usernames", ["lifestudio_toyokawa", "amoretto_lifecasting_aichi"])
    if not cookie_str:
        return JSONResponse({"ok": False, "message": "Cookieが未入力です"}, status_code=400)
    if _ig_job and _ig_job.is_running():
        return JSONResponse({"ok": False, "message": "収集中です"}, status_code=400)
    _ig_job = IGCollectionJob(db, cookie_str, usernames)
    _ig_job.start()
    return {"ok": True, "message": "Instagram収集を開始しました"}


@app.post("/api/instagram/collect/stop")
async def ig_collect_stop():
    if _ig_job:
        _ig_job.stop()
    return {"ok": True}


@app.get("/api/instagram/collect/stream")
async def ig_collect_stream():
    async def generator():
        last_idx = 0
        idle_count = 0
        while True:
            new_entries = (_ig_job.progress_log[last_idx:] if _ig_job else [])
            if new_entries:
                idle_count = 0
                for entry in new_entries:
                    last_idx += 1
                    yield f"data: {__import__('json').dumps(entry, ensure_ascii=False)}\n\n"
                    if entry.get("type") in ("done", "error", "stopped"):
                        return
            else:
                idle_count += 1
                if idle_count % 4 == 0:
                    yield f"data: {{\"type\": \"ping\"}}\n\n"
            await asyncio.sleep(0.5)
    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/instagram/stats")
async def ig_stats(username: str = None):
    running = _ig_job.is_running() if _ig_job else False
    stats = await asyncio.to_thread(db.get_instagram_stats, username)
    return {"running": running, "stats": stats}


# ─── Instagram Graph API ─────────────────────────────────

@app.post("/api/instagram/graph/setup")
async def ig_graph_setup(request: Request):
    """アクセストークンを検証し、IGアカウント情報を取得してDBに保存"""
    body = await request.json()
    token = body.get("token", "").strip()
    if not token:
        return JSONResponse({"ok": False, "message": "トークンが入力されていません"}, status_code=400)

    def _setup():
        # トークン検証
        v = validate_token(token)
        if not v["ok"]:
            return {"ok": False, "message": f"トークンエラー: {v['error']}"}
        # IGビジネスアカウント取得
        ig = get_ig_account(token)
        if not ig["ok"]:
            return {"ok": False, "message": ig["error"]}
        # DBに保存
        db.save_ig_token(token, ig["ig_user_id"], ig["username"])
        return {
            "ok": True,
            "ig_user_id": ig["ig_user_id"],
            "username": ig["username"],
            "name": ig.get("name", ""),
            "followers": ig.get("followers", 0),
            "media_count": ig.get("media_count", 0),
        }

    result = await asyncio.to_thread(_setup)
    return result


@app.get("/api/instagram/graph/status")
async def ig_graph_status():
    """保存済みトークン情報とアカウント情報を返す"""
    def _get():
        info = db.get_ig_token()
        if not info or not info.get("is_set"):
            return {"connected": False}
        return {
            "connected": True,
            "token_masked": info["token_masked"],
            "ig_user_id": info["ig_user_id"],
            "username": info["username"],
        }
    return await asyncio.to_thread(_get)


@app.post("/api/instagram/graph/collect/start")
async def ig_graph_collect_start():
    """Graph APIでInstagram投稿を収集開始"""
    global _ig_graph_job

    def _load():
        return db.get_ig_token()

    info = await asyncio.to_thread(_load)
    if not info or not info.get("is_set"):
        return JSONResponse({"ok": False, "message": "トークン未設定。先にセットアップしてください"}, status_code=400)
    if _ig_graph_job and _ig_graph_job.is_running():
        return JSONResponse({"ok": False, "message": "収集中です。先に停止してください"}, status_code=400)

    token = info["token"]
    ig_user_id = info["ig_user_id"]
    username = info["username"]
    _ig_graph_job = IGGraphCollectionJob(db, token, ig_user_id, username)
    _ig_graph_job.start()
    return {"ok": True, "message": f"@{username} の収集を開始しました"}


@app.post("/api/instagram/graph/collect/stop")
async def ig_graph_collect_stop():
    if _ig_graph_job:
        _ig_graph_job.stop()
    return {"ok": True}


@app.get("/api/instagram/graph/collect/stream")
async def ig_graph_collect_stream():
    async def generator():
        last_idx = 0
        idle_count = 0
        while True:
            new_entries = (_ig_graph_job.progress_log[last_idx:] if _ig_graph_job else [])
            if new_entries:
                idle_count = 0
                for entry in new_entries:
                    last_idx += 1
                    yield f"data: {json.dumps(entry, ensure_ascii=False)}\n\n"
                    if entry.get("type") in ("done", "error", "stopped"):
                        return
            else:
                idle_count += 1
                if idle_count % 4 == 0:
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/instagram/graph/stats")
async def ig_graph_stats():
    """Graph API収集データの統計"""
    running = _ig_graph_job.is_running() if _ig_graph_job else False

    def _load():
        info = db.get_ig_token()
        username = info["username"] if (info and info.get("is_set")) else None
        stats = db.get_instagram_stats(username)
        hashtags = db.get_instagram_hashtags(username, limit=30)
        return stats, hashtags

    stats, hashtags = await asyncio.to_thread(_load)
    return {"running": running, "stats": stats, "hashtags": hashtags}


if __name__ == "__main__":
    print("=" * 50)
    print(" Lifestudio ERP アナライザ 起動中...")
    print(" http://localhost:8000 をブラウザで開いてください")
    print("=" * 50)
    webbrowser.open("http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
