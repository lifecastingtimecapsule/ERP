"""
Instagram投稿データ収集モジュール
ブラウザのInstagramセッションCookieを使い、
/api/v1/feed/user/{user_id}/ エンドポイントから投稿を収集する。
"""
import re
import time
import threading
import requests
from datetime import datetime


IG_APP_ID = "936619743392459"
BASE_URL = "https://www.instagram.com"

ACCOUNTS = {
    "amoretto_lifecasting_aichi": None,   # user_idはAPI取得
    "lifestudio_toyokawa": None,
}


def make_ig_session(cookie_str: str) -> requests.Session:
    """Cookie文字列からrequestsセッションを生成"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "x-ig-app-id": IG_APP_ID,
        "x-requested-with": "XMLHttpRequest",
        "Referer": "https://www.instagram.com/",
        "Accept": "application/json",
        "Accept-Language": "ja-JP,ja;q=0.9",
    })
    # Cookie文字列をパース
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            session.cookies.set(k.strip(), v.strip(), domain=".instagram.com")

    # csrftokenをヘッダーにも設定
    csrf = session.cookies.get("csrftoken", domain=".instagram.com")
    if csrf:
        session.headers["x-csrftoken"] = csrf

    return session


def test_ig_session(session: requests.Session) -> bool:
    """セッションの有効性確認"""
    try:
        r = session.get(
            f"{BASE_URL}/api/v1/accounts/current_user/?edit=true",
            timeout=10,
        )
        return r.status_code == 200 and "username" in r.text
    except Exception:
        return False


def get_user_id(session: requests.Session, username: str) -> str | None:
    """ユーザー名からuser_idを取得"""
    try:
        r = session.get(
            f"{BASE_URL}/api/v1/users/web_profile_info/?username={username}",
            timeout=15,
        )
        data = r.json()
        return data.get("data", {}).get("user", {}).get("id")
    except Exception:
        return None


def fetch_posts_page(session: requests.Session, user_id: str, max_id: str = "") -> dict:
    """1ページ分（最大50件）の投稿を取得"""
    params = {"count": 50}
    if max_id:
        params["max_id"] = max_id
    try:
        r = session.get(
            f"{BASE_URL}/api/v1/feed/user/{user_id}/",
            params=params,
            timeout=20,
        )
        return r.json()
    except Exception as e:
        return {"status": "error", "error": str(e)}


def parse_post(item: dict, username: str) -> dict:
    """APIレスポンスの1投稿をDBに保存する形式に変換"""
    return {
        "pk": item.get("pk"),
        "username": username,
        "taken_at": item.get("taken_at"),
        "media_type": item.get("media_type"),     # 1=写真, 2=動画, 8=カルーセル
        "product_type": item.get("product_type"),  # feed / clips / carousel_container
        "like_count": item.get("like_count", 0),
        "comment_count": item.get("comment_count", 0),
        "play_count": item.get("play_count", 0),
        "caption": (item.get("caption") or {}).get("text", ""),
        "shortcode": item.get("code"),
    }


# ─── コレクションジョブ ───────────────────────────────────

class IGCollectionJob:
    def __init__(self, db, cookie_str: str, usernames: list):
        self.db = db
        self.cookie_str = cookie_str
        self.usernames = usernames
        self._stop = False
        self._running = False
        self._thread = None
        self.progress_log = []

    def start(self):
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop = True

    def is_running(self):
        return self._running

    def _log(self, msg_type: str, message: str, current=0, total=0):
        pct = round(current / total * 100) if total > 0 else 0
        self.progress_log.append({
            "type": msg_type,
            "message": message,
            "current": current,
            "total": total,
            "percent": pct,
        })

    def _run(self):
        self._running = True
        session = make_ig_session(self.cookie_str)

        self._log("phase", "📸 Instagram接続確認中...")
        if not test_ig_session(session):
            self._log("error", "❌ Instagramセッションが無効です。Cookieを確認してください。")
            self._running = False
            return

        self._log("info", "✅ Instagram接続OK")

        total_saved = 0

        for username in self.usernames:
            if self._stop:
                break

            self._log("phase", f"🔍 @{username} のユーザーID取得中...")
            user_id = get_user_id(session, username)
            if not user_id:
                self._log("error", f"❌ @{username} のユーザーIDが取得できませんでした")
                continue

            self._log("info", f"✅ @{username} ID={user_id}")

            already = self.db.get_instagram_fetch_count(username)
            self._log("info", f"  既存保存数: {already}件")

            max_id = ""
            page = 0
            account_total = 0

            while not self._stop:
                page += 1
                data = fetch_posts_page(session, user_id, max_id)

                if data.get("status") != "ok":
                    self._log("error", f"❌ API エラー: {data.get('error', data.get('status'))}")
                    break

                items = data.get("items", [])
                if not items:
                    break

                posts = [parse_post(item, username) for item in items]
                self.db.save_instagram_posts(posts)
                account_total += len(posts)
                total_saved += len(posts)

                self._log(
                    "progress",
                    f"@{username}: {account_total}件収集中... (ページ{page})",
                    current=account_total,
                    total=account_total + (50 if data.get("more_available") else 0),
                )

                if not data.get("more_available"):
                    break

                max_id = data.get("next_max_id", "")
                if not max_id:
                    break

                time.sleep(0.8)  # レート制限対策

            self._log("info", f"✅ @{username}: 合計{account_total}件完了")

        if self._stop:
            self._log("stopped", f"⏹ 中断しました（保存済み: {total_saved}件）")
        else:
            self._log("done", f"🎉 Instagram収集完了！合計{total_saved}件保存しました")

        self._running = False
