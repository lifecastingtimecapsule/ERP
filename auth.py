"""
Lifestudio ERP アナライザ - 認証モジュール
シンプルなセッションCookieベースの認証
"""
import hashlib
import hmac
import json
import os
import secrets
from pathlib import Path

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

# ─── 設定ファイルパス ─────────────────────────────
BASE_DIR = Path(__file__).parent
AUTH_CONFIG = BASE_DIR / "auth_config.json"

# セッション有効期間（秒）
SESSION_MAX_AGE = 60 * 60 * 8  # 8時間
COOKIE_NAME = "erp_session"

# ─── 初期設定 ─────────────────────────────────────

def _load_config() -> dict:
    if AUTH_CONFIG.exists():
        with open(AUTH_CONFIG, encoding="utf-8") as f:
            return json.load(f)
    # 初回：デフォルト設定を生成
    cfg = {
        "secret_key": secrets.token_hex(32),
        "username": "admin",
        "password_hash": _hash_password("lifestudio"),
    }
    _save_config(cfg)
    return cfg


def _save_config(cfg: dict):
    with open(AUTH_CONFIG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.chmod(AUTH_CONFIG, 0o600)


def _hash_password(password: str) -> str:
    salt = "lifestudio_erp_salt"
    return hashlib.sha256(f"{salt}{password}".encode()).hexdigest()


# ─── 公開API ──────────────────────────────────────

def verify_credentials(username: str, password: str) -> bool:
    """ユーザー名とパスワードを検証"""
    cfg = _load_config()
    return (
        hmac.compare_digest(cfg["username"], username)
        and hmac.compare_digest(cfg["password_hash"], _hash_password(password))
    )


def create_session_token() -> str:
    """ログイン成功時にセッショントークンを生成"""
    cfg = _load_config()
    s = URLSafeTimedSerializer(cfg["secret_key"])
    return s.dumps({"user": cfg["username"]})


def verify_session_token(token: str) -> bool:
    """セッショントークンを検証"""
    if not token:
        return False
    try:
        cfg = _load_config()
        s = URLSafeTimedSerializer(cfg["secret_key"])
        s.loads(token, max_age=SESSION_MAX_AGE)
        return True
    except (BadSignature, SignatureExpired):
        return False


def change_password(new_password: str):
    """パスワードを変更"""
    cfg = _load_config()
    cfg["password_hash"] = _hash_password(new_password)
    _save_config(cfg)


def get_username() -> str:
    return _load_config()["username"]
