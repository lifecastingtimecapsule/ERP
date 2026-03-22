"""
SQLite データベース操作モジュール
"""
import sqlite3
import threading
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "lifeerp.db"


class Database:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()

    def _get_conn(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def close(self):
        """既存の接続を閉じる（DB再作成時に使用）"""
        if hasattr(self._local, "conn") and self._local.conn is not None:
            try:
                self._local.conn.close()
            except Exception:
                pass
            self._local.conn = None

    # スキーマ定義（個別CREATE文のリスト）
    _SCHEMA_STATEMENTS = [
        """CREATE TABLE IF NOT EXISTS reservations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kokyaku_no TEXT NOT NULL,
            yoyaku_date TEXT,
            time TEXT,
            shoot_type TEXT,
            received_date TEXT,
            branch_id INTEGER DEFAULT 49
        )""",
        "CREATE INDEX IF NOT EXISTS idx_res_kokyaku ON reservations(kokyaku_no, branch_id)",
        """CREATE TABLE IF NOT EXISTS eseq_map (
            kokyaku_no TEXT NOT NULL,
            e_seq TEXT NOT NULL,
            branch_id INTEGER DEFAULT 49,
            PRIMARY KEY (kokyaku_no, branch_id)
        )""",
        """CREATE TABLE IF NOT EXISTS children (
            kokyaku_no TEXT NOT NULL,
            child_number INTEGER NOT NULL,
            gender TEXT,
            birthday TEXT,
            branch_id INTEGER DEFAULT 49,
            fetched_at TEXT,
            PRIMARY KEY (kokyaku_no, child_number, branch_id)
        )""",
        """CREATE TABLE IF NOT EXISTS customer_info (
            kokyaku_no TEXT NOT NULL,
            address_raw TEXT,
            prefecture TEXT,
            city TEXT,
            mother_birthday TEXT,
            branch_id INTEGER DEFAULT 49,
            PRIMARY KEY (kokyaku_no, branch_id)
        )""",
        """CREATE TABLE IF NOT EXISTS collection_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS instagram_posts (
            pk TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            taken_at INTEGER,
            media_type INTEGER,
            product_type TEXT,
            like_count INTEGER DEFAULT 0,
            comment_count INTEGER DEFAULT 0,
            play_count INTEGER DEFAULT 0,
            caption TEXT,
            shortcode TEXT,
            fetched_at TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_ig_username ON instagram_posts(username)",
        "CREATE INDEX IF NOT EXISTS idx_ig_taken_at ON instagram_posts(taken_at)",
    ]

    def init(self):
        # 既存接続をクリア（再初期化対応）
        self.close()

        # ジャーナル/WAL/SHMファイルの破損回復
        for ext in ["-journal", "-wal", "-shm"]:
            p = Path(str(self.db_path) + ext)
            if p.exists():
                try:
                    p.unlink()
                    print(f"[INFO] 古い{ext}ファイルを削除しました")
                except OSError:
                    try:
                        with open(p, "wb") as f:
                            f.truncate(0)
                        print(f"[INFO] {ext}ファイルをクリアしました")
                    except OSError as e2:
                        print(f"[WARN] {ext}ファイル処理失敗（無視）: {e2}")

        conn = self._get_conn()

        # ジャーナルモードを設定（WAL → MEMORY → OFF の順に試行）
        journal_mode = "off"
        for mode in ["WAL", "MEMORY", "OFF"]:
            try:
                result = conn.execute(f"PRAGMA journal_mode={mode};").fetchone()
                journal_mode = result[0] if result else mode.lower()
                # テスト書き込みで確認
                conn.execute("SELECT 1")
                print(f"[INFO] journal_mode={journal_mode}")
                break
            except Exception:
                self.close()
                conn = self._get_conn()
                continue

        # DB整合性チェック（journal_mode=off ではスキップ可能）
        if journal_mode != "off":
            try:
                result = conn.execute("PRAGMA integrity_check;").fetchone()
                if result[0] != "ok":
                    raise sqlite3.DatabaseError(f"DB破損検出: {result[0]}")
            except Exception as e:
                print(f"[WARN] DB整合性チェック失敗: {e}")
                raise

        # スキーマ作成（個別execute で実行 → executescriptのジャーナル問題を回避）
        for stmt in self._SCHEMA_STATEMENTS:
            conn.execute(stmt)
        conn.commit()

        # 既存テーブルへの列追加マイグレーション
        try:
            conn.execute("ALTER TABLE customer_info ADD COLUMN mother_birthday TEXT")
            conn.commit()
        except Exception:
            pass  # 既に存在する場合はスキップ

    # ─── 予約データ ─────────────────────────────────────

    def import_reservations(self, records, branch_id=49):
        conn = self._get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM reservations WHERE branch_id=?", (branch_id,)
        ).fetchone()[0]
        if count >= len(records):
            return count
        with self._lock:
            for r in records:
                conn.execute(
                    """INSERT OR IGNORE INTO reservations
                       (kokyaku_no, yoyaku_date, time, shoot_type, received_date, branch_id)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        r.get("e_seq", ""),
                        r.get("yoyaku_date", ""),
                        r.get("time", ""),
                        r.get("shoot_type", ""),
                        r.get("received_date", ""),
                        branch_id,
                    ),
                )
            conn.commit()
        return len(records)

    def get_reservations(self, branch_id=49):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM reservations WHERE branch_id=? ORDER BY yoyaku_date",
            (branch_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_unique_kokyaku(self, branch_id=49):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT DISTINCT kokyaku_no FROM reservations WHERE branch_id=?", (branch_id,)
        ).fetchall()
        return {r[0] for r in rows}

    # ─── e_seq マップ ────────────────────────────────────

    def save_eseq_batch(self, mapping, branch_id=49):
        conn = self._get_conn()
        with self._lock:
            for kno, e_seq in mapping.items():
                conn.execute(
                    "INSERT OR REPLACE INTO eseq_map (kokyaku_no, e_seq, branch_id) VALUES (?,?,?)",
                    (kno, e_seq, branch_id),
                )
            conn.commit()

    def get_eseq_map(self, branch_id=49):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT kokyaku_no, e_seq FROM eseq_map WHERE branch_id=?", (branch_id,)
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    # ─── 顧客データ（子供 + 住所） ──────────────────────

    def save_customer_data(self, kokyaku_no, children, address_raw="", prefecture="", city="", mother_birthday="", branch_id=49):
        conn = self._get_conn()
        now = datetime.now().isoformat()
        with self._lock:
            # 子供データ
            conn.execute(
                "DELETE FROM children WHERE kokyaku_no=? AND branch_id=?",
                (kokyaku_no, branch_id),
            )
            if not children:
                conn.execute(
                    """INSERT INTO children (kokyaku_no, child_number, gender, birthday, branch_id, fetched_at)
                       VALUES (?,0,NULL,NULL,?,?)""",
                    (kokyaku_no, branch_id, now),
                )
            else:
                for child in children:
                    conn.execute(
                        """INSERT INTO children (kokyaku_no, child_number, gender, birthday, branch_id, fetched_at)
                           VALUES (?,?,?,?,?,?)""",
                        (
                            kokyaku_no,
                            child.get("子供番号", 0),
                            child.get("性別"),
                            child.get("誕生日"),
                            branch_id,
                            now,
                        ),
                    )
            # 住所・母親データ
            conn.execute(
                """INSERT OR REPLACE INTO customer_info (kokyaku_no, address_raw, prefecture, city, mother_birthday, branch_id)
                   VALUES (?,?,?,?,?,?)""",
                (kokyaku_no, address_raw, prefecture, city, mother_birthday or None, branch_id),
            )
            conn.commit()

    def get_fetched_kokyaku(self, branch_id=49):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT DISTINCT kokyaku_no FROM children WHERE branch_id=?", (branch_id,)
        ).fetchall()
        return {r[0] for r in rows}

    def get_all_customer_data(self, branch_id=49):
        """kokyaku_no → {children: [...], address: {...}} の辞書"""
        conn = self._get_conn()
        child_rows = conn.execute(
            """SELECT kokyaku_no, child_number, gender, birthday
               FROM children WHERE branch_id=?
               ORDER BY kokyaku_no, child_number""",
            (branch_id,),
        ).fetchall()
        addr_rows = conn.execute(
            "SELECT kokyaku_no, address_raw, prefecture, city, mother_birthday FROM customer_info WHERE branch_id=?",
            (branch_id,),
        ).fetchall()

        result = {}
        for r in child_rows:
            kno = r[0]
            if kno not in result:
                result[kno] = {"children": [], "address": {}, "mother_birthday": ""}
            if r[1] and r[1] > 0 and r[3]:
                result[kno]["children"].append(
                    {"子供番号": r[1], "性別": r[2], "誕生日": r[3]}
                )
        for r in addr_rows:
            kno = r[0]
            if kno not in result:
                result[kno] = {"children": [], "address": {}, "mother_birthday": ""}
            result[kno]["address"] = {
                "raw": r[1] or "",
                "prefecture": r[2] or "",
                "city": r[3] or "",
            }
            result[kno]["mother_birthday"] = r[4] or ""
        return result

    # ─── 統計 ────────────────────────────────────────────

    def get_collection_stats(self, branch_id=49):
        conn = self._get_conn()
        total_res = conn.execute(
            "SELECT COUNT(*) FROM reservations WHERE branch_id=?", (branch_id,)
        ).fetchone()[0]
        total_customers = conn.execute(
            "SELECT COUNT(DISTINCT kokyaku_no) FROM reservations WHERE branch_id=?", (branch_id,)
        ).fetchone()[0]
        fetched = conn.execute(
            "SELECT COUNT(DISTINCT kokyaku_no) FROM children WHERE branch_id=?", (branch_id,)
        ).fetchone()[0]
        children_with_bday = conn.execute(
            "SELECT COUNT(*) FROM children WHERE branch_id=? AND birthday IS NOT NULL AND child_number > 0",
            (branch_id,),
        ).fetchone()[0]
        with_address = conn.execute(
            "SELECT COUNT(*) FROM customer_info WHERE branch_id=? AND city IS NOT NULL AND city != ''",
            (branch_id,),
        ).fetchone()[0]
        return {
            "total_reservations": total_res,
            "total_customers": total_customers,
            "fetched_customers": fetched,
            "children_with_birthday": children_with_bday,
            "customers_with_address": with_address,
        }

    # ─── 分析クエリ ──────────────────────────────────────

    def get_analytics(self, branch_id=49):
        conn = self._get_conn()

        # 1. 来店回数分布
        visit_rows = conn.execute(
            """SELECT kokyaku_no, COUNT(*) as cnt
               FROM reservations WHERE branch_id=?
               GROUP BY kokyaku_no""",
            (branch_id,),
        ).fetchall()
        visit_dist = {}
        visit_map = {}  # kokyaku_no → 来店回数
        for r in visit_rows:
            kno, cnt = r[0], r[1]
            visit_map[kno] = cnt
            bucket = str(cnt) if cnt <= 5 else "6以上"
            visit_dist[bucket] = visit_dist.get(bucket, 0) + 1

        # 2. 撮影種類×月別（月単位件数）
        monthly_rows = conn.execute(
            """SELECT substr(yoyaku_date,1,7) as month, shoot_type, COUNT(*) as cnt
               FROM reservations WHERE branch_id=? AND yoyaku_date IS NOT NULL
               GROUP BY month, shoot_type ORDER BY month""",
            (branch_id,),
        ).fetchall()
        monthly = [{"month": r[0], "shoot_type": r[1], "count": r[2]} for r in monthly_rows]

        # 2b. 月別合計件数
        monthly_total_rows = conn.execute(
            """SELECT substr(yoyaku_date,1,7) as month, COUNT(*) as cnt
               FROM reservations WHERE branch_id=? AND yoyaku_date IS NOT NULL
               GROUP BY month ORDER BY month""",
            (branch_id,),
        ).fetchall()
        monthly_total = [{"month": r[0], "count": r[1]} for r in monthly_total_rows]

        # 3. 撮影種類ランキング
        type_rows = conn.execute(
            """SELECT shoot_type, COUNT(*) as cnt
               FROM reservations WHERE branch_id=?
               GROUP BY shoot_type ORDER BY cnt DESC""",
            (branch_id,),
        ).fetchall()
        shoot_types = [{"type": r[0], "count": r[1]} for r in type_rows]

        # 4. 子供の撮影時年齢分布（kokyaku_no・shoot_typeも取得して後続分析に使う）
        age_rows = conn.execute(
            """SELECT r.kokyaku_no, r.yoyaku_date, r.shoot_type, c.birthday
               FROM reservations r
               JOIN children c ON r.kokyaku_no=c.kokyaku_no AND c.branch_id=r.branch_id
               WHERE r.branch_id=? AND c.birthday IS NOT NULL AND c.child_number > 0""",
            (branch_id,),
        ).fetchall()
        age_dist = {}
        for row in age_rows:
            age = _calc_age(row[3], row[1])
            if 0 <= age <= 15:
                age_dist[age] = age_dist.get(age, 0) + 1

        # 5. 年齢×撮影種類クロス集計
        cross = {}
        shoot_type_set = set()
        for row in age_rows:
            stype = row[2] or "不明"
            age = _calc_age(row[3], row[1])
            if age < 0:
                continue
            bucket = _age_bucket(age)
            shoot_type_set.add(stype)
            cross.setdefault(bucket, {})[stype] = cross.get(bucket, {}).get(stype, 0) + 1

        # 6. 兄弟順×撮影傾向
        sibling_rows = conn.execute(
            """SELECT c.child_number, r.shoot_type, COUNT(*) as cnt
               FROM children c
               JOIN reservations r ON c.kokyaku_no=r.kokyaku_no AND c.branch_id=r.branch_id
               WHERE c.branch_id=? AND c.child_number > 0
               GROUP BY c.child_number, r.shoot_type""",
            (branch_id,),
        ).fetchall()
        sibling_order = {}
        for row in sibling_rows:
            cn = row[0]
            label = f"第{cn}子" if cn <= 3 else "第4子以上"
            stype = row[1] or "不明"
            cnt = row[2]
            sibling_order.setdefault(label, {})[stype] = sibling_order.get(label, {}).get(stype, 0) + cnt

        # 7. 兄弟人数×来店回数
        sibling_count_rows = conn.execute(
            """SELECT c.kokyaku_no, MAX(c.child_number) as num_children
               FROM children c
               WHERE c.branch_id=? AND c.child_number > 0
               GROUP BY c.kokyaku_no""",
            (branch_id,),
        ).fetchall()
        sibling_visit = {}
        for row in sibling_count_rows:
            kno = row[0]
            num_c = row[1]
            label = str(num_c) if num_c <= 3 else "4以上"
            visits = visit_map.get(kno, 1)
            sibling_visit.setdefault(label, []).append(visits)
        sibling_visit_summary = {
            k: {
                "avg": round(sum(v) / len(v), 1) if v else 0,
                "count": len(v),
                "total_visits": sum(v),
            }
            for k, v in sibling_visit.items()
        }

        # 8. 地域別来店ランキング（県＋市レベルのみ）
        area_rows = conn.execute(
            """SELECT ci.prefecture, ci.city,
                      COUNT(DISTINCT r.kokyaku_no) as customer_cnt,
                      COUNT(*) as visit_cnt
               FROM reservations r
               JOIN customer_info ci ON r.kokyaku_no=ci.kokyaku_no AND ci.branch_id=r.branch_id
               WHERE r.branch_id=?
                 AND ci.city IS NOT NULL AND ci.city != ''
                 AND ci.city LIKE '%市'
               GROUP BY ci.prefecture, ci.city
               ORDER BY customer_cnt DESC LIMIT 30""",
            (branch_id,),
        ).fetchall()
        area_ranking = [
            {"prefecture": r[0] or "", "city": r[1], "customers": r[2], "visits": r[3]}
            for r in area_rows
        ]

        # 9. 地域×撮影種類（県＋市レベルのみ）
        area_type_rows = conn.execute(
            """SELECT ci.prefecture, ci.city, r.shoot_type, COUNT(*) as cnt
               FROM reservations r
               JOIN customer_info ci ON r.kokyaku_no=ci.kokyaku_no AND ci.branch_id=r.branch_id
               WHERE r.branch_id=?
                 AND ci.city IS NOT NULL AND ci.city != ''
                 AND ci.city LIKE '%市'
               GROUP BY ci.prefecture, ci.city, r.shoot_type
               ORDER BY cnt DESC""",
            (branch_id,),
        ).fetchall()
        area_type = {}
        for row in area_type_rows:
            key = f"{row[0] or ''}{row[1]}"
            stype = row[2] or "不明"
            area_type.setdefault(key, {})[stype] = row[3]

        # 10. 0歳来店者の次回撮影連鎖
        #     0歳時に来店した顧客を特定し、その次の撮影種類を集計
        zero_age_chain = {}
        cust_visits_map = {}  # kokyaku_no → [(date, shoot_type, age), ...]
        for row in age_rows:
            kno, date, stype, bday = row[0], row[1], row[2], row[3]
            age = _calc_age(bday, date)
            cust_visits_map.setdefault(kno, []).append((date, stype, age))

        for kno, visits in cust_visits_map.items():
            visits.sort(key=lambda x: x[0])
            for i, (date, stype, age) in enumerate(visits):
                if age == 0 and i + 1 < len(visits):
                    next_stype = visits[i + 1][1]
                    zero_age_chain.setdefault(stype, {})[next_stype] = (
                        zero_age_chain.get(stype, {}).get(next_stype, 0) + 1
                    )

        # 11. 初来店→次回撮影分析（月単位件数付き）
        all_res = conn.execute(
            """SELECT kokyaku_no, shoot_type, yoyaku_date
               FROM reservations WHERE branch_id=? ORDER BY kokyaku_no, yoyaku_date""",
            (branch_id,),
        ).fetchall()
        first_visit_chain = {}       # {初回種類: {2回目種類: 件数}}
        first_visit_by_month = {}    # {月: {種類: 件数}} 初来店のみ
        seen_first = {}              # kokyaku_no → 初回shoot_type
        seen_second = set()          # 2回目を記録済みのkokyaku_no

        for row in all_res:
            kno, stype, date = row[0], row[1], row[2]
            month = date[:7] if date else ""
            if kno not in seen_first:
                # 初来店
                seen_first[kno] = stype
                if month:
                    first_visit_by_month.setdefault(month, {})[stype] = (
                        first_visit_by_month.get(month, {}).get(stype, 0) + 1
                    )
            elif kno not in seen_second:
                # 2回目の来店（異なる種類かどうかに関わらず記録）
                seen_second.add(kno)
                from_t = seen_first[kno]
                first_visit_chain.setdefault(from_t, {})[stype] = (
                    first_visit_chain.get(from_t, {}).get(stype, 0) + 1
                )

        # 初来店×月別をリスト形式に変換
        first_visit_monthly = [
            {"month": m, "shoot_type": st, "count": cnt}
            for m, types in sorted(first_visit_by_month.items())
            for st, cnt in types.items()
        ]

        # 12. 母親年齢分布
        mother_rows = conn.execute(
            """SELECT ci.mother_birthday, r.yoyaku_date
               FROM customer_info ci
               JOIN reservations r ON ci.kokyaku_no=r.kokyaku_no AND ci.branch_id=r.branch_id
               WHERE ci.branch_id=? AND ci.mother_birthday IS NOT NULL AND ci.mother_birthday != ''""",
            (branch_id,),
        ).fetchall()
        mother_age_dist = {}
        seen_mother = set()
        for row in mother_rows:
            mbday, ref_date = row[0], row[1]
            age = _calc_age(mbday, ref_date)
            if 20 <= age <= 60:
                bucket = f"{(age // 5) * 5}代前半" if age % 10 < 5 else f"{(age // 5) * 5 - (age // 5) * 5 % 10 + 5}代後半"
                # シンプルに5歳刻みで集計
                bucket = f"{age // 5 * 5}〜{age // 5 * 5 + 4}歳"
                mother_age_dist[bucket] = mother_age_dist.get(bucket, 0) + 1

        return {
            "visit_distribution": visit_dist,
            "monthly_by_type": monthly,
            "monthly_total": monthly_total,
            "shoot_types": shoot_types,
            "age_distribution": {str(k): v for k, v in sorted(age_dist.items())},
            "age_type_cross": cross,
            "shoot_type_list": sorted(shoot_type_set),
            "sibling_order": sibling_order,
            "sibling_visit_summary": sibling_visit_summary,
            "area_ranking": area_ranking,
            "area_type": area_type,
            "zero_age_chain": zero_age_chain,
            "first_visit_chain": first_visit_chain,
            "first_visit_monthly": first_visit_monthly,
            "mother_age_distribution": {k: v for k, v in sorted(mother_age_dist.items())},
        }

    # ─── Instagram ───────────────────────────────────────

    def save_instagram_posts(self, posts: list):
        """Instagram投稿データを一括保存（重複はスキップ）"""
        conn = self._get_conn()
        now = datetime.now().isoformat()
        with self._lock:
            for p in posts:
                conn.execute(
                    """INSERT OR REPLACE INTO instagram_posts
                       (pk, username, taken_at, media_type, product_type,
                        like_count, comment_count, play_count, caption, shortcode, fetched_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        p.get("pk"), p.get("username"), p.get("taken_at"),
                        p.get("media_type"), p.get("product_type"),
                        p.get("like_count", 0), p.get("comment_count", 0),
                        p.get("play_count", 0), p.get("caption"),
                        p.get("shortcode"), now,
                    ),
                )
            conn.commit()

    def get_instagram_stats(self, username: str = None):
        """Instagram投稿の集計データを返す"""
        conn = self._get_conn()
        where = "WHERE username=?" if username else ""
        params = (username,) if username else ()

        # 投稿総数
        total = conn.execute(
            f"SELECT COUNT(*) FROM instagram_posts {where}", params
        ).fetchone()[0]

        # メディア種別
        type_rows = conn.execute(
            f"""SELECT media_type, COUNT(*) as cnt FROM instagram_posts {where}
                GROUP BY media_type ORDER BY cnt DESC""", params
        ).fetchall()
        media_types = [{"type": r[0], "count": r[1]} for r in type_rows]

        # 月別投稿数
        monthly_rows = conn.execute(
            f"""SELECT strftime('%Y-%m', datetime(taken_at, 'unixepoch', 'localtime')) as month,
                       COUNT(*) as cnt, AVG(like_count) as avg_likes, AVG(comment_count) as avg_comments
                FROM instagram_posts {where} AND taken_at IS NOT NULL
                {'' if not where else 'AND taken_at IS NOT NULL'.replace('AND','',1) if not where else ''}
                GROUP BY month ORDER BY month""",
            params
        ).fetchall()

        # 月別のクエリを修正
        monthly_rows = conn.execute(
            f"""SELECT strftime('%Y-%m', datetime(taken_at, 'unixepoch', 'localtime')) as month,
                       COUNT(*) as cnt,
                       CAST(AVG(like_count) AS INTEGER) as avg_likes,
                       CAST(AVG(comment_count) AS INTEGER) as avg_comments,
                       SUM(play_count) as total_plays
                FROM instagram_posts
                {"WHERE username=?" if username else ""}
                GROUP BY month ORDER BY month""",
            params
        ).fetchall()
        monthly = [
            {"month": r[0], "count": r[1], "avg_likes": r[2],
             "avg_comments": r[3], "total_plays": r[4] or 0}
            for r in monthly_rows
        ]

        # 曜日別投稿数
        weekday_rows = conn.execute(
            f"""SELECT strftime('%w', datetime(taken_at, 'unixepoch', 'localtime')) as wd,
                       COUNT(*) as cnt
                FROM instagram_posts
                {"WHERE username=?" if username else ""}
                GROUP BY wd ORDER BY wd""",
            params
        ).fetchall()
        wd_names = ["日","月","火","水","木","金","土"]
        weekday = [{"day": wd_names[int(r[0])], "count": r[1]} for r in weekday_rows]

        # エンゲージメント上位投稿
        top_rows = conn.execute(
            f"""SELECT pk, shortcode, taken_at, like_count, comment_count,
                       play_count, caption, media_type, product_type
                FROM instagram_posts
                {"WHERE username=?" if username else ""}
                ORDER BY (like_count + comment_count) DESC LIMIT 10""",
            params
        ).fetchall()
        top_posts = [
            {"pk": r[0], "shortcode": r[1], "taken_at": r[2],
             "likes": r[3], "comments": r[4], "plays": r[5] or 0,
             "caption": (r[6] or "")[:80], "media_type": r[7],
             "product_type": r[8]}
            for r in top_rows
        ]

        # ユーザーリスト
        users = conn.execute(
            "SELECT DISTINCT username FROM instagram_posts ORDER BY username"
        ).fetchall()

        # 時間帯別投稿数
        hour_rows = conn.execute(
            f"""SELECT CAST(strftime('%H', datetime(taken_at, 'unixepoch', 'localtime')) AS INTEGER) as hour,
                       COUNT(*) as cnt
                FROM instagram_posts
                {"WHERE username=?" if username else ""}
                GROUP BY hour ORDER BY hour""",
            params
        ).fetchall()
        hourly = [{"hour": r[0], "count": r[1]} for r in hour_rows]

        return {
            "total": total,
            "users": [r[0] for r in users],
            "media_types": media_types,
            "monthly": monthly,
            "weekday": weekday,
            "hourly": hourly,
            "top_posts": top_posts,
        }

    def get_instagram_hashtags(self, username: str = None, limit: int = 30) -> list:
        """キャプションからハッシュタグを抽出して頻度集計"""
        import re
        from collections import Counter
        conn = self._get_conn()
        where = "WHERE username=? AND caption IS NOT NULL" if username else "WHERE caption IS NOT NULL"
        params = (username,) if username else ()
        rows = conn.execute(
            f"SELECT caption FROM instagram_posts {where}", params
        ).fetchall()
        tags = Counter()
        for (caption,) in rows:
            if caption:
                for tag in re.findall(r'#[\w\u3040-\u30ff\u4e00-\u9fff]+', caption):
                    tags[tag.lower()] += 1
        return [{"tag": t, "count": c} for t, c in tags.most_common(limit)]

    def save_ig_token(self, token: str, ig_user_id: str, username: str):
        """Graph APIトークンをDBに安全保存"""
        self.set_state("ig_graph_token", token)
        self.set_state("ig_graph_user_id", ig_user_id)
        self.set_state("ig_graph_username", username)

    def get_ig_token(self) -> dict:
        """保存済みトークン情報を取得"""
        token = self.get_state("ig_graph_token") or ""
        return {
            "token": token,
            "token_masked": ("*" * (len(token) - 6) + token[-6:]) if len(token) > 6 else "未設定",
            "ig_user_id": self.get_state("ig_graph_user_id") or "",
            "username": self.get_state("ig_graph_username") or "",
            "is_set": bool(token),
        }

    def get_instagram_fetch_count(self, username: str):
        conn = self._get_conn()
        return conn.execute(
            "SELECT COUNT(*) FROM instagram_posts WHERE username=?", (username,)
        ).fetchone()[0]

    def get_state(self, key):
        conn = self._get_conn()
        row = conn.execute(
            "SELECT value FROM collection_state WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else None

    def set_state(self, key, value):
        conn = self._get_conn()
        with self._lock:
            conn.execute(
                "INSERT OR REPLACE INTO collection_state (key, value) VALUES (?,?)",
                (key, value),
            )
            conn.commit()


# ─── ヘルパー関数 ────────────────────────────────────────

def _calc_age(birthday_str, reference_date_str):
    try:
        by, bm, bd = map(int, birthday_str.split("-"))
        ry, rm, rd = map(int, reference_date_str.split("-"))
        age = ry - by
        if (rm, rd) < (bm, bd):
            age -= 1
        return max(age, 0)
    except Exception:
        return -1


def _age_bucket(age):
    if age <= 1:
        return "0-1歳"
    elif age <= 3:
        return "2-3歳"
    elif age <= 5:
        return "4-5歳"
    elif age <= 7:
        return "6-7歳"
    else:
        return "8歳以上"
