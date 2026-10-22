# -*- coding: utf-8 -*-
"""验证 fixed_files_v3 的三层修复（离线仿真，不联网）"""
import ast
import contextlib
import logging
import os
import sqlite3
import sys
import types
from datetime import datetime, time as dt_time

import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, 'test3'))

logging.basicConfig(level=logging.INFO, format='%(levelname)-7s %(message)s')
logging.getLogger('utils').setLevel(logging.INFO)

from utils.data_initializer import (  # noqa: E402
    DataInitializer, StaleDataSourceError,
)

PASS, FAIL = [], []


def check(name, cond, extra=''):
    (PASS if cond else FAIL).append(name)
    print(f"{'✓' if cond else '✗'} {name} {extra}")


# ---------------- 假数据库 ----------------
class FakeDB:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            "CREATE TABLE stock_kline(code TEXT, date TEXT, open REAL, high REAL,"
            " low REAL, close REAL, volume INTEGER)")
        self.conn.execute("CREATE TABLE stock_basic(code TEXT, name TEXT, market_cap REAL)")
        self.conn.commit()

    def connect(self):
        return self.conn

    def query(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    @contextlib.contextmanager
    def transaction(self):
        try:
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def max_date(self):
        row = self.conn.execute("SELECT MAX(date) FROM stock_kline").fetchone()
        return row[0] if row and row[0] else None

    def rows(self):
        return self.conn.execute("SELECT COUNT(*) FROM stock_kline").fetchone()[0]


class FakeFetcher:
    """tf_end/tc_end 控制两个数据源各自能看到的最新K线日期"""

    def __init__(self, tf_end, tc_end, tf_ok=True):
        self.tf_end = tf_end
        self.tc_end = tc_end
        self.tf_ok = tf_ok
        self.tf_calls = 0
        self.tc_calls = 0

    @staticmethod
    def _df(end, bars=60):
        dates = pd.bdate_range(end=end, periods=bars).strftime('%Y-%m-%d').tolist()
        return pd.DataFrame({
            'date': dates,
            'open': [10.0] * bars, 'high': [10.5] * bars,
            'low': [9.5] * bars, 'close': [10.2] * bars,
            'volume': [100000] * bars,
        })

    def _fetch_stock_batch_tickflow(self, codes, days):
        self.tf_calls += 1
        if not self.tf_ok:
            return ({}, False)
        return ({c: self._df(self.tf_end) for c in codes}, True)

    def _fetch_stock_batch_tencent(self, codes, years=3, concurrency=2):
        self.tc_calls += 1
        return {c: self._df(self.tc_end) for c in codes}


def make_di(fetcher, db=None):
    db = db or FakeDB()
    return DataInitializer(db, fetcher, None, None), db


CODES = [f"{600000 + i}" for i in range(250)]

# ==================== T1: 时效探测能识别"主源旧、腾讯新" ====================
print("\n=== T1 时效探测 ===")
f = FakeFetcher(tf_end='2026-10-21', tc_end='2026-10-22')
di, _ = make_di(f)
target, force = di._probe_source_freshness(CODES, {}, 250)
check('T1 基准日取两者较新者', target == '2026-10-22', f'got={target}')
check('T1 判定需要强制降级', force is True, f'got={force}')
check('T1 回写 latest_available_date',
      getattr(f, 'latest_available_date', None) == '2026-10-22')

# 正常情况：两源一致
f2 = FakeFetcher(tf_end='2026-10-22', tc_end='2026-10-22')
di2, _ = make_di(f2)
target2, force2 = di2._probe_source_freshness(CODES, {}, 250)
check('T1 两源一致时不降级', (target2 == '2026-10-22' and force2 is False))

# ==================== T2: 主源返回旧数据 → 本轮自动走腾讯 ====================
print("\n=== T2 主源旧数据自动降级 ===")
f = FakeFetcher(tf_end='2026-10-21', tc_end='2026-10-22')
di, db = make_di(f)
di._init_kline_history_data(CODES, years=1)
check('T2 数据库最新日期为今天', db.max_date() == '2026-10-22', f'got={db.max_date()}')
check('T2 数据确实写入', db.rows() == 250 * 60, f'got={db.rows()}')
check('T2 全部走腾讯（不再逐批撞 TickFlow）', f.tf_calls == 1, f'tf_calls={f.tf_calls}')

# ==================== T3: 主源在探测后才变旧 → 逐批校验兜底 ====================
print("\n=== T3 逐批校验兜底 ===")
f = FakeFetcher(tf_end='2026-10-22', tc_end='2026-10-22')
di, db = make_di(f)
# 探测阶段返回今天，正式抓取时 TickFlow 变旧
orig = f._fetch_stock_batch_tickflow
state = {'n': 0}


def flaky(codes, days):
    state['n'] += 1
    if state['n'] == 1:          # 第一次是探测
        return orig(codes, days)
    return ({c: FakeFetcher._df('2026-10-21') for c in codes}, True)


f._fetch_stock_batch_tickflow = flaky
di._init_kline_history_data(CODES, years=1)
check('T3 逐批校验后仍有今天的数据', db.max_date() == '2026-10-22', f'got={db.max_date()}')

# ==================== T4: 两个源都旧 → 只能拿到旧数据（由 main 层拦截）====================
print("\n=== T4 全源滞后 ===")
f = FakeFetcher(tf_end='2026-10-21', tc_end='2026-10-21')
di, db = make_di(f)
di._init_kline_history_data(CODES, years=1)
check('T4 全源滞后时不冒充今天', db.max_date() == '2026-10-21', f'got={db.max_date()}')

# ==================== T5: 缺口规划（真增量）====================
print("\n=== T5 缺口规划 ===")
f = FakeFetcher(tf_end='2026-10-22', tc_end='2026-10-22')
di, db = make_di(f)
today = datetime.now().date()


def seed(code, days_ago):
    d = pd.Timestamp(today - pd.Timedelta(days=days_ago)).strftime('%Y-%m-%d')
    db.conn.execute("INSERT INTO stock_kline VALUES (?,?,?,?,?,?,?)",
                    (code, d, 1, 1, 1, 1, 1))
    db.conn.execute("INSERT INTO stock_basic VALUES (?,?,?)", (code, 'x', 1))


seed('600001', 1)
seed('600002', 20)
seed('600003', 60)
seed('600004', 300)
days_map, desc = di._build_gap_plan(['600001', '600002', '600003', '600004', '600005'])
check('T5 一天缺口→25根', days_map['600001'] == 25, f"got={days_map['600001']}")
check('T5 20天缺口→50根', days_map['600002'] == 50, f"got={days_map['600002']}")
check('T5 60天缺口→100根', days_map['600003'] == 100, f"got={days_map['600003']}")
check('T5 300天缺口→260根', days_map['600004'] == 260, f"got={days_map['600004']}")
check('T5 从没拉过→260根', days_map['600005'] == 260, f"got={days_map['600005']}")

# ==================== T6: 增量不丢老股（query_all 修复）====================
print("\n=== T6 增量模式不跳过老股 ===")
f = FakeFetcher(tf_end='2026-10-22', tc_end='2026-10-22')
di, db = make_di(f)
for c in CODES:
    db.conn.execute("INSERT INTO stock_basic VALUES (?,?,?)", (c, 'x', 1))
db.conn.commit()
existing = di._load_existing_codes()
check('T6 能读到已有股票', len(existing) == len(CODES), f'got={len(existing)}')
days_map, _ = di._build_gap_plan(CODES)
check('T6 老股仍有抓取计划', len(days_map) == len(CODES), f'got={len(days_map)}')

# ==================== T7: main.py 更新后复核 ====================
print("\n=== T7 main._smart_update 复核 ===")
src = open(os.path.join(BASE, 'fixed_files_v3', 'main.py'), encoding='utf-8').read()
tree = ast.parse(src)
cls = next(n for n in tree.body
           if isinstance(n, ast.ClassDef) and n.name == 'QuantSystem')
fns = [n for n in cls.body if isinstance(n, ast.FunctionDef)
       and n.name in ('_smart_update', '_source_latest_available_date')]
check('T7 找到两个方法', len(fns) == 2, f'got={[n.name for n in fns]}')

stub = types.ModuleType('utils.trading_time_validator')


class _V:
    def __init__(self, db):
        self.db = db

    def get_last_update_date(self):
        return self.db.max_date()


stub.TradingTimeValidator = _V
sys.modules['utils.trading_time_validator'] = stub

ns = {'dt_time': dt_time, '_now_cn': lambda: datetime(2026, 10, 22, 16, 30)}
exec(compile(ast.Module(body=fns, type_ignores=[]), '<main-fix>', 'exec'), ns)


class Dummy:
    pass


for k, v in ns.items():
    if isinstance(v, types.FunctionType):
        setattr(Dummy, k, v)


class StubFetcher:
    def __init__(self, db, land='2026-10-22', boom=False, available='2026-10-22'):
        self.db = db
        self.land = land
        self.boom = boom
        self.available = available

        class Inner:
            pass

        self.stock_data_fetcher = Inner()
        if available:
            self.stock_data_fetcher.latest_available_date = available

    def init_full_data(self, max_stocks=None, incremental=True):
        if self.land:
            self.db.conn.execute(
                "INSERT INTO stock_kline VALUES ('600000',?,1,1,1,1,1)", (self.land,))
            self.db.conn.commit()
        if self.boom:
            raise RuntimeError('boom')


def run_smart(db, fetcher):
    q = Dummy()
    q.db_manager = db
    q.fetcher = fetcher
    q.data_date = None
    q._smart_update(max_stocks=None)
    return q.data_date


# 场景A：更新没落地 → 必须中止
dbA = FakeDB()
dbA.conn.execute("INSERT INTO stock_kline VALUES ('600000','2026-10-21',1,1,1,1,1)")
dbA.conn.commit()
try:
    run_smart(dbA, StubFetcher(dbA, land=None))
    check('T7 未更新到今天就中止', False, '没有抛异常')
except RuntimeError as e:
    check('T7 未更新到今天就中止', '已中止' in str(e), f'{e}')

# 场景B：正常落地 → 通过，data_date 用真实日期
dbB = FakeDB()
dbB.conn.execute("INSERT INTO stock_kline VALUES ('600000','2026-10-21',1,1,1,1,1)")
dbB.conn.commit()
date_b = run_smart(dbB, StubFetcher(dbB))
check('T7 落地后放行', date_b == '2026-10-22', f'got={date_b}')

# 场景C：更新过程报错但数据已到位 → 放行
dbC = FakeDB()
dbC.conn.execute("INSERT INTO stock_kline VALUES ('600000','2026-10-21',1,1,1,1,1)")
dbC.conn.commit()
date_c = run_smart(dbC, StubFetcher(dbC, boom=True))
check('T7 报错但数据到位仍放行', date_c == '2026-10-22', f'got={date_c}')

# 场景D：数据已是最新 → 直接跳过网络更新
class NoRunFetcher:
    def init_full_data(self, **kw):
        raise AssertionError('不应触发网络更新')


dbD = FakeDB()
dbD.conn.execute("INSERT INTO stock_kline VALUES ('600000','2026-10-22',1,1,1,1,1)")
dbD.conn.commit()
date_d = run_smart(dbD, NoRunFetcher())
check('T7 已最新时跳过更新', date_d == '2026-10-22', f'got={date_d}')

# ==================== T8: init_full_data 增量模式端到端 ====================
print("\n=== T8 增量模式端到端 ===")


class FullFetcher(FakeFetcher):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.days_seen = []

    def get_all_stock_codes(self, max_retries=3):
        return {c: '股票' + c for c in CODES}

    def get_stock_market_cap(self):
        return {}

    def _fetch_stock_batch_tickflow(self, codes, days):
        self.days_seen.append(days)
        return super()._fetch_stock_batch_tickflow(codes, days)


def seed_kline(db, code, date):
    db.conn.execute("INSERT INTO stock_kline VALUES (?,?,?,?,?,?,?)",
                    (code, date, 1, 1, 1, 1, 1))
    db.conn.execute("INSERT INTO stock_basic VALUES (?,?,?)", (code, 'x', 1))
    db.conn.commit()


f = FullFetcher(tf_end='2026-10-22', tc_end='2026-10-22')
di, db = make_di(f)
# 200 只老股（库里已有），50 只新股（不在 stock_basic 里）
for c in CODES[:200]:
    seed_kline(db, c, '2026-10-21')

di.init_full_data(stock_codes=CODES, incremental=True, years=1)
all_today = db.conn.execute(
    "SELECT COUNT(DISTINCT code) FROM stock_kline WHERE date='2026-10-22'").fetchone()[0]
check('T8 全部股票都补到了今天', all_today == 250, f'got={all_today}')
check('T8 老股只按缺口抓 25 根', 25 in f.days_seen, f'days_seen={sorted(set(f.days_seen))}')
check('T8 新股按全量抓 250 根', 250 in f.days_seen, f'days_seen={sorted(set(f.days_seen))}')

print("\n" + "=" * 50)
print(f"通过 {len(PASS)} 项，失败 {len(FAIL)} 项")
if FAIL:
    for n in FAIL:
        print("  失败:", n)
    sys.exit(1)
print("全部通过")
