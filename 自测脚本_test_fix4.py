# -*- coding: utf-8 -*-
"""v4 自测：覆盖率口径 + v3 三层修复回归（离线仿真，不联网）"""
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
sys.path.insert(0, os.path.join(BASE, 'test4'))

logging.basicConfig(level=logging.INFO, format='%(levelname)-7s %(message)s')

from utils.data_initializer import DataInitializer, StaleDataSourceError  # noqa: E402

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
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def execute_with_retry(self, sql, params=(), max_retries=3):
        return self.conn.execute(sql, params)

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

    def seed(self, codes, date):
        self.conn.executemany(
            "INSERT OR REPLACE INTO stock_kline VALUES (?,?,?,?,?,?,?)",
            [(c, date, 1, 1, 1, 1, 1) for c in codes])
        self.conn.commit()


class FakeFetcher:
    """tf_end/tc_end 控制两个数据源能看到的最新K线日期"""

    def __init__(self, tf_end, tc_end, tf_ok=True):
        self.tf_end, self.tc_end, self.tf_ok = tf_end, tc_end, tf_ok
        self.tf_calls = self.tc_calls = 0
        self.tc_years = []

    @staticmethod
    def _df(end, bars=60):
        dates = pd.bdate_range(end=end, periods=bars).strftime('%Y-%m-%d').tolist()
        return pd.DataFrame({
            'date': dates, 'open': [10.0] * bars, 'high': [10.5] * bars,
            'low': [9.5] * bars, 'close': [10.2] * bars, 'volume': [100000] * bars})

    def _fetch_stock_batch_tickflow(self, codes, days):
        self.tf_calls += 1
        if not self.tf_ok:
            return ({}, False)
        return ({c: self._df(self.tf_end) for c in codes}, True)

    def _fetch_stock_batch_tencent(self, codes, years=3, concurrency=2):
        self.tc_calls += 1
        self.tc_years.append(years)
        return {c: self._df(self.tc_end) for c in codes}


def make_di(fetcher, db=None):
    db = db or FakeDB()
    return DataInitializer(db, fetcher, None, None), db


CODES = [f"{600000 + i}" for i in range(250)]

# ==================== 回归：时效探测 ====================
print("\n=== R1 时效探测（v3 回归）===")
f = FakeFetcher(tf_end='2026-10-21', tc_end='2026-10-22')
di, _ = make_di(f)
target, force = di._probe_source_freshness(CODES, {}, 250)
check('R1 基准日取较新者 + 强制降级', (target, force) == ('2026-10-22', True), f'got={(target, force)}')

# ==================== 回归：主源旧数据 → 整轮走腾讯 ====================
print("\n=== R2 主源旧数据自动降级（v3 回归）===")
f = FakeFetcher(tf_end='2026-10-21', tc_end='2026-10-22')
di, db = make_di(f)
di._init_kline_history_data(CODES, years=1)
check('R2 库里到 10-22', db.max_date() == '2026-10-22', f'got={db.max_date()}')
check('R2 只探测一次 TickFlow', f.tf_calls == 1, f'tf_calls={f.tf_calls}')

# ==================== v4 新增：降级时用 1 年而不是 3 年 ====================
print("\n=== N1 降级抓取按缺口缩到 1 年 ===")
f = FakeFetcher(tf_end='2026-10-21', tc_end='2026-10-22')
di, db = make_di(f)
for c in CODES:
    db.seed([c], '2026-10-21')
days_map, _ = di._build_gap_plan(CODES)
di._init_kline_history_data(CODES, years=3, days_map=days_map)
check('N1 腾讯只按 1 年拉', set(f.tc_years) == {1}, f'years={set(f.tc_years)}')
check('N1 数据补齐到 10-22', db.max_date() == '2026-10-22')

print("\n=== N2 全量模式下不缩年（仍按 years）===")
f = FakeFetcher(tf_end='2026-10-21', tc_end='2026-10-22')  # 主源旧 → 强制走腾讯
di, db = make_di(f)
di._init_kline_history_data(CODES[:40], years=3, days_map=None)
check('N2 无缺口计划时按 3 年拉', 3 in set(f.tc_years), f'years={sorted(set(f.tc_years))}')
check('N2 数据补齐到 10-22', db.max_date() == '2026-10-22')

# ==================== v4 核心：main._smart_update 覆盖率口径 ====================
print("\n=== M 覆盖率口径 ===")
src = open(os.path.join(BASE, 'fixed_files_v4', 'main.py'), encoding='utf-8').read()
tree = ast.parse(src)
cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'QuantSystem')
names = ('_smart_update', '_data_coverage', '_source_latest_available_date')
fns = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in names]
check('M0 找到三个方法', len(fns) == 3, f'got={[n.name for n in fns]}')

stub = types.ModuleType('utils.trading_time_validator')


class _V:
    def __init__(self, db):
        self.db = db

    def get_last_update_date(self):
        return self.db.max_date()


stub.TradingTimeValidator = _V
sys.modules['utils.trading_time_validator'] = stub

ns = {'dt_time': dt_time, '_now_cn': lambda: datetime(2026, 10, 22, 16, 30)}
exec(compile(ast.Module(body=fns, type_ignores=[]), '<main-v4>', 'exec'), ns)


class Dummy:
    pass


for k, v in ns.items():
    if isinstance(v, types.FunctionType):
        setattr(Dummy, k, v)


class StubFetcher:
    """init_full_data 时把 target 日写进前 n_fill 只股票"""

    def __init__(self, db, n_fill=0, total=0, codes=None, available='2026-10-22', boom=False):
        self.db, self.n_fill, self.total, self.boom = db, n_fill, total, boom
        self.codes = codes or []
        self.called = False
        self.available = available

        class Inner:
            pass

        self.stock_data_fetcher = Inner()
        if available:
            self.stock_data_fetcher.latest_available_date = available

    def init_full_data(self, max_stocks=None, incremental=True):
        self.called = True
        if self.n_fill:
            self.db.seed(self.codes[:self.n_fill], '2026-10-22')
        if self.boom:
            raise RuntimeError('boom')


def run_smart(db, fetcher):
    q = Dummy()
    q.db_manager = db
    q.fetcher = fetcher
    q.data_date = None
    q._smart_update(max_stocks=None)
    return q.data_date


TGT = '2026-10-22'
BASE_CODES = [f"{600000 + i}" for i in range(1000)]

# 场景A：MAX(date)=今天，但只有 46/1000 只有今天的K线（10-22 的真实情形）
db = FakeDB()
db.seed(BASE_CODES, '2026-10-21')
db.seed(BASE_CODES[:46], TGT)
check('A0 造出假信号：MAX=今天', db.max_date() == TGT)
f = StubFetcher(db, n_fill=0, codes=BASE_CODES)
try:
    run_smart(db, f)
    check('A1 假信号下不再跳过更新', False, '竟然直接通过了')
except RuntimeError:
    check('A1 假信号下不再跳过更新', f.called, f'called={f.called}')
    check('A2 覆盖率不足时中止', True)

# 场景B：假信号 + 更新真的补齐 → 放行
db = FakeDB()
db.seed(BASE_CODES, '2026-10-21')
db.seed(BASE_CODES[:46], TGT)
f = StubFetcher(db, n_fill=len(BASE_CODES), codes=BASE_CODES)
date_b = run_smart(db, f)
check('B 补齐后放行', f.called and date_b == TGT, f'date={date_b}')

# 场景C：覆盖率 100% → 跳过网络更新
db = FakeDB()
db.seed(BASE_CODES, TGT)


class NoRun:
    def init_full_data(self, **kw):
        raise AssertionError('不应触发网络更新')


date_c = run_smart(db, NoRun())
check('C 覆盖率足够时跳过更新', date_c == TGT, f'date={date_c}')

# 场景D：覆盖率 50% 且更新后仍 50% → 中止
db = FakeDB()
db.seed(BASE_CODES, '2026-10-21')
db.seed(BASE_CODES[:500], TGT)
f = StubFetcher(db, n_fill=0, codes=BASE_CODES)
try:
    run_smart(db, f)
    check('D 半覆盖仍中止', False, '竟然通过了')
except RuntimeError as e:
    check('D 半覆盖仍中止', '覆盖不足' in str(e), f'{e}')

# 场景E：数据库为空 → 更新后仍空 → 中止（不除零）
db = FakeDB()
f = StubFetcher(db, n_fill=0, codes=[])
try:
    run_smart(db, f)
    check('E 空库不除零', False, '竟然通过了')
except RuntimeError:
    check('E 空库不除零', True)

# 场景F：更新报错但覆盖到位 → 放行
db = FakeDB()
db.seed(BASE_CODES, '2026-10-21')
f = StubFetcher(db, n_fill=len(BASE_CODES), codes=BASE_CODES, boom=True)
date_f = run_smart(db, f)
check('F 报错但覆盖到位仍放行', date_f == TGT, f'date={date_f}')

# ==================== 回归：增量端到端 ====================
print("\n=== R3 增量模式端到端（v3 回归）===")


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


f = FullFetcher(tf_end='2026-10-22', tc_end='2026-10-22')
di, db = make_di(f)
for c in CODES[:200]:
    db.seed([c], '2026-10-21')
    db.conn.execute("INSERT INTO stock_basic VALUES (?,?,?)", (c, 'x', 1))
db.conn.commit()
di.init_full_data(stock_codes=CODES, incremental=True, years=1)
cnt = db.conn.execute(
    "SELECT COUNT(DISTINCT code) FROM stock_kline WHERE date='2026-10-22'").fetchone()[0]
check('R3 全部补到今天', cnt == 250, f'got={cnt}')
check('R3 老股只抓 25 根 / 新股 250 根', sorted(set(f.days_seen)) == [25, 250],
      f'days_seen={sorted(set(f.days_seen))}')

print("\n" + "=" * 50)
print(f"通过 {len(PASS)} 项，失败 {len(FAIL)} 项")
if FAIL:
    for n in FAIL:
        print("  失败:", n)
    sys.exit(1)
print("全部通过")
