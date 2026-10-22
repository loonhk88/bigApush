"""
数据初始化器 - 初始化各类数据（基础数据、K线、资金流向等）
"""
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta

# 配置日志
logger = logging.getLogger(__name__)


# ==================== 数据时效校验（防止用旧数据选股）====================

class StaleDataSourceError(RuntimeError):
    """数据源返回旧数据（HTTP 正常但最新K线日期落后）

    出现该异常时必须中止本次运行：宁可今天不出选股结果，
    也不能拿昨天的行情当今天的结果写进表格/飞书。
    """


# 单批次内"看不到基准日"的股票占比超过该阈值，即判定为数据源时效问题
# （正常停牌股占比远低于此值，10-22 事故时是 100%）
STALE_RATIO_THRESHOLD = 0.5
# 时效探测取样股票数（同时向 TickFlow 与腾讯财经索取，比对最新K线日期）
PROBE_SAMPLE_SIZE = 30


def _norm_date(value):
    """把各种日期表示统一成 YYYY-MM-DD 字符串"""
    if value is None:
        return None
    if hasattr(value, 'strftime'):
        try:
            return value.strftime('%Y-%m-%d')
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == '-' and text[7] == '-':
        return text[:10]
    digits = text.replace('-', '').replace('/', '').replace(' ', '')[:8]
    if len(digits) == 8 and digits.isdigit():
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return text[:10]


def _df_max_date(df):
    """取 DataFrame 中最新一根K线的日期（YYYY-MM-DD），取不到返回 None"""
    if df is None or len(df) == 0:
        return None
    try:
        dates = [_norm_date(v) for v in df['date'].tolist()]
    except Exception:
        return None
    dates = [d for d in dates if d]
    return max(dates) if dates else None


class DataInitializer:
    """数据初始化器"""
    
    def __init__(self, db_manager, stock_data_fetcher, kline_fetcher, fund_flow_fetcher,
                 progress_callback=None):
        """
        初始化数据初始化器
        
        参数：
            db_manager: 数据库管理器
            stock_data_fetcher: 股票数据采集器
            kline_fetcher: K线数据处理器
            fund_flow_fetcher: 资金流向数据采集器
            progress_callback: 进度回调函数 callback(progress_pct: int, message: str)
        """
        self.db_manager = db_manager
        self.stock_data_fetcher = stock_data_fetcher
        self.kline_fetcher = kline_fetcher
        self.fund_flow_fetcher = fund_flow_fetcher
        self.progress_callback = progress_callback
    
    def _report_progress(self, progress_pct: int, message: str = ""):
        """上报进度（通过回调函数）"""
        if self.progress_callback:
            try:
                self.progress_callback(progress_pct, message)
            except Exception:
                pass
    
    # ==================== 基础数据初始化 ====================
    
    def _init_basic_data(self, stock_codes: list, stock_dict: dict = None) -> None:
        """
        初始化基础数据（包括股票基本信息和市值）
        
        参数：
            stock_codes: 股票代码列表
            stock_dict: 股票代码到名称的映射字典
        """
        logger.info("开始初始化基础数据...")
        success_count = 0
        failed_count = 0
        
        try:
            # 步骤1：获取股票名称（批量）
            logger.info("获取股票基本信息...")
            if stock_dict:
                all_stocks = stock_dict
                logger.info(f"使用传入的股票基本信息: {len(all_stocks)} 只股票")
            else:
                all_stocks = self.stock_data_fetcher.get_all_stock_codes()
                
                if not all_stocks:
                    logger.error("获取股票基本信息失败")
                    return
                
                logger.info(f"成功获取 {len(all_stocks)} 只股票的基本信息")
            
            # 步骤2：获取股票市值（批量）
            logger.info("从 Tushare 获取股票市值信息...")
            market_caps = self.stock_data_fetcher.get_stock_market_cap()
            
            if market_caps:
                logger.info(f"成功获取 {len(market_caps)} 只股票的市值信息")
            else:
                logger.warning("未获取到市值信息，将使用默认值 0")
            
            # 步骤3：批量保存到数据库
            with self.db_manager.transaction():
                logger.info("保存股票基本信息和市值到数据库...")
                
                for idx, code in enumerate(stock_codes, 1):
                    try:
                        # 从批量获取的数据中查找基本信息
                        name = all_stocks.get(code, '')
                        # 获取市值信息，如果没有则使用 0
                        market_cap = market_caps.get(code, 0)
                        
                        if name:
                            # 保存基本信息和市值到 stock_basic 表
                            insert_sql = """
                            INSERT OR REPLACE INTO stock_basic 
                            (code, name, market_cap)
                            VALUES (?, ?, ?)
                            """
                            self.db_manager.execute_with_retry(insert_sql, (code, name, market_cap))
                            success_count += 1
                        else:
                            failed_count += 1
                            logger.debug(f"股票 {code} 不在批量获取的数据中")
                        
                        # 定期输出进度（每100只股票输出一次）
                        if idx % 100 == 0:
                            logger.info(f"基础数据采集进度: {idx}/{len(stock_codes)}, 成功: {success_count}")
                    
                    except Exception as e:
                        failed_count += 1
                        logger.warning(f"处理 {code} 基础数据失败: {e}")
                
                logger.info(f"基础数据保存完成: 成功 {success_count} 只, 失败 {failed_count} 只")
        
        except Exception as e:
            logger.error(f"初始化基础数据失败: {e}")

    # ==================== 数据时效探测与补齐 ====================

    def _probe_source_freshness(self, stock_codes: list, days_map: dict, default_days: int) -> tuple:
        """探测各数据源当前能看到的最新K线日期（整轮只做一次）

        取少量样本，同时向 TickFlow 与腾讯财经索取，比较最新K线日期：
        - 腾讯财经比 TickFlow 新 → 主数据源在返回旧数据，本轮直接降级到腾讯财经
        - 两者一样新             → 正常，记下该日期作为逐批校验的时效基准

        这样做的好处：不依赖交易日历，节假日/停牌日不会误判，只认"能不能看到同一天的K线"。

        返回：
            (target_date, force_fallback)
            - target_date: 所有数据源能看到的最新K线日期（YYYY-MM-DD），探测失败为 None
            - force_fallback: True 表示主数据源返回旧数据，本轮应直接走腾讯财经
        """
        sample = list(stock_codes[:PROBE_SAMPLE_SIZE])
        if not sample:
            return (None, False)
        sample_days = max([days_map.get(c, default_days) for c in sample] + [default_days])

        tf_max = None
        tc_max = None
        try:
            tf_dict, tf_ok = self.stock_data_fetcher._fetch_stock_batch_tickflow(
                sample, days=sample_days)
            if tf_ok and tf_dict:
                tf_max = max([d for d in (_df_max_date(v) for v in tf_dict.values()) if d] or [None])
        except Exception as e:
            logger.warning(f"时效探测: TickFlow 取样失败: {e}")

        try:
            tc_dict = self.stock_data_fetcher._fetch_stock_batch_tencent(
                sample, years=1, concurrency=2)
            if tc_dict:
                tc_max = max([d for d in (_df_max_date(v) for v in tc_dict.values()) if d] or [None])
        except Exception as e:
            logger.warning(f"时效探测: 腾讯财经取样失败: {e}")

        target = max([d for d in (tf_max, tc_max) if d], default=None)
        if not target:
            logger.warning("时效探测: 两个数据源取样均无数据，跳过逐批时效校验")
            return (None, False)

        # 回写"当前数据源能看到的最新日期"，供上层复核区分
        # "数据源还没发布今天的数据" 与 "更新失败"（见 main.py::_smart_update）
        try:
            self.stock_data_fetcher.latest_available_date = target
        except Exception:
            pass

        logger.info(
            f"数据源时效探测({len(sample)}只样本): TickFlow 最新={tf_max or '无'} | "
            f"腾讯财经 最新={tc_max or '无'} | 基准={target}"
        )

        if tf_max and tc_max and tf_max < tc_max:
            logger.warning("=" * 60)
            logger.warning(f"!!! 主数据源 TickFlow 返回旧数据（{tf_max} < 腾讯财经 {tc_max}）!!!")
            logger.warning("!!! 本轮直接降级到腾讯财经，避免用旧数据出选股结果 !!!")
            logger.warning("=" * 60)
            return (target, True)

        return (target, False)

    def _repair_batch_by_tencent(self, kline_dict: dict, stale_codes: list,
                                 target_date: str, batch_num: int, years: int) -> int:
        """用腾讯财经重取"看不到基准日"的股票，替换 kline_dict 中对应项

        返回：
            成功补齐（最新日期 >= 基准日）的股票数；为 0 说明腾讯财经也旧，调用方应中止
        """
        if not stale_codes:
            return 0
        try:
            tc_dict = self.stock_data_fetcher._fetch_stock_batch_tencent(
                stale_codes, years=years, concurrency=2) or {}
        except Exception as e:
            logger.warning(f"批次{batch_num}: 腾讯财经补齐请求失败: {e}")
            return 0

        fixed_n = 0
        for code, df in tc_dict.items():
            max_d = _df_max_date(df)
            if max_d and max_d >= target_date:
                kline_dict[code] = df
                fixed_n += 1
        logger.info(f"批次{batch_num}: 腾讯财经补齐 {fixed_n}/{len(stale_codes)} 只")
        return fixed_n

    # ==================== K线数据初始化 ====================
    
    def _init_kline_history_data(self, stock_codes: list, years: int = 3,
                                  progress_range: tuple = (0, 100),
                                  days_map: dict = None) -> None:
        """
        初始化K线历史数据（TickFlow 优先，智能降级 + 自动恢复 + 时效校验）

        策略：
        0. 先用少量样本比对 TickFlow 与腾讯财经的最新K线日期，主数据源返回旧数据则整轮直接降级
        1. 优先使用 TickFlow 批量 API，批次间延迟 3 秒防止限流
        2. TickFlow 失败时等待 30 秒后重试 1 次，再失败才降级
        3. 连续 3 次 TickFlow 失败才永久降级；否则尝试恢复
        4. 降级期间每 5 批自动探测 TickFlow 是否恢复
        5. 腾讯财经降级使用 2 线程低并发，避免反爬
        6. 每批写入前校验最新K线日期，落后于基准日即判为数据源问题，主动改用腾讯财经重取

        参数：
            stock_codes: 股票代码列表
            years: 获取数据的年份数（默认 3 年）
            progress_range: 进度映射区间 (start, end)，默认 (0, 100)
            days_map: {股票代码: 需要抓取的K线根数}，用于增量模式只补缺口；
                      为 None 时统一按 years*250 抓取
        """
        import time as time_module
        # batch_size: 每批处理的股票数，与日常更新对齐
        batch_size = 100
        # days: 将年份转换为交易日数，与 TickFlow batch API 参数对齐
        default_days = years * 250
        # 增量模式下每只股票各自的抓取根数
        days_map = days_map or {}
        # 降级到腾讯财经时用多少年：增量只补缺口（最多 260 根）时按 1 年（365 根）抓即可，
        # 单请求体积小一半以上，也能压住"全天走腾讯财经"时的总耗时
        fallback_years = years
        if days_map and max(days_map.values()) <= 300:
            fallback_years = 1
        total = len(stock_codes)
        progress_start, progress_end = progress_range
        success_count = 0
        failed_count = 0
        total_inserted = 0
        # 记录未取到K线的股票，主流程结束后统一重试一次，便于定位与补救
        failed_codes = []
        # fallback_mode: 是否已降级到腾讯财经
        fallback_mode = False
        # 连续 TickFlow 失败计数（用于判断是否永久降级）
        consecutive_tf_fails = 0
        # 连续 3 次失败才永久降级
        TF_PERMANENT_THRESHOLD = 3
        # TickFlow 失败后重试前的等待时间（秒）
        TF_RETRY_DELAY = 30
        # 降级后每隔多少批次探测一次 TickFlow 恢复
        TF_PROBE_INTERVAL = 5
        # 降级模式下计数器
        fallback_batch_count = 0
        # 腾讯财经连续空批次计数（用于动态退避）
        consecutive_empty_tencent = 0
        # 一旦确认数据源在返回旧数据，本轮锁死腾讯财经，不再来回切换
        tencent_locked = False
        # 时效校验命中次数（仅用于收尾日志）
        stale_batches = 0

        if total == 0:
            logger.debug("没有需要处理的股票，跳过K线初始化")
            return

        logger.info("=" * 60)
        logger.info(f"K线初始化开始 | 股票: {total} 只 | 年份: {years}年 | 批次大小: {batch_size}")
        logger.info(f"数据源策略: TickFlow 优先(批间 3s 延迟) → TickFlow 失败重试(30s) → 腾讯财经降级(2线程)")
        logger.info(f"永久降级阈值: 连续 {TF_PERMANENT_THRESHOLD} 次 TickFlow 失败")
        logger.info("=" * 60)

        # ============ 步骤0: 数据源时效探测（整轮只做一次，约 10-20 秒）============
        # TickFlow 在"当天行情已发布"时仍可能只返回昨天为止的数据（HTTP 200），
        # 光看 api_ok 是判断不出来的，必须比最新K线日期。
        target_date, force_fallback = self._probe_source_freshness(
            stock_codes, days_map, default_days)
        if force_fallback:
            fallback_mode = True
            tencent_locked = True
        if target_date:
            logger.info(f"时效基准日: {target_date}（逐批校验用）")
        else:
            logger.warning("时效基准日未知，跳过逐批时效校验（将依赖上层复核）")

        try:
            # 分批处理
            for batch_idx in range(0, total, batch_size):
                # 当前批次的股票代码列表
                batch_codes = stock_codes[batch_idx:batch_idx + batch_size]
                batch_num = batch_idx // batch_size + 1
                # 本批需要抓取的K线根数（增量模式下按缺口算，取批内最大值保证每只都够）
                days = max(days_map.get(c, default_days) for c in batch_codes) \
                    if days_map else default_days

                # ============ 步骤1: 选择数据源获取 K 线 ============
                if not fallback_mode:
                    # ---------- TickFlow 优先模式 ----------
                    kline_dict, api_ok = self.stock_data_fetcher._fetch_stock_batch_tickflow(
                        batch_codes, days=days
                    )
                    tickflow_hit = len(kline_dict)
                    if api_ok:
                        # TickFlow 成功，重置失败计数
                        consecutive_tf_fails = 0
                        logger.debug(f"批次{batch_num}: TickFlow 命中 {tickflow_hit}/{len(batch_codes)} 只")
                    else:
                        # TickFlow 失败 → 等待后重试1次
                        consecutive_tf_fails += 1
                        logger.warning(
                            f"批次{batch_num}: TickFlow 失败(连续第{consecutive_tf_fails}次)，"
                            f"等待 {TF_RETRY_DELAY}s 后重试…"
                        )
                        time_module.sleep(TF_RETRY_DELAY)
                        # 重试 TickFlow
                        kline_dict, api_ok = self.stock_data_fetcher._fetch_stock_batch_tickflow(
                            batch_codes, days=days
                        )
                        if api_ok:
                            # 重试成功
                            consecutive_tf_fails = 0
                            tickflow_hit = len(kline_dict)
                            logger.info(
                                f"批次{batch_num}: TickFlow 重试成功，命中 {tickflow_hit}/{len(batch_codes)} 只"
                            )
                        elif consecutive_tf_fails >= TF_PERMANENT_THRESHOLD:
                            # 连续失败达到阈值 → 永久降级
                            remaining = total - batch_idx
                            logger.warning("=" * 50)
                            logger.warning(
                                f"!!! TickFlow 连续失败 {consecutive_tf_fails} 次(批次{batch_num})，永久切换到腾讯财经 !!!"
                            )
                            logger.warning(
                                f"数据源切换: TickFlow → 腾讯财经(批量并发,2线程)"
                            )
                            logger.warning(
                                f"影响范围: 剩余 {remaining} 只 | 已成功: {success_count} 只"
                            )
                            logger.warning("=" * 50)
                            fallback_mode = True
                            # 用腾讯财经获取当前批次
                            kline_dict = self.stock_data_fetcher._fetch_stock_batch_tencent(
                                batch_codes, years=fallback_years, concurrency=2
                            )
                            tencent_hit = len(kline_dict)
                            logger.info(
                                f"批次{batch_num} 降级获取: 腾讯财经 {tencent_hit}/{len(batch_codes)} 只"
                            )
                        else:
                            # 连续失败次数未达阈值，临时用腾讯财经处理本批，下次继续尝试TickFlow
                            logger.warning(
                                f"批次{batch_num}: TickFlow 重试仍失败(连续{consecutive_tf_fails}/{TF_PERMANENT_THRESHOLD})，"
                                f"本批临时降级到腾讯财经，下批恢复 TickFlow"
                            )
                            kline_dict = self.stock_data_fetcher._fetch_stock_batch_tencent(
                                batch_codes, years=fallback_years, concurrency=2
                            )
                            tencent_hit = len(kline_dict)
                            logger.info(
                                f"批次{batch_num} 临时降级: 腾讯财经 {tencent_hit}/{len(batch_codes)} 只"
                            )
                else:
                    # ---------- 降级模式 ----------
                    # 定期探测 TickFlow 是否已恢复（因"返回旧数据"而锁死的本轮不再探测）
                    fallback_batch_count += 1
                    if (not tencent_locked) and fallback_batch_count % TF_PROBE_INTERVAL == 0:
                        logger.info(f"批次{batch_num}: 探测 TickFlow 恢复状态(降级后第{fallback_batch_count}批)…")
                        probe_dict, probe_ok = self.stock_data_fetcher._fetch_stock_batch_tickflow(
                            batch_codes[:20], days=days  # 只探测 20 只，降低探测成本
                        )
                        if probe_ok and len(probe_dict) > 0:
                            logger.info(
                                f"批次{batch_num}: TickFlow 已恢复！切换回 TickFlow 模式"
                            )
                            fallback_mode = False
                            consecutive_tf_fails = 0
                            fallback_batch_count = 0
                            # 用 TickFlow 获取完整批次
                            kline_dict, api_ok = self.stock_data_fetcher._fetch_stock_batch_tickflow(
                                batch_codes, days=days
                            )
                            if not api_ok:
                                # 探测成功但完整批次失败，回到降级
                                logger.warning("批次{batch_num}: TickFlow 恢复探测成功但完整批次失败，维持降级")
                                fallback_mode = True
                                kline_dict = self.stock_data_fetcher._fetch_stock_batch_tencent(
                                    batch_codes, years=fallback_years, concurrency=2
                                )
                                tencent_hit = len(kline_dict)
                        else:
                            logger.debug(f"批次{batch_num}: TickFlow 仍未恢复，继续使用腾讯财经")
                            kline_dict = self.stock_data_fetcher._fetch_stock_batch_tencent(
                                batch_codes, years=fallback_years, concurrency=2
                            )
                            tencent_hit = len(kline_dict)
                    else:
                        # 直接使用腾讯财经
                        kline_dict = self.stock_data_fetcher._fetch_stock_batch_tencent(
                            batch_codes, years=fallback_years, concurrency=2
                        )
                        tencent_hit = len(kline_dict)
                        logger.info(f"批次{batch_num}: 腾讯财经(降级模式) 获取 {tencent_hit}/{len(batch_codes)} 只")

                # ============ 步骤1.5: 逐批时效校验（防止数据源"成功但返回旧数据"）============
                # 10-22 事故根因：TickFlow 全天返回截至 10-21 的数据，api_ok 一直是 True，
                # 于是拿 10-21 的行情算出了 10-22 的选股结果。这里必须比日期。
                if target_date and kline_dict:
                    batch_stale = [c for c in kline_dict if _df_max_date(kline_dict[c]) is None
                                   or _df_max_date(kline_dict[c]) < target_date]
                    if batch_stale:
                        stale_n = len(batch_stale)
                        stale_ratio = stale_n / len(kline_dict)
                        logger.info(
                            f"批次{batch_num} 时效校验: 最新 {len(kline_dict) - stale_n} 只 / "
                            f"落后(<{target_date}) {stale_n} 只 ({stale_ratio:.0%})"
                        )
                        if stale_ratio >= STALE_RATIO_THRESHOLD:
                            stale_batches += 1
                            logger.warning("=" * 50)
                            logger.warning(
                                f"!!! 批次{batch_num}: {stale_n}/{len(kline_dict)} 只看不到 {target_date}，"
                                f"判定为数据源时效问题，改用腾讯财经重新获取 !!!"
                            )
                            logger.warning("=" * 50)
                            fixed_n = self._repair_batch_by_tencent(
                                kline_dict, batch_stale, target_date, batch_num, fallback_years)
                            if fixed_n == 0:
                                raise StaleDataSourceError(
                                    f"数据源全线滞后：TickFlow 与腾讯财经都拿不到 {target_date} 的K线"
                                    f"（批次{batch_num}，{stale_n} 只样本全部落后），"
                                    f"已中止本次运行，避免用旧数据选股"
                                )
                            # 本轮剩余批次直接走腾讯财经，不再逐批先撞一次 TickFlow
                            if not fallback_mode:
                                logger.warning(
                                    f"批次{batch_num}: 本轮剩余批次全部改用腾讯财经（已锁定，不再探测 TickFlow 恢复）"
                                )
                                fallback_mode = True
                                tencent_locked = True

                # ============ 步骤2: 批量写入数据库（executemany 高效模式）============
                batch_inserted = 0
                with self.db_manager.transaction():
                    # 在事务中获取连接，与 kline_updater 保持一致
                    conn = self.db_manager.connect()
                    cursor = conn.cursor()
                    insert_sql = """
                    INSERT OR REPLACE INTO stock_kline
                    (code, date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                    for code, df_kline in kline_dict.items():
                        try:
                            if df_kline is None or len(df_kline) == 0:
                                continue

                            # 准备数据：向量化处理比 iterrows 快 10-100 倍
                            # 统一日期格式为 YYYY-MM-DD
                            from utils.date_utils import normalize_date
                            dates = df_kline['date'].apply(lambda x: normalize_date(x) if x is not None else None)
                            volumes = df_kline['volume'].fillna(0).astype(int)
                            opens = df_kline['open'].astype(float)
                            highs = df_kline['high'].astype(float)
                            lows = df_kline['low'].astype(float)
                            closes = df_kline['close'].astype(float)

                            # 构建记录列表
                            records = list(zip(
                                [code] * len(df_kline), dates, opens, highs, lows, closes, volumes
                            ))

                            # executemany 批量插入
                            cursor.executemany(insert_sql, records)
                            batch_inserted += len(records)
                            success_count += 1

                        except Exception as e:
                            failed_count += 1
                            logger.debug(f"批量保存 {code} K线失败: {e}")

                # ============ 步骤3: 统计与进度 ============
                # 统计批次中本批无数据的股票数（不在 kline_dict 即为最终失败）
                batch_failed_codes = [c for c in batch_codes if c not in kline_dict]
                batch_failed_count = len(batch_failed_codes)
                failed_count += batch_failed_count
                failed_codes.extend(batch_failed_codes)

                total_inserted += batch_inserted

                # 每批次输出进度日志
                completed = min(batch_idx + len(batch_codes), total)
                progress_pct = completed / total * 100
                if fallback_mode:
                    mode_label = "[腾讯财经降级]"
                elif consecutive_tf_fails > 0:
                    mode_label = f"[TickFlow(重试{consecutive_tf_fails}次)]"
                else:
                    mode_label = "[TickFlow]"
                logger.info(
                    f"K线初始化进度 {mode_label}: {completed}/{total} "
                    f"({progress_pct:.1f}%) | 成功: {success_count} | "
                    f"本批插入: {batch_inserted} 条 | 累计插入: {total_inserted} 条"
                )
                # 映射到全局进度区间并上报
                global_pct = int(progress_start + progress_pct / 100.0 * (progress_end - progress_start))
                self._report_progress(global_pct,
                    f"K线数据: {completed}/{total} ({progress_pct:.0f}%)")

                # ============ 批次间延迟（防限流核心策略）============
                if not fallback_mode:
                    # TickFlow 模式：3 秒延迟，防止免费 API 限流
                    time_module.sleep(3)
                else:
                    # 降级模式：基础 2 秒延迟
                    time_module.sleep(2)
                    # 腾讯财经空批次动态退避（全部股票无数据即为空批次）
                    if batch_failed_count == len(batch_codes):
                        consecutive_empty_tencent += 1
                        backoff = min(consecutive_empty_tencent * 3, 30)
                        logger.warning(
                            f"腾讯财经连续 {consecutive_empty_tencent} 批全空，退避 {backoff}s"
                        )
                        time_module.sleep(backoff)
                    else:
                        consecutive_empty_tencent = 0

            # ============ 步骤4: 对缺失K线的股票重试一次 ============
            # 主流程里 TickFlow 偶发漏单很常见，换腾讯财经再补一次，能救回大部分
            # 整个重试阶段独立 try：即使这里出错也不影响已经写入的数据和后续选股
            if failed_codes:
                logger.warning(f"本次有 {len(failed_codes)} 只股票未取得K线，开始重试（腾讯财经，2线程）...")
                recovered_codes = set()
                try:
                    retry_dict = self.stock_data_fetcher._fetch_stock_batch_tencent(
                        failed_codes, years=fallback_years, concurrency=2
                    ) or {}

                    if retry_dict:
                        from utils.date_utils import normalize_date
                        retry_sql = """
                        INSERT OR REPLACE INTO stock_kline
                        (code, date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                        with self.db_manager.transaction():
                            conn = self.db_manager.connect()
                            cursor = conn.cursor()
                            for code, df_kline in retry_dict.items():
                                try:
                                    if df_kline is None or len(df_kline) == 0:
                                        continue
                                    records = list(zip(
                                        [code] * len(df_kline),
                                        df_kline['date'].apply(
                                            lambda x: normalize_date(x) if x is not None else None),
                                        df_kline['open'].astype(float),
                                        df_kline['high'].astype(float),
                                        df_kline['low'].astype(float),
                                        df_kline['close'].astype(float),
                                        df_kline['volume'].fillna(0).astype(int),
                                    ))
                                    cursor.executemany(retry_sql, records)
                                    total_inserted += len(records)
                                    success_count += 1
                                    recovered_codes.add(code)
                                except Exception as e:
                                    logger.debug(f"重试保存 {code} K线失败: {e}")
                except Exception as e:
                    logger.warning(f"失败股票重试阶段出错，跳过重试（不影响主流程）: {e}")

                still_failed = [c for c in failed_codes if c not in recovered_codes]
                logger.info(f"重试结果: 补回 {len(recovered_codes)} 只，仍缺失 {len(still_failed)} 只")
                if still_failed:
                    logger.warning(
                        f"以下 {len(still_failed)} 只股票所有数据源均无K线（多为停牌/退市/新股上市不足），本次跳过: "
                        f"{still_failed[:50]}{' …' if len(still_failed) > 50 else ''}"
                    )

            # 计算最终失败数
            final_failed = total - success_count
            logger.info(
                f"K线历史数据初始化完成: 成功 {success_count} 只, "
                f"失败 {final_failed} 只, 总记录 {total_inserted} 条"
            )
            if stale_batches:
                logger.warning(
                    f"本次有 {stale_batches} 个批次命中数据源时效问题，已改用腾讯财经补齐"
                )

            # 失败数超过阈值时抛出异常，提示重新初始化
            FAILURE_THRESHOLD = 1000
            if final_failed > FAILURE_THRESHOLD:
                raise RuntimeError(
                    f"K线初始化失败数({final_failed}只)超过阈值({FAILURE_THRESHOLD}只)，"
                    f"成功率仅 {success_count / total * 100:.1f}%。"
                    f"请检查数据源连通性后重新执行初始化。"
                )

        except StaleDataSourceError:
            logger.error("=" * 60)
            logger.error("数据源时效校验未通过，已中止K线初始化（避免用旧数据选股）")
            logger.error("=" * 60)
            raise
        except Exception as e:
            logger.error(f"初始化K线历史数据失败: {e}")
            # 重新抛出异常，让上层 init_all 感知失败
            raise
    
    # ==================== 行业和板块数据初始化 ====================
    
    def _init_industry_data(self, stock_codes: list) -> None:
        """
        初始化行业数据
        
        参数：
            stock_codes: 股票代码列表
        """
        logger.info("初始化行业数据...")
        # TODO: 实现行业数据初始化逻辑
        logger.info("行业数据初始化完成")
    
    def _init_sector_data(self, stock_codes: list) -> None:
        """
        初始化板块数据
        
        参数：
            stock_codes: 股票代码列表
        """
        logger.info("初始化板块数据...")
        # TODO: 实现板块数据初始化逻辑
        logger.info("板块数据初始化完成")
    
    # ==================== 资金流向数据初始化 ====================
    
    def _init_fund_flow_data(self, stock_codes: list, include_industry_sector: bool = True) -> dict:
        """
        初始化资金流向数据
        
        参数：
            stock_codes: 股票代码列表
            include_industry_sector: 是否包括行业和板块资金流向
        
        返回：
            初始化统计信息字典
        """
        logger.info("开始初始化资金流向数据...")
        stats = {
            'stock_moneyflow': 0,
            'industry_moneyflow': 0,
            'sector_moneyflow': 0
        }
        
        try:
            # TODO: 实现资金流向数据初始化逻辑
            logger.info("资金流向数据初始化完成")
        except Exception as e:
            logger.error(f"初始化资金流向数据失败: {e}")
        
        return stats
    
    # ==================== 事件数据初始化 ====================
    
    def _init_event_data(self, stock_codes: list) -> dict:
        """
        初始化事件数据（暂时不可用）
        Tushare anns_d 接口权限未开通，暂时跳过事件数据初始化
        
        参数：
            stock_codes: 股票代码列表
        
        返回：
            初始化统计信息字典
        """
        logger.info("事件数据初始化暂时跳过（Tushare 权限未开通）")
        return {'event_data': 0}

    # ==================== 增量模式：缺口规划 ====================

    def _load_existing_codes(self) -> set:
        """读取 stock_basic 中已有的股票代码

        注意：DBManager 只有 query()，没有 query_all()。
        原代码调用 query_all() 抛 AttributeError 又被裸 except 吞掉，
        导致 existing_stocks 恒为空、增量模式天天变成全市场重拉。
        """
        try:
            rows = self.db_manager.query("SELECT DISTINCT code FROM stock_basic")
            return {row['code'] for row in rows if row.get('code')}
        except Exception as e:
            logger.warning(f"读取已有股票列表失败，本次按全量处理: {e}")
            return set()

    def _build_gap_plan(self, old_codes: list) -> tuple:
        """为已有股票计算各自需要补多少根K线（真增量，避免每天全市场重拉）

        日常每个交易日跑一次时，绝大多数股票只需 25 根就够；
        机器人停跑一段时间后，缺口大的股票自动加大抓取根数。

        返回：
            (days_map, 说明文字)，days_map 为 {股票代码: 需要抓取的K线根数}
        """
        last_map = {}
        try:
            rows = self.db_manager.query(
                "SELECT code, MAX(date) AS last_date FROM stock_kline GROUP BY code")
            for row in rows:
                _code = row.get('code')
                _last = _norm_date(row.get('last_date'))
                if _code and _last:
                    last_map[_code] = _last
        except Exception as e:
            logger.warning(f"读取各股票最新K线日期失败，本轮按全量处理: {e}")
            return ({}, '按全量处理')

        today = datetime.now().date()
        days_map = {}
        buckets = {}
        for code in old_codes:
            last = last_map.get(code)
            if not last:
                need = 260
            else:
                try:
                    gap_days = (today - datetime.strptime(last, '%Y-%m-%d').date()).days
                except Exception:
                    gap_days = 9999
                if gap_days <= 7:
                    need = 25
                elif gap_days <= 30:
                    need = 50
                elif gap_days <= 90:
                    need = 100
                else:
                    need = 260
            days_map[code] = need
            buckets[need] = buckets.get(need, 0) + 1

        desc = ', '.join(f'{k}根×{v}只' for k, v in sorted(buckets.items()))
        logger.info(f"K线缺口规划: {desc}")
        return (days_map, desc)

    # ==================== 统一初始化入口 ====================
    
    def init_full_data(self, max_stocks: Optional[int] = None, years: int = 3,
                       incremental: bool = False, stock_dict: dict = None,
                       stock_codes: list = None) -> None:
        """
        统一的初始化入口，支持全量和增量两种模式
        
        参数：
            max_stocks: 最多初始化多少只股票（None 表示全部）
            years: 获取K线数据的年份数（默认 3 年）
            incremental: 是否增量更新（默认 False，全量初始化）
                        增量语义：新股票拉 years 年全量；已有股票只补"最后一根K线之后"的缺口
            stock_dict: 预获取的股票代码到名称的映射字典（可选，避免重复拉取）
            stock_codes: 直接传入股票代码列表（可选，跳过API拉取步骤）
        """
        mode = "增量" if incremental else "全量"
        logger.info(f"开始{mode}初始化数据...")
        
        try:
            # 获取股票代码：优先使用传入的列表
            if stock_codes is not None:
                all_stocks = stock_dict or {}
                stock_codes = list(stock_codes)
            else:
                # 获取所有股票代码（如果未传入则拉取）
                if stock_dict:
                    all_stocks = stock_dict
                else:
                    self._report_progress(0, "正在获取股票列表...")
                    all_stocks = self.stock_data_fetcher.get_all_stock_codes()
                stock_codes = list(all_stocks.keys())
            
            # 限制股票数量（先截断，再区分新股/老股，避免 max_stocks 只作用于其中一类）
            if max_stocks:
                stock_codes = stock_codes[:max_stocks]

            if not stock_codes:
                logger.info("没有需要初始化的股票，跳过")
                return

            # 增量模式不再"过滤掉已有股票"（那会让老股永远不更新），
            # 而是区分：新股票拉全量历史，已有股票只补缺口
            new_codes, old_codes = [], []
            if incremental:
                existing_stocks = self._load_existing_codes()
                for _code in stock_codes:
                    if _code in existing_stocks:
                        old_codes.append(_code)
                    else:
                        new_codes.append(_code)
                logger.info(
                    f"增量模式: 已有股票 {len(old_codes)} 只(仅补缺口) / "
                    f"新股票 {len(new_codes)} 只(拉全量)"
                )
            else:
                new_codes = list(stock_codes)
            
            total = len(stock_codes)
            logger.info(f"准备初始化 {total} 只股票的数据...")
            
            # 进度分段（映射到 0-100）：
            # 基础数据 0%-15%, K线 15%-80%, 行业 80%-85%, 板块 85%-90%, 资金 90%-95%, 事件 95%-100%
            stages = [0, 15, 80, 85, 90, 95, 100]
            
            # 1. 初始化基础数据
            self._report_progress(stages[0], f"正在初始化基础数据（{total}只）...")
            self._init_basic_data(stock_codes, stock_dict if not incremental else all_stocks)
            self._report_progress(stages[1], "基础数据初始化完成")
            
            # 2. 初始化K线历史数据（内部会按批次回调 + 逐批做时效校验）
            #    先补新股票的全量历史，再补已有股票的缺口
            if new_codes:
                self._report_progress(
                    stages[1], f"正在获取K线数据（新股 {len(new_codes)} 只 / 全量 {years} 年）...")
                self._init_kline_history_data(new_codes, years=years,
                                              progress_range=(stages[1], stages[2]))
            if old_codes:
                days_map, gap_desc = self._build_gap_plan(old_codes)
                # 按缺口从小到大排，让每批的抓取根数尽量一致，避免小缺口股票被大缺口拖累
                old_codes.sort(key=lambda c: days_map.get(c, years * 250))
                self._report_progress(
                    stages[1], f"正在补K线缺口（{len(old_codes)} 只 / {gap_desc}）...")
                self._init_kline_history_data(old_codes, years=years,
                                              progress_range=(stages[1], stages[2]),
                                              days_map=days_map)
            self._report_progress(stages[2], "K线数据初始化完成")
            
            # 3. 初始化行业数据
            self._report_progress(stages[2], "正在初始化行业数据...")
            self._init_industry_data(stock_codes)
            self._report_progress(stages[3], "行业数据初始化完成")
            
            # 4. 初始化板块数据
            self._report_progress(stages[3], "正在初始化板块数据...")
            self._init_sector_data(stock_codes)
            self._report_progress(stages[4], "板块数据初始化完成")
            
            # 5. 初始化资金流向数据
            self._report_progress(stages[4], "正在初始化资金流向数据...")
            self._init_fund_flow_data(stock_codes)
            self._report_progress(stages[5], "资金流向数据初始化完成")
            
            # 6. 初始化事件数据
            self._report_progress(stages[5], "正在初始化事件数据...")
            self._init_event_data(stock_codes)
            self._report_progress(stages[6], "事件数据初始化完成")
            
            logger.info(f"{mode}初始化完成")
        
        except StaleDataSourceError:
            # 时效校验未通过属于"必须让上层知道"的致命错误，不能像普通错误那样只记日志
            raise
        except Exception as e:
            logger.error("=" * 60)
            logger.error(f"{mode}初始化失败: {e}")
            logger.error("请检查数据源连通性后重新执行初始化（运行主程序将自动触发）")
            logger.error("=" * 60)
    
    # ==================== 映射更新检查 ====================
    
    def _check_mapping_update_needed(self, mapping_type: str) -> bool:
        """
        检查映射是否需要更新
        
        参数：
            mapping_type: 映射类型 (industry, sector)
        
        返回：
            是否需要更新
        """
        logger.debug(f"检查 {mapping_type} 映射是否需要更新...")
        # TODO: 实现映射更新检查逻辑
        return False
    
    def _update_industry_data(self, stock_codes: list) -> None:
        """
        更新行业数据
        
        参数：
            stock_codes: 股票代码列表
        """
        logger.info("更新行业数据...")
        # TODO: 实现行业数据更新逻辑
        logger.info("行业数据更新完成")
    
    def _update_sector_data(self, stock_codes: list) -> None:
        """
        更新板块数据
        
        参数：
            stock_codes: 股票代码列表
        """
        logger.info("更新板块数据...")
        # TODO: 实现板块数据更新逻辑
        logger.info("板块数据更新完成")
