from datetime import timedelta
from typing import Dict, List, Optional, Callable
import traceback

from pandas import DataFrame, Timestamp
from tqsdk import TqApi, TqAuth
import pandas

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.utility import ZoneInfo


INTERVAL_VT2TQ: Dict[Interval, int] = {
    Interval.MINUTE: 60,
    Interval.HOUR: 60 * 60,
    Interval.DAILY: 60 * 60 * 24,
    Interval.TICK: 0
}

CHINA_TZ = ZoneInfo("Asia/Shanghai")


class TqsdkDatafeed(BaseDatafeed):
    """天勤TQsdk数据服务接口"""

    def __init__(self):
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        """查询k线数据"""
        # 初始化API
        try:
            api: TqApi = TqApi(auth=TqAuth(self.username, self.password))
        except Exception:
            output(traceback.format_exc())
            return None

        # 查询数据
        tq_symbol: str = f"{req.exchange.value}.{req.symbol}"

        df: DataFrame = api.get_kline_data_series(
            symbol=tq_symbol,
            duration_seconds=INTERVAL_VT2TQ[req.interval],
            start_dt=req.start,
            end_dt=(req.end + timedelta(1))
        )

        # 关闭API
        api.close()

        # 解析数据
        bars: List[BarData] = []

        if df is not None:
            for tp in df.itertuples():
                # 天勤时间为与1970年北京时间相差的秒数，需要加上8小时差
                dt: Timestamp = Timestamp(tp.datetime).to_pydatetime() + timedelta(hours=8)

                bar: BarData = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    interval=req.interval,
                    datetime=dt.replace(tzinfo=CHINA_TZ),
                    open_price=tp.open,
                    high_price=tp.high,
                    low_price=tp.low,
                    close_price=tp.close,
                    volume=tp.volume,
                    open_interest=tp.open_oi,
                    gateway_name="TQ",
                )
                bars.append(bar)

        return bars

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[TickData]]:
        """查询k线数据"""
        # 初始化API
        try:
            api: TqApi = TqApi(auth=TqAuth(self.username, self.password))
        except Exception:
            output(traceback.format_exc())
            return None

        # 查询数据
        tq_symbol: str = f"{req.exchange.value}.{req.symbol}"

        df: DataFrame = api.get_tick_data_series(
            symbol=tq_symbol,
            start_dt=req.start,
            end_dt=(req.end + timedelta(1))
        )

        # 关闭API
        api.close()

        # 解析数据
        data: List[TickData] = []

        if df is not None:
            for tp in df.itertuples():
                # 天勤时间为与1970年北京时间相差的秒数，需要加上8小时差
                dt: Timestamp = Timestamp(tp.datetime).to_pydatetime() + timedelta(hours=8)

                # symbol = req.symbol,
                # exchange = req.exchange,
                # interval = req.interval,
                # datetime = dt.replace(tzinfo=CHINA_TZ),
                # open_price = tp.open,
                # high_price = tp.high,
                # low_price = tp.low,
                # close_price = tp.close,
                # volume = tp.volume,
                # open_interest = tp.open_oi,
                # gateway_name = "TQ",

                # *id: 12345
                # tick序列号
                # *datetime: 1501074872000000000(tick从交易所发出的时间(按北京时间)，自unix
                # epoch(1970 - 01 - 01
                # 00: 00:00
                # GMT)以来的纳秒数)
                # *last_price: 3887.0(最新价)
                # *average: 3820.0(当日均价)
                # *highest: 3897.0(当日最高价)
                # *lowest: 3806.0(当日最低价)
                # *ask_price1: 3886.0(卖一价)
                # *ask_volume1: 3(卖一量)
                # *bid_price1: 3881.0(买一价)
                # *bid_volume1: 18(买一量)
                # *volume: 7823(当日成交量)
                # *amount: 19237841.0(成交额)
                # *open_interest: 1941(持仓量)
                if pandas.isna(tp.last_price):
                    print(tp)
                    continue

                tick: TickData = TickData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=dt.replace(tzinfo=CHINA_TZ),
                    open_price=0,
                    high_price=tp.highest,
                    low_price=tp.lowest,
                    pre_close=0,
                    last_price=tp.last_price,
                    volume=tp.volume,
                    turnover=tp.amount,
                    open_interest=getattr(tp, "open_interest", 0),
                    limit_up=9999999,
                    limit_down=0,
                    bid_price_1=tp.bid_price1,
                    ask_price_1=tp.ask_price1,
                    bid_volume_1=tp.bid_volume1,
                    ask_volume_1=tp.ask_volume1,
                    gateway_name="TQ"
                )
                data.append(tick)

        return data
