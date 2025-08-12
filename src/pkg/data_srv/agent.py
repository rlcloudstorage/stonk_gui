"""src/pkg/data_srv/agent.py\n
Collect open, high, low, close, volume \n
(ohlc) data from various online sources.\n
Process data into lines; volume, average price,\n
close location value, etc. Returns a tuple.\n
class TiingoDataProcessor\n
class YahooFinanceDataProcessor
"""

import datetime, logging, os, pickle, time

from statistics import fmean

import pandas as pd

from dotenv import load_dotenv
from numpy.lib.stride_tricks import sliding_window_view
from pkg import DEBUG


load_dotenv()

logging.getLogger("peewee").setLevel(logging.WARNING)
logging.getLogger("yfinance").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class BaseProcessor:
    """"""

    def __init__(self, ctx: dict):
        self.data_line = ctx["interface"]["data_line"]
        self.data_provider = ctx["data_service"]["data_provider"]
        self.frequency = ctx["data_service"]["data_frequency"]
        self.lookback = int(ctx["data_service"]["data_lookback"])
        self.scaler = self._set_sklearn_scaler(ctx["data_service"]["sklearn_scaler"])
        self.start_date, self.end_date = self._start_end_date
        self.window_size = int(ctx["interface"]["window_size"])
        self.work_dir = ctx["default"]["work_dir"]

    @property
    def _start_end_date(self):
        """Set the start and end dates"""
        lookback = int(self.lookback)
        start = datetime.date.today() - datetime.timedelta(days=lookback)
        end = datetime.date.today()
        return start, end

    def _sliding_window_scaled_data(self, data_list: list):
        """"""
        if DEBUG:
            logger.debug(f"_sliding_window_scaled_data(data_list={data_list})")

        scaled_data = list()
        v = sliding_window_view(x=data_list, window_shape=self.window_size)

        # scale each row in window view then append last item to scaled_data list
        for row in v:
            scaled_row = self.scaler.fit_transform(X=row.reshape(-1, 1))
            scaled_item = int((scaled_row.item(-1) + 10) * 100)
            scaled_data.append(scaled_item)

        # pad front of scaled_data with average value
        return [int(fmean(scaled_data))] * (self.window_size - 1) + scaled_data

    def _set_sklearn_scaler(self, scaler):
        """Uses config file [data_service][sklearn_scaler] value"""
        if scaler == "MinMaxScaler":
            from sklearn.preprocessing import MinMaxScaler

            return MinMaxScaler()
        elif scaler == "RobustScaler":
            from sklearn.preprocessing import RobustScaler

            # return RobustScaler(quantile_range=(0.0, 100.0))
            return RobustScaler()

    def download_and_parse_price_data(self, ticker: str) -> tuple:
        """Returns a tuple, (ticker, dataframe)"""
        if DEBUG:
            logger.debug(f"download_and_parse_price_data(self={self}, ticker={ticker})")

        if not DEBUG:
            print(f"  - fetching {ticker}...\t", end="")
        data_gen = eval(f"self._{self.data_provider}_data_generator(ticker=ticker)")

        if not DEBUG:
            print("processing data\t", end="")
        return eval(f"self._process_{self.data_provider}_data(data_gen=data_gen)")


class TiingoDataProcessor(BaseProcessor):
    """Fetch ohlc price data from tiingo.com"""

    from tiingo import TiingoClient

    def __init__(self, ctx: dict):
        super().__init__(ctx=ctx)
        self.api_key = {os.getenv("TOKEN_TIINGO")}
        self.frequency = ctx["data_service"]["data_frequency"]

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"api_token={self.api_key}, "
            f"data_line={self.data_line}, "
            f"data_provider={self.data_provider}, "
            f"frequency={self.frequency}, "
            f"scaler={self.scaler}, "
            f"start_date={self.start_date}, "
            f"end_date={self.end_date})"
        )

    def _tiingo_data_generator(self, ticker: str) -> object:
        """Yields a tuple (ticker, json)"""

        if DEBUG:
            logger.debug(f"_tiingo_data_generator(ticker={ticker})")

        config = {}
        # To reuse the same HTTP Session across API calls
        # (and have better performance), include a session key.
        config["session"] = True
        # If you don't have your API key as an environment variable,
        # pass it in via a configuration dictionary.
        config["api_key"] = self.api_key
        config["api_key"] = os.getenv("TOKEN_TIINGO")
        # Initialize
        client = self.TiingoClient(config)

        try:
            historical_prices = client.get_ticker_price(
                ticker=ticker, fmt='json', startDate=self.start_date, endDate=self.end_date, frequency=self.frequency
            )
        except Exception as e:
            logger.debug(f"*** ERROR *** {e}")
        else:
            # # pickle ticker, historical_prices
            # with open(f"{self.work_dir}{ticker}.t.pkl", "wb") as pkl:
            #     pickle.dump((ticker, historical_prices), pkl)
            yield ticker, historical_prices

        # # yield data from saved pickle file
        # with open(f"{self.work_dir}{ticker}.t.pkl", "rb") as pkl:
        #     ticker, historical_prices = pickle.load((pkl))
        # yield ticker, historical_prices

    def _process_tiingo_data(self, data_gen: object) -> list[tuple]:
        """Returns a tuple (ticker, dataframe)"""
        if DEBUG:
            logger.debug(f"_process_tiingo_data(data_gen={type(data_gen)})")

        ticker, dict_list = next(data_gen)  # unpack items in data_gen

        # create empty dataframe with index as a timestamp
        df = pd.DataFrame(
            index=[
                round(time.mktime(datetime.datetime.strptime(d["date"][:10], "%Y-%m-%d").timetuple()))
                for d in dict_list
            ]
        )
        df.index.name = "date"

        # difference between the close and open price
        clop = [round((d["adjClose"] - d["adjOpen"]) * 100) for d in dict_list]
        if DEBUG:
            logger.debug(f"clop: {clop} {type(clop)}")

        # close location value, relative to the high-low range
        try:
            clv = [
                round(((2 * d["adjClose"] - d["adjLow"] - d["adjHigh"]) / (d["adjHigh"] - d["adjLow"])) * 100)
                for d in dict_list
            ]
            if DEBUG:
                logger.debug(f"clv: {clv} {type(clv)}")
        except ZeroDivisionError as e:
            logger.debug(f"*** ERROR *** {e}")

        # close weighted average price exclude open price
        cwap = [round(((d["adjHigh"] + d["adjLow"] + 2 * d["adjClose"]) / 4) * 100) for d in dict_list]
        if DEBUG:
            logger.debug(f"cwap array: {cwap} {type(cwap)}")

        sc_cwap = self._sliding_window_scaled_data(data_list=cwap)
        if DEBUG:
            logger.debug(f"scaled cwap: {sc_cwap} {type(sc_cwap)} len {len(cwap)}")

        # difference between the high and low price
        hilo = [round((d["adjHigh"] - d["adjLow"]) * 100) for d in dict_list]
        if DEBUG:
            logger.debug(f"hilo: {hilo} {type(hilo)}")

        # number of shares traded
        volume = [d["adjVolume"] for d in dict_list]
        if DEBUG:
            logger.debug(f"volume array: {volume} {type(volume)}")

        sc_vol = self._sliding_window_scaled_data(data_list=volume)
        if DEBUG:
            logger.debug(f"scaled volume: {sc_vol} {type(sc_vol)} len {len(volume)}")

        # price times number of shares traded
        mass = [c * v for c, v in zip(cwap, volume)]
        sc_mass = self._sliding_window_scaled_data(data_list=mass)
        if DEBUG:
            logger.debug(f"scaled_mass: {sc_mass} {type(sc_mass)}")

        # insert values for each data line into df
        for i, item in enumerate(self.data_line):
            df.insert(loc=i, column=f"{item.lower()}", value=eval(item.lower()), allow_duplicates=True)

        return ticker, df


class YahooFinanceDataProcessor(BaseProcessor):
    """Fetch ohlc price data using yfinance"""

    import yfinance as yf

    def __init__(self, ctx: dict):
        super().__init__(ctx=ctx)
        self.interval = self._parse_frequency

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"data_line={self.data_line}, "
            f"data_provider={self.data_provider}, "
            f"interval={self.interval}, "
            f"scaler={self.scaler}, "
            f"start_date={self.start_date}, "
            f"end_date={self.end_date})"
        )

    @property
    def _parse_frequency(self):
        """Convert daily/weekly frequency to provider format"""
        frequency_dict = {"daily": "1d", "weekly": "1w"}
        return frequency_dict[self.frequency]

    def _yfinance_data_generator(self, ticker: str) -> object:
        """Yields a generator object tuple (ticker, dataframe)"""

        if DEBUG:
            logger.debug(f"_yfinance_data_generator(ticker={ticker})")

        try:
            yf_data = self.yf.Ticker(ticker=ticker)
            yf_df = yf_data.history(start=self.start_date, end=self.end_date, interval=self.interval)
        except Exception as e:
            logger.debug(f"*** ERROR *** {e}")
        else:
            # # pickle ticker, yf_df
            # with open(f"{self.work_dir}{ticker}.yf.pkl", "wb") as pkl:
            #     pickle.dump((ticker, yf_df), pkl)
            yield ticker, yf_df

        # # yield data from saved pickle file
        # with open(f"{self.work_dir}{ticker}.yf.pkl", "rb") as pkl:
        #     ticker, df = pickle.load((pkl))
        # yield ticker, df

    def _process_yfinance_data(self, data_gen: object) -> pd.DataFrame:
        """Returns a tuple (ticker, dataframe)"""
        if DEBUG:
            logger.debug(f"_process_yfinance_data(data_gen={type(data_gen)})")

        ticker, yf_df = next(data_gen)
        # remove unused columns
        yf_df = yf_df.drop(columns=yf_df.columns.values[-3:], axis=1)
        if DEBUG:
            logger.debug(f"ticker: {ticker}, yf_df:\n{yf_df}")

        # create empty dataframe with index as a timestamp, trim off minutes seconds
        df = pd.DataFrame(index=yf_df.index.values.astype(int) // 10**9)
        df.index.name = "date"

        # difference between the close and open price
        clop = list(round((yf_df["Close"] - yf_df["Open"]) * 100).astype(int))
        if DEBUG:
            logger.debug(f"clop: {clop} {type(clop)}")

        # close location value, relative to the high-low range
        try:
            clv = list(
                round(
                    (2 * yf_df["Close"] - yf_df["Low"] - yf_df["High"]) / (yf_df["High"] - yf_df["Low"]) * 100
                ).astype(int)
            )
            if DEBUG:
                logger.debug(f"clv: {clv} {type(clv)}")
        except ZeroDivisionError as e:
            logger.debug(f"*** ERROR *** {e}")

        # close weighted average price exclude open price
        cwap = list(round((2 * yf_df["Close"] + yf_df["High"] + yf_df["Low"]) * 25).astype(int))
        if DEBUG:
            logger.debug(f"cwap array: {cwap} {type(cwap)}")

        sc_cwap = self._sliding_window_scaled_data(data_list=cwap)
        if DEBUG:
            logger.debug(f"scaled cwap: {sc_cwap} {type(sc_cwap)} len {len(cwap)}")

        # difference between the high and low price
        hilo = list(round((yf_df["High"] - yf_df["Low"]) * 100).astype(int))
        if DEBUG:
            logger.debug(f"hilo: {hilo} {type(hilo)}")

        # number of shares traded
        volume = list(yf_df["Volume"])
        if DEBUG:
            logger.debug(f"volume array: {volume} {type(volume)}")

        sc_vol = self._sliding_window_scaled_data(data_list=volume)
        if DEBUG:
            logger.debug(f"scaled volume: {sc_vol} {type(sc_vol)} len {len(volume)}")

        # price times number of shares traded
        mass = [c * v for c, v in zip(cwap, volume)]
        sc_mass = self._sliding_window_scaled_data(data_list=mass)
        if DEBUG:
            logger.debug(f"scaled_mass: {sc_mass} {type(sc_mass)}")

        # insert values for each data line into df
        for i, item in enumerate(self.data_line):
            df.insert(loc=i, column=f"{item.lower()}", value=eval(item.lower()), allow_duplicates=True)

        return ticker, df
