"""src/pkg/data_srv/client.py\n
fetch_indicator_data(ctx) - fetch data lines\n
fetch_target_data(ctx) - fetch OHLC data
"""
import logging

from pkg import DEBUG
from pkg.data_srv import utils


logger = logging.getLogger(__name__)


def fetch_indicator_data(ctx:dict)->None:
    """Data for calculating indicators i.e. clv, price, volume etc."""
    if DEBUG: logger.debug(f"fetch_indicator_data(ctx={type(ctx)})")
    if not DEBUG:
        print(" Begin download process:")

    # create database
    utils.create_sqlite_indicator_database(ctx=ctx)

    # select data provider
    processor = _select_data_provider(ctx=ctx)

    # get and save data for each ticker
    for index, ticker in enumerate(ctx['interface']['arguments']):
        if not DEBUG:
            print(f"  - fetching {ticker}\t", end="")

        ctx['interface']['index'] = index  # alphavantage may throttle at five downloads
        data_tuple = processor.download_and_parse_price_data(ticker=ticker)
        utils.write_indicator_data_to_sqlite_db(ctx=ctx, data_tuple=data_tuple)

    if not DEBUG:
        print(" finished.")


def fetch_target_data(ctx:dict)->None:
    """ohlc price data for target symbol."""
    if DEBUG: logger.debug(f"fetch_target_data(ctx={type(ctx)})")

#     _, df = _select_data_provider(ctx=ctx, symbol=symbol)

#     tuple_list = process.df_to_list_of_tuples(symbol=symbol, df=df)

#     db_writer = utils.SqliteWriter(ctx=ctx)
#     db_writer.save_target_data(tuple_list=tuple_list)


def _select_data_provider(ctx:dict)->object:
    """Use provider from data service config file"""
    if DEBUG: logger.debug(f"_select_data_provider(ctx={ctx})")

    match ctx['data_service']['data_provider']:
        case "alphavantage":
            from pkg.data_srv.agent import AlphaVantageDataProcessor
            return AlphaVantageDataProcessor(ctx=ctx)
        case "tiingo":
            from pkg.data_srv.agent import TiingoDataProcessor
            return TiingoDataProcessor(ctx=ctx)
        case "yfinance":
            from pkg.data_srv.agent import YahooFinanceDataProcessor
            return YahooFinanceDataProcessor(ctx=ctx)
        case _:
            raise ValueError(f"unknown provider: {ctx['data_service']['data_provider']}")
