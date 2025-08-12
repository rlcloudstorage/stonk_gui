"""src/pkg/data_srv/client.py\n
fetch_stonk_data(ctx: dict) -> None
"""

import logging

from pkg import DEBUG
from pkg.data_srv import utils


logger = logging.getLogger(__name__)


def fetch_stonk_data(ctx: dict) -> None:
    """Data for calculating indicators i.e. clv, price, volume etc."""
    if DEBUG:
        logger.debug(f"fetch_stonk_data(ctx={ctx}")
    if not DEBUG:
        print(" Begin download process:")

    # create database
    utils.create_sqlite_stonk_database(ctx=ctx)

    # select data provider
    processor = _select_data_provider(ctx=ctx)

    # get and save data for each ticker
    for index, ticker in enumerate(ctx["interface"]["ticker"]):
        if not DEBUG:
            print(f"  - fetching {ticker}\t", end="")

        ctx["interface"]["index"] = index  # alphavantage may throttle at five downloads
        data_tuple = processor.download_and_parse_price_data(ticker=ticker)
        utils.write_data_line_to_stonk_table(ctx=ctx, data_tuple=data_tuple)

    if not DEBUG:
        print(" finished.")


def _select_data_provider(ctx: dict) -> object:
    """Use provider from data service config file"""
    if DEBUG:
        logger.debug(f"_select_data_provider(ctx={type(ctx)})")

    match ctx["data_service"]["data_provider"]:
        case "tiingo":
            from pkg.data_srv.agent import TiingoDataProcessor
            return TiingoDataProcessor(ctx=ctx)
        case "yfinance":
            from pkg.data_srv.agent import YahooFinanceDataProcessor
            return YahooFinanceDataProcessor(ctx=ctx)
        case _:
            raise ValueError(f"unknown provider: {ctx['data_service']['data_provider']}")
