"""src/pkg/data_srv/utils.py\n
create_sqlite_ohlc_database(ctx: dict) -> None\n
create_sqlite_stonk_database(ctx: dict) -> None\n
write_data_line_to_stonk_table(ctx: dict, data_tuple: tuple) -> None"""

import logging

from pathlib import Path

from pkg import DEBUG
from pkg.ctx_mgr import SqliteConnectManager


logger = logging.getLogger(__name__)


def create_sqlite_ohlc_database(ctx: dict) -> None:
    """Create sqlite3 database. Table for each ticker symbol, column for ohlc."""
    if DEBUG:
        logger.debug(f"create_sqlite_ohlc_database(ctx={ctx})")

    # create data folder in users work_dir
    Path(f"{ctx['default']['work_dir']}{ctx['interface']['command']}").mkdir(parents=True, exist_ok=True)
    # if old database exists remove it
    Path(f"{ctx['default']['work_dir']}{ctx['interface']['command']}/{ctx['interface']['database']}").unlink(
        missing_ok=True
    )

    try:
        with SqliteConnectManager(ctx=ctx, mode="rwc") as con:
            # create table for each ticker symbol
            for table in ctx["interface"]["ticker"]:
                # create ohlc table for ticker
                con.cursor.execute(
                    f"""
                    CREATE TABLE {table} (
                        date      INTEGER    NOT NULL,
                        Open      INTEGER,
                        High      INTEGER,
                        Low       INTEGER,
                        Close     INTEGER,
                        Volume    INTEGER,
                        PRIMARY KEY (date)
                    )"""
                )
    except con.sqlite3.Error as e:
        logger.debug(f"*** ERROR *** {e}")

    if not DEBUG:
        print(f"\n Created db: '{con.db_path}'")


def create_sqlite_stonk_database(ctx: dict) -> None:
    """Create sqlite3 database. Table for each ticker symbol, column for each data line."""
    if DEBUG:
        logger.debug(f"create_sqlite_indicator_database(ctx={type(ctx)})")

    # create data folder in users work_dir
    Path(f"{ctx['default']['work_dir']}{ctx['interface']['command']}").mkdir(parents=True, exist_ok=True)
    # if old database exists remove it
    Path(f"{ctx['default']['work_dir']}{ctx['interface']['command']}/{ctx['interface']['database']}").unlink(
        missing_ok=True
    )

    try:
        with SqliteConnectManager(ctx=ctx, mode="rwc") as con:
            # create table for each ticker symbol
            for table in ctx["interface"]["ticker"]:
                con.cursor.execute(
                    f"""
                    CREATE TABLE {table.upper()} (
                        date    INTEGER    NOT NULL,
                        PRIMARY KEY (date)
                    )
                """
                )
                # add column for each indicator (data_line)
                for col in ctx["interface"]["data_line"]:
                    con.cursor.execute(
                        f"""
                        ALTER TABLE {table} ADD COLUMN {col.lower()} INTEGER
                    """
                    )
    except con.sqlite3.Error as e:
        logger.debug(f"*** ERROR *** {e}")

    if not DEBUG:
        print(f"\n Created db: '{con.db_path}'")


def write_data_line_to_stonk_table(ctx: dict, data_tuple: tuple) -> None:
    """"""
    if DEBUG:
        logger.debug(
            f"write_data_line_to_stonk_table(ctx={ctx}, data_tuple[0]: {data_tuple[0]}, data_tuple[1]:\n{data_tuple[1]})"
        )
    if not DEBUG:
        print(f"writing to db\t")

    stonk_table = data_tuple[0]
    data_list = list(data_tuple[1].itertuples(index=True, name=None))
    try:
        with SqliteConnectManager(ctx=ctx, mode="rw") as con:
            if DEBUG:
                logger.debug(f"stonk_table: {stonk_table}, data_list: {data_list}, {type(data_list)}")
            # con.cursor.executemany(f"INSERT INTO {stonk_table} VALUES (?,?,?)", data_list)
            con.cursor.executemany(f"INSERT INTO {stonk_table} VALUES (?,?,?,?,?,?,?,?,?)", data_list)
    except con.sqlite3.Error as e:
        logger.debug(f"*** Error *** {e}")


# def write_stonk_data_to_data_line_table(ctx: dict, data_tuple: tuple)->None:
#     """tables are data lines, columns are ticker symbols. Very slow!"""
#     if DEBUG:
#         logger.debug(f"write_stonk_data_to_data_line_table(ctx={type(ctx)}, data_tuple={type(data_tuple)})")
#     if not DEBUG:
#         print(f"writing to db\t")

#     with sqlite3.connect(database=db) as conn:
#         cursor = conn.cursor()
#         for row in tuple_list:
#             symbol = type(row).__name__
#             date = row.Index
#             for dl in data_line:
#                 table = dl.lower()
#                 value = getattr(row, table)
#                 try:
#                     if index == 0:
#                         query = f"INSERT INTO {table} (Date, {symbol}) VALUES (?, ?)"
#                         # if DEBUG: logger.debug(f"query: {query}")
#                         cursor.execute(query, (date, value))
#                     else:
#                         # query = f"UPDATE {table} SET {symbol} = ? WHERE Date = {date}", (value,)
#                         # if DEBUG: logger.debug(f"query: {query}")
#                         cursor.execute(f"UPDATE {table} SET {symbol} = ? WHERE Date = {date}", (value,))
#                 except Exception as e:
#                     logger.debug(f"*** ERROR *** {e}")
#                 else:
#                     conn.commit()
