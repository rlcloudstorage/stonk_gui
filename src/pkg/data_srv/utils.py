"""src/pkg/data_srv/utils.py\n
create_sqlite_indicator_database(ctx: dict) -> None\n
write_indicator_data_to_sqlite_db(ctx: dict, data_tuple: tuple)->None"""

import logging

from pathlib import Path

from pkg import DEBUG
from pkg.ctx_mgr import SqliteConnectManager


logger = logging.getLogger(__name__)


def create_sqlite_indicator_database(ctx: dict) -> None:
    """Create sqlite3 database. Table for each ticker symbol, column for each data line."""
    if DEBUG:
        logger.debug(f"create_sqlite_indicator_database(ctx={type(ctx)})")

    # create data folder in users work_dir
    Path(f"{ctx['default']['work_dir']}/data").mkdir(parents=True, exist_ok=True)
    # if old database exists remove it
    Path(f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}").unlink(missing_ok=True)

    try:
        with SqliteConnectManager(ctx=ctx, mode="rwc") as conn:
            # create table for each ticker symbol
            for table in ctx["interface"]["arguments"]:
                conn.cursor.execute(
                    f"""
                    CREATE TABLE {table.upper()} (
                        date    INTEGER    NOT NULL,
                        PRIMARY KEY (date)
                    )
                """
                )
                # add column for each indicator (data_line)
                for col in ctx["interface"]["data_line"]:
                    conn.cursor.execute(
                        f"""
                        ALTER TABLE {table} ADD COLUMN {col.lower()} INTEGER
                    """
                    )
    except conn.sqlite3.Error as e:
        logger.debug(f"*** ERROR *** {e}")

    if not DEBUG:
        print(f"\n Created db: '{conn.db_path}'")

    # # create table for target symbol (ohlc prices)
    # for table in ctx['interface']['target_data']:
    #     db.cursor.execute(f'''
    #         CREATE TABLE {table} (
    #             Date      INTEGER    NOT NULL,
    #             Open      INTEGER    NOT NULL,
    #             High      INTEGER    NOT NULL,
    #             Low       INTEGER    NOT NULL,
    #             Close     INTEGER    NOT NULL,
    #             Volume    INTEGER    NOT NULL,
    #             PRIMARY KEY (Date)
    #         )
    #         WITHOUT ROWID
    #     ''')


def write_indicator_data_to_sqlite_db(ctx: dict, data_tuple: tuple)->None:
    """"""
    if DEBUG:
        logger.debug(f"write_indicator_data_to_sqlite_db(ctx={type(ctx)}, data_tuple={type(data_tuple)})")

    if not DEBUG:
        print(f"writing to db\t")
    try:
        with SqliteConnectManager(ctx=ctx, mode="rw") as con:
            for row in data_tuple[1].itertuples(index=True, name=None):
                con.cursor.execute(f"INSERT INTO {data_tuple[0]} VALUES (?,?,?,?,?,?,?)", row)
    except con.sqlite3.Error as e:
        logger.debug(f"*** Error *** {e}")


# tables are data lines, columns are ticker symbols. Very slow!
# with sqlite3.connect(database=db) as conn:
#     cursor = conn.cursor()
#     for row in tuple_list:
#         symbol = type(row).__name__
#         date = row.Index
#         for dl in data_line:
#             table = dl.lower()
#             value = getattr(row, table)
#             try:
#                 if index == 0:
#                     query = f"INSERT INTO {table} (Date, {symbol}) VALUES (?, ?)"
#                     # if DEBUG: logger.debug(f"query: {query}")
#                     cursor.execute(query, (date, value))
#                 else:
#                     # query = f"UPDATE {table} SET {symbol} = ? WHERE Date = {date}", (value,)
#                     # if DEBUG: logger.debug(f"query: {query}")
#                     cursor.execute(f"UPDATE {table} SET {symbol} = ? WHERE Date = {date}", (value,))
#             except Exception as e:
#                 logger.debug(f"*** ERROR *** {e}")
#             else:
#                 conn.commit()
