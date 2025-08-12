"""src/pkg/chart_srv/client.py\n
begin_chart_download(ctx) - fetch charts/heatmaps
"""

import logging

from pathlib import Path

from pkg import DEBUG


logger = logging.getLogger(__name__)


def begin_chart_download(ctx):
    """Check if `chart` or `heatmap` folder exists. Direct workflow of client"""
    if DEBUG:
        logger.debug(f"begin_chart_download(ctx={ctx})")

    command = ctx["interface"]["command"]
    # check 'heatmap' folder exists in users 'work_dir', if not create 'heatmap' folder
    Path(f"{ctx['default']['work_dir']}/{command}").mkdir(parents=True, exist_ok=True)

    if not DEBUG:
        print("\n Begin download")
    _download(ctx=ctx)

    if not DEBUG:
        print(" Finished!")
    if not DEBUG:
        print(f" Saved {command}s to '{ctx['default']['work_dir']}{command}'\n")


def _download(ctx):
    """Direct download to chart or heatmap"""
    if DEBUG:
        logger.debug(f"_download(ctx={type(ctx)})")

    # Select which version of the webscraper to use
    if ctx["interface"]["command"] == "chart":
        from pkg.chart_srv.scraper.stock_chart import WebScraper
    elif ctx["interface"]["command"] == "heatmap":
        from pkg.chart_srv.scraper.heat_map import WebScraper

    start = WebScraper(ctx)
    try:
        start.webscraper()
    except Exception as e:
        print(e)
