"""src/pkg/chart_srv/scraper/stock_chart.py\n
Use selenium webdriver get base_url update chart size,\n
color, and RSI indicator. Return new base url then use\n
urllib3 to get image source for stock symbol and period.\n
Save image to work directory.
"""

import io, os
import logging, logging.config
from time import sleep
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import urllib3

from PIL import Image

# TODO add Chrome/Firefox option
# from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    TimeoutException,
)
from pkg import DEBUG


logging.config.fileConfig(fname="src/logger.ini")
logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class WebScraper:
    """Fetch and save stockcharts from stockcharts.com"""

    def __init__(self, ctx):
        self.base_url = ctx["chart_service"]["url_stockchart"]
        self.chart_dir = f"{ctx['default']['work_dir']}chart"
        self.http = urllib3.PoolManager()
        self.period = ctx["interface"]["opt_trans"]
        self.symbol = ctx["interface"]["arguments"]

    def __repr__(self):
        return f"<class '{self.__class__.__name__}'> __dict__= {self.__dict__})"

    def webscraper(self):
        """Main entry point to class. Directs workflow of webscraper."""
        if DEBUG:
            logger.debug(f"webscraper(self={self})")

        # opt = ChromeOptions()
        opt = FirefoxOptions()
        opt.add_argument("--headless=new")
        # opt.add_argument(
        #     "--user-agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'"
        # )
        opt.add_argument("--user-agent='Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0'")
        # opt.page_load_strategy = "eager"
        opt.page_load_strategy = "none"
        # driver = Chrome(options=opt)
        driver = Firefox(options=opt)
        driver.get(self.base_url)

        try:
            self._set_indicator_RSI(driver=driver)
            self._set_chart_size_landscape(driver=driver)
            self._set_chart_color_dark(driver=driver)
            # self._click_update_button(driver=driver)
            self.url = self._get_chart_src_attribute(driver=driver)
            self._fetch_stockchart(url=self.url)
        except (ElementClickInterceptedException, ElementNotInteractableException, TimeoutException, Exception) as e:
            logger.debug(f"*** ERROR *** {e}")
        finally:
            driver.quit()

    def _click_update_button(self, driver: object):
        """click refresh chart"""
        try:
            button = WebDriverWait(driver=driver, timeout=10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "/html/body/div[1]/div[2]/div[5]/div[2]/div[1]/div[1]/div[3]/button[1]")
                )
            )
            loc = button.location_once_scrolled_into_view
        except Exception as e:
            logger.debug(
                f"_click_update_button(self, driver)\nExpected condition element to be clickable not met.\n{e} Trying alternate XPATH.\n"
            )
            try:
                button = WebDriverWait(driver=driver, timeout=10).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "/html/body/div[1]/div[2]/div[6]/div[2]/div[1]/div[1]/div[3]/button[1]")
                    )
                )
                loc = button.location_once_scrolled_into_view
            except Exception as e:
                logger.debug(f"_click_update_button(self={self}, driver={driver}) {e}")
        finally:
            if DEBUG:
                logger.debug(f"button: {button}, loc: {loc}")
            button.click()

    def _fetch_stockchart(self, url: str) -> object:
        """modify chart url, get image source and save"""
        if DEBUG:
            logger.debug(f"_fetch_stockchart(url={url})")

        for symbol in self.symbol:
            for period in self.period:
                if not DEBUG:
                    print(f"  fetching {symbol} {period}...")
                mod_url = self._modify_query_period_and_symbol(period=period, symbol=symbol)
                self._get_img_src_convert_bytes_to_png_and_save(url=mod_url, period=period, symbol=symbol)

    def _get_chart_src_attribute(self, driver: object) -> str:
        """modify base_url, set size, color, and RSI indicator, return modified base_url"""
        sleep(1)
        try:
            img_element = WebDriverWait(driver=driver, timeout=10).until(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        "#chart-image",
                        # By.XPATH,
                        # '//*[@id="chart-image"]'
                    )
                )
            )
            loc = img_element.location_once_scrolled_into_view
            if DEBUG:
                logger.debug(f"img_element: {img_element}, loc: {loc}")
            return img_element.get_attribute("src")

        except Exception as e:
            logger.debug(f"an error occured while getting chart source url: {e}")
            url = input(" Input chart source url manually or press enter to exit:")
            if url:
                return url
            else:
                SystemExit

    def _get_img_src_convert_bytes_to_png_and_save(self, url: str, period: str, symbol: str):
        """Get the chart image source and convert the bytes to
        a .png image then save to the chart work directory.
        """
        if DEBUG:
            logger.debug(f"_get_img_src_convert_bytes_to_png_and_save(url={url} {type(url)})")

        image_src = self.http.request("GET", url, headers={"User-agent": "Mozilla/5.0"})
        image = Image.open(io.BytesIO(image_src.data)).convert("RGB")
        image.save(os.path.join(self.chart_dir, f"{symbol}_{period[:1].lower()}.png"), "PNG", quality=80)

    def _modify_query_period_and_symbol(self, period: str, symbol: str) -> str:
        """Use urllib.parse to modify the default query parameters
        with new period, symbol.
        """
        if DEBUG:
            logger.debug(
                f"_modify_query_period_and_symbol(period={period} {type(period)}, symbol={symbol} {type(symbol)})"
            )

        parsed_url = urlparse(url=self.url)
        query_dict = parse_qs(parsed_url.query)
        if period != "Daily":
            query_dict["p"] = period[0]
            query_dict["yr"] = "5"
        query_dict["s"] = symbol
        encoded_params = urlencode(query_dict, doseq=True)

        return urlunparse(parsed_url._replace(query=encoded_params))

    def _set_chart_color_dark(self, driver: object):
        """set color to night"""
        color_element = WebDriverWait(driver=driver, timeout=10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="chart-settings-color-scheme-menu"]'))
        )
        loc = color_element.location_once_scrolled_into_view
        if DEBUG:
            logger.debug(f"color_element: {color_element}, loc: {loc}")

        color = Select(color_element)
        color.select_by_value("night")
        if DEBUG:
            logger.debug(f"color: {color}")

    def _set_indicator_RSI(self, driver: object):
        """set indicator overlay toRSI"""
        indicator_element = WebDriverWait(driver=driver, timeout=10).until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    "#indicator-menu-1",
                    # By.XPATH,
                    # '//*[@id="indicator-menu-1"]'
                )
            )
        )
        loc = indicator_element.location_once_scrolled_into_view
        if DEBUG:
            logger.debug(f"indicator_element: {indicator_element}, loc: {loc}")

        indicator = Select(indicator_element)
        indicator.select_by_value("RSI")
        if DEBUG:
            logger.debug(f"indicator: {indicator}")

    def _set_chart_size_landscape(self, driver: object):
        """set chart size to Landscape"""
        size_element = WebDriverWait(driver=driver, timeout=10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="chart-settings-chart-size-menu"]'))
        )
        loc = size_element.location_once_scrolled_into_view
        if DEBUG:
            logger.debug(f"size_element: {size_element}, loc: {loc}")

        size = Select(size_element)
        size.select_by_value("Landscape")
        if DEBUG:
            logger.debug(f"size: {size}")
