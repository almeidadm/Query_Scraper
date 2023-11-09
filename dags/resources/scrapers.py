import time
import pandas as pd
import numpy as np
import logging
from abc import ABC, abstractmethod
from datetime import datetime

from playwright.sync_api import sync_playwright


class QScraper(ABC):

    def __init__(
            self,
            search_url: str,
            element_selector: str,
            title_selector: str,
            price_selector: str,
            url_selector: str,
            type_element: str = "css selector",
            **kwargs,
    ):
        self.search_url = search_url
        self.element_selector = element_selector
        self.title_selector = title_selector
        self.price_selector = price_selector
        self.url_selector = url_selector
        self.type_element = type_element
        self.date = datetime.today()

    @abstractmethod
    def _locate_elements(self, selector, href: bool = False):
        pass

    @abstractmethod
    def _goto(self, url):
        pass

    def _iteration(self, query):
        query = query.replace(' ', '+')
        query = query.lower()
        url = self.search_url.replace("{query}", query)
        if "{page}" in url:
            i = 1
            while i < 20:
                yield url.replace("{page}", str(i))
                i += 1
        else:
            yield url

    def scrape(self, query):

        df = pd.DataFrame({"title": [], "price": [], "url": []})
        for current_url in self._iteration(query):
            logging.info(f'Current URL: {current_url}')
            self._goto(current_url)

            titles = self._locate_elements(self.title_selector)
            prices = self._locate_elements(self.price_selector)
            urls = self._locate_elements(self.url_selector, href=True)

            if len(prices) == 0:
                break

            limit = min([len(li) for li in [titles, prices, urls]])
            aux = pd.DataFrame({"title": titles[:limit], "price": prices[:limit], "url": urls[:limit]})

            df = pd.concat([df, aux], ignore_index=True)
        
        df['date'] = self.date.strftime('%d/%m/%Y')
        df['query'] = query
        return df


class PlaywrightScraper(QScraper):
    def __init__(
            self,
            search_url: str,
            element_selector: str,
            title_selector: str,
            price_selector: str,
            url_selector: str,
            type_element: str = "css selector",
            **kwargs
    ):
        super().__init__(
            search_url,
            element_selector,
            title_selector,
            price_selector,
            url_selector,
            type_element,
            **kwargs,
        )
        self._name = None
        self._path = None

    def _locate_elements(self, selector, href: bool = False):
        try:
            elements = self.page.query_selector_all(selector)
            if href:
                elements = [e.get_attribute("href") for e in elements]
            else:
                elements = [e.text_content().strip() for e in elements]
        except Exception as e:
            logging.error(f'_locate_elements: {e}')
            elements = []
        return elements

    def _goto(self, url):
        self.page.goto(url, timeout=0)
        path = f'{self._path}/{self._name}_{self.date.strftime("%d%m%Y")}.png'
        self.page.screenshot(path=path, full_page=True, timeout=0)
        self.page.keyboard.press('End')

    def scrape(self, query, id_item, path):
        self._name = id_item
        self._path = path
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            self.page = context.new_page()
            df = super().scrape(query)
            context.close()
            browser.close()
        df['id_item'] = id_item
        logging.info(f'number of collected prices: {df.shape[0]}')
        return df
 