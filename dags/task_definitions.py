import unicodedata
from datetime import datetime

import pandas as pd
from playwright.sync_api import sync_playwright

from resources.logger import LoggingClass
from resources.constructors import make_path
from resources.scrapers import PlaywrightScraper


class ScrapeSpecifications(LoggingClass):
    def __init__(self, **configs):
        super().__init__()
        self.domain = configs['domain']
        self.specs_selector = configs['specs_selector']   

    def _handle_url(self, url):
        if self.domain not in url:
            return f'{self.domain}{url}'
        else:
            return url

    def scrape(self, urls):
        urls = set(urls)
        self.info(f'Number of urls to be requested: {len(urls)}')
        specs = []
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            page = context.new_page()
            for url in set(urls):
                self.info(f'requesting url: {url}')
                page.goto(url, timeout=0)
                box_text = page.query_selector(self.specs_selector)
                text = box_text.text_content()
                text = text.replace('\t', '')
                specs.append((url, text))
                self.info(text)
            context.close()
            browser.close()
        return pd.DataFrame(specs, columns=['url', 'specs'])

class ScrapingProcess(LoggingClass):
    REPLACES = {
        ' EM ': ' ',
        ':': '',
        'FEMININA': 'FEMININO',
        'FEMENINO': 'FEMININO',
        'LONA': 'NAPA',
        'MA...': 'MASCULINO',
        'MASC...': 'MASCULINO',
        'MASCU...': 'MASCULINO',
        'MASCULI...': 'MASCULINO',
        'MASCULIN...': 'MASCULINO',
        'MASCULINA': 'MASCULINO',
        'MASCULINO...': 'MASCULINO',
        'MENINA': 'FEMININO',
        'MENINO': 'MASCULINO',
        'NOBUC...': 'NOBUCK',
        'OLIMPIKUS': 'OLYMPIKUS', 
        'RASTEIRA': 'SANDALIA',
        'SAPATO': '',
        'UNISSEX': 'FEMININO MASCULINO',
    }

    def __init__(self, config, logger=None) -> None:
        self.config = config
        self.date = datetime.today()
        super().__init__(logger=logger)

    def prepare(self, **context):
        self.info("Creating path")
        output_path = make_path(
            root=f'./data_output/{self.config["name"]}/', add=['prints', 'prints/pesquisa']
        )

        self.info("Reading csv file")
        df = pd.read_csv("./data_input/querys.csv", dtype=str)

        self.info(f"Queries to be scraped: {df}")

        context["ti"].xcom_push(key="output_path", value=output_path)
        df.to_parquet(f"{output_path}/raw_queries.parquet")
    
    def collect(self, **context):
        output_path = context["ti"].xcom_pull(
            key="output_path", task_ids="prepare"
        )
        df = pd.read_parquet(f'{output_path}/raw_queries.parquet')

        scraper = PlaywrightScraper(**self.config)

        dfs = []
        for i, item in df.iterrows():
            self.info(f'Searching query: {item["query"]}')
            dfs.append(
                scraper.scrape(item['query'], i, f'{output_path}/prints/pesquisa')
            )

        scraped = pd.concat(dfs, ignore_index=True)
        self.info(f"Collected Dataframe: {scraped}")
        scraped.to_parquet(f'{output_path}/data_scraped.parquet')

    def scrape_specifications(self, **context):
        output_path = context["ti"].xcom_pull(
            key="output_path", task_ids="prepare"
        )
        df = pd.read_parquet(f'{output_path}/data_scraped.parquet')
        specs = self._scrape_specifications(df)
        specs.to_parquet(f'{output_path}/data_specs.parquet')

    def validade_collected_items(self, **context):
        output_path = context["ti"].xcom_pull(
            key="output_path", task_ids="prepare"
        )
        data_collected = pd.read_parquet(
            f'{output_path}/data_specs.parquet'
        )
        data_collected = self._filter_valid_items(data_collected)
        data_collected.to_parquet(f'{output_path}/data_filtered.parquet')

    def print_items(self, **context):
        output_path = context["ti"].xcom_pull(
            key="output_path", task_ids="prepare"
        )
        df = pd.read_parquet(f'{output_path}/data_filtered.parquet')
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            page = context.new_page()
            for i, item in df.iterrows():
                url = ScrapeSpecifications(**self.config)._handle_url(item['url'])
                self.info(f'requesting url: {url}')
                page.goto(url, timeout=0)
                page.screenshot(
                    path=f'{output_path}/prints/{i}_{self.date.strftime("%Y%m%d")}.png',
                    full_page=True,
                    timeout=0,
                )

    def export(self, **context):
        output_path = context["ti"].xcom_pull(
            key="output_path", task_ids="prepare"
        )
        df = pd.read_parquet(f"{output_path}/data_filtered.parquet")
        df = df[['title', 'price', 'url', 'date', 'query']]
        df.to_excel(f'{output_path}/data_collected.xlsx', index=False)

    def _normalize_string(self, string):
        text = unicodedata.normalize('NFKD', string)
        text = text.encode('ascii', 'ignore')
        return text.decode('utf-8')

    def _replace_descriptions(self, descs):
        aux = []
        for desc in descs:
            desc = desc.upper()
            for k, v in self.REPLACES.items():
                desc = desc.replace(k, v)
            aux.append(desc)
        return aux

    def _filter_valid_items(self, items):
        if items.shape[0] == 0:
            return items
        items['description'] = [
            f'{item["title"]} {item["specs"]}' for _, item in items.iterrows()
        ]
        items['description'] = [
            self._normalize_string(text) for text in items['description']
        ]
        items['query'] = [
            self._normalize_string(text) for text in items['query']
        ]
        items['description'] = self._replace_descriptions(
            list(items['description'])
        )
        items['query'] = self._replace_descriptions(list(items['query']))
        items = items.drop_duplicates('url')
        list_of_index = []
        for i, item in items.iterrows():
            query_set = set(item['query'].split())
            description_set = set(item['description'].split())
            self.info(f'URL {item["url"]}')
            self.info(f'Query set {query_set}')
            self.info(f'Desc set {description_set}')
            if query_set.issubset(description_set):
                self.info('IS A VALID ITEM')
                list_of_index.append(i)
            else:
                diff = {i for i in query_set if i not in description_set}
                self.info(f'Difference {diff}')
                self.info('INVALID ITEM')
            self.info('------------------------')
        items = items.loc[list_of_index]
        return items.reset_index()

    def _scrape_specifications(self, df):
        scraper = ScrapeSpecifications(**self.config)
        df['url'] = [scraper._handle_url(url) for url in df['url']]
        specs = scraper.scrape(list(df['url']))
        merged = pd.merge(df, specs, how='left')
        self.info(f'dataframe with specifications: {merged}')
        return merged.reset_index()