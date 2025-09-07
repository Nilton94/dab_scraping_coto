import requests
import asyncio, aiohttp
from random_user_agent.user_agent import UserAgent
import json
import datetime, pytz
import polars as pl
import polars.selectors as cs
from dataclasses import dataclass
from ...utils.literals import URLTYPE
from ...utils.logger import get_logger

logger = get_logger()

@dataclass
class CotoScraper:
    url = 'https://api.cotodigital.com.ar/sitios/cdigi/categoria?&format=json&pushSite=CotoDigital'
    url_template = 'https://api.cotodigital.com.ar/sitios/cdigi/'
    url_params = {'general':'categoria?&format=json&pushSite=CotoDigital', 'items':'{category_path}?_No={page_number}&Nrpp={number_per_page}&format=json&pushSite=CotoDigital'}
    ua = UserAgent()
    user_agents = ua.get_random_user_agent()
    headers = {'user-agent': user_agents.strip(), 'encoding':'utf-8'}
    default_tz=pytz.timezone("America/Sao_Paulo")

    def get_general_urls(self):
        '''
            Return general data from the main page of groceries website Coto, like categories and subcategories domains.

            Args:
                self: Instance of the CotoScraper class.

            Returns:
                dict: A dictionary containing the JSON response from the website along with URL, URL parameters, timestamp, and headers.
        '''

        try:
            logger.info(f'Starting requests for url {self.url}!')
            r = requests.get(url = self.url, headers = self.headers)
            json_response = json.loads(r.text)

            json_response.update(
                {
                    "url": self.url_template,
                    "url_params": self.url_params,
                    "timestamp": str(datetime.datetime.now(tz=self.default_tz)),
                    "timezone": self.default_tz.zone,
                    "headers": self.headers
                }
            )
            logger.info(f'Success loading data from url {self.url}!')
            return json_response
        except Exception as e:
            logger.info(f'Error loading data from url {self.url}!\n{e}')
            return f'Erro: {e}'
        
    def parse_base_json(self):
        '''
            Parse the JSON response to extract department, category, and subcategory information along with URLs.

            Args:
                self: Instance of the CotoScraperTotalItems class.

            Returns:
                list: A list of dictionaries containing department, category, subcategory details, and URLs.
        '''
        logger.info(f'Starting parsing department json file')

        df = self.get_general_urls()
        
        data = [
            {
                "department_id": row['topLevelCategory'].get('categoryId', ''),
                "department_name": row['topLevelCategory'].get('displayName', ''),
                "department_url": row['topLevelCategory'].get('navigationState', ''),
                "category_id": cat.get('categoryId', ''),
                "category_name": cat.get('displayName', ''),
                "category_url": cat.get('navigationState', ''),
                "subcategory_id": subcat.get('categoryId', ''),
                "subcategory_name": subcat.get('displayName', ''),
                "subcategory_url": subcat.get('navigationState', ''),
                "url": df['url'],
                "url_params": df['url_params'],
                "headers": df['headers'],
                "timestamp": df['timestamp'],
                "timezone": df['timezone']
            }
            for row in df['contents'][0]['Left'][1]['contents'][0]['categories']
            for cat in row.get('subCategories', [])
            for subcat in cat.get('subCategories2', [])
        ]

        logger.info(f'Success parsing {len(data)} collections from department json file!')
        return data
        

@dataclass
class CotoScraperTotalItems(CotoScraper):

    async def __get_page_async_info(self, session: aiohttp.ClientSession, id: str, name: str, url: str, prefix: URLTYPE) -> dict:
        '''
            General function to retrieve website data asynchronously from a given URL.

            Params:
                session: aiohttp session
                id: department/category/subcategory id
                name: department/category/subcategory name
                url: department/category/subcategory url
                prefix: department, category or subcategory

            Returns:
                dict: A dictionary containing the id, name, url, extended url, number of items, and other metadata.
        '''

        extend_url = f"https://api.cotodigital.com.ar/sitios/cdigi/{url}?&format=json&pushSite=CotoDigital"
        
        logger.info(f'Starting requests for url {url}!')
        try:
            async with session.get(extend_url) as response:
                
                # Para pegar casos em que o path muda
                results = json.loads(await response.text())
                
                results.update(
                    {
                        f'{prefix}_id': id,
                        f'{prefix}_name': name,
                        f'{prefix}_url': url,
                        f'{prefix}_extended_url': extend_url,
                        'timestamp': str(datetime.datetime.now(tz=self.default_tz)),
                        'timezone': self.default_tz.zone,
                        'status_code': response.status
                    }
                )

                logger.info(f'Sucesses loading data from url {url} with status code {response.status}!')

        except Exception as e:
                try:
                    status_code = response.status
                except:
                    status_code = 404

                logger.error(f'Error loading data from url {url} with status code {status_code}!\n{e}')

                results = {
                    f'{prefix}_id': id,
                    f'{prefix}_name': name,
                    f'{prefix}_url': url,
                    f'{prefix}_extended_url': extend_url,
                    'timestamp': str(datetime.datetime.now(tz=self.default_tz)),
                    'timezone': self.default_tz.zone,
                    'status_code': status_code
                }
        
        return results
            

    async def get_total_items(self, prefix: URLTYPE = 'department') -> (list[dict] | str):
        '''
            Return the total number of items in a given prefix filtering from the parsed JSON data with all departments, categories and subcategories.

            Args:
                prefix: department, category or subcategory
            
            Returns:
                list[dict]: A list of dictionaries containing the id, name, url, extended url, number of items, and other metadata.
        '''
        logger.info(f'Starting processing total items from {prefix}!')

        try:
            df = self.parse_base_json()
            url_data = pl.DataFrame(df).select(cs.starts_with(f'{prefix}')).unique().to_dicts()

            results = []
            async with aiohttp.ClientSession(headers = self.headers) as session:
                tasks = [
                    self.__get_page_async_info(session = session, id = row[f'{prefix}_id'], name = row[f'{prefix}_name'], url = row[f'{prefix}_url'], prefix = prefix)
                    for row in url_data
                ]
                html_pages = await asyncio.gather(*tasks)
                results.extend(html_pages)
            
            # logger.info(f'Success processing {len(results)} items from {prefix}!')

            return results
        except Exception as e:
            logger.error(f'Error loading total items for {prefix}!\n{e}')
            return f'Erro: {e}'
        

    async def parse_item_json(self, prefix: URLTYPE = 'department') -> list[dict]:
        '''
            Parse the JSON response to extract the total number of items for each department, category, or subcategory.

            Args:
                prefix: department, category or subcategory

            Returns:
                list: A list of dictionaries containing the id, name, url, extended url, number of items, and other metadata.
        '''
        
        logger.info(f'Starting parsing {prefix} json!')

        data = await self.get_total_items(prefix=prefix)
        
        results = []
        
        for row in data:
            try:
                index = [idx for idx, row in enumerate(row['contents'][0]['Main']) if 'contents' in row.keys()][0]
                items = row['contents'][0]['Main'][index]['contents'][0]['totalNumRecs']

                item =  {
                    f'{prefix}_id': row[f'{prefix}_id'],
                    f'{prefix}_name': row[f'{prefix}_name'],
                    f'{prefix}_url': row[f'{prefix}_url'],
                    f'{prefix}_extended_url': row[f'{prefix}_extended_url'],
                    f"{prefix}_items": items,
                    f"{prefix}_no": 0,
                    f"{prefix}_nrpp": items,
                    f"{prefix}_final_url": [f"https://api.cotodigital.com.ar/sitios/cdigi/{row[f'{prefix}_url']}?_No={i}&Nrpp=999&format=json" for i in range(0, items, 999)],
                    "timestamp": row['timestamp'],
                    "timezone": row['timezone'],
                    "status_code": row['status_code']
                }

                logger.info(f'[{prefix}] - sucess processing item {row[f"{prefix}_name"]} with status code {row["status_code"]}')
                results.append(item)
            
            except Exception as e:
                logger.error(f'[{prefix}] - error processing item!\n{e}')

                item =  {
                    f'{prefix}_id': row[f'{prefix}_id'],
                    f'{prefix}_name': row[f'{prefix}_name'],
                    f'{prefix}_url': row[f'{prefix}_url'],
                    f'{prefix}_extended_url': row[f'{prefix}_extended_url'],
                    f"{prefix}_items": None,
                    f"{prefix}_no": 0,
                    f"{prefix}_nrpp": None,
                    f"{prefix}_final_url": None,
                    "timestamp": row['timestamp'],
                    "timezone": row['timezone'],
                    "status_code": row['status_code']
                }

                results.append(item)

        logger.info(f'Finish processing {len(results)} collections from {prefix}!')
        return results
    

class CotoScraperItems(CotoScraperTotalItems):
    
    async def __get_item_async_info(self, session: aiohttp.ClientSession, id: str, url: str):
        try:
            async with session.get(url) as response:
                
                logger.info(f'[{id}] - Getting data from {url}!')

                results = json.loads(await response.text())
                
                results.update(
                    {
                        'id': id,
                        'url': url,
                        'timestamp': str(datetime.datetime.now(tz=self.default_tz)),
                        'timezone': self.default_tz.zone,
                        'status_code': response.status
                    }
                )

                logger.info(f'[{id}] - Data loaded from {url} with status code {response.status}!')

        except Exception as e:
                try:
                    status_code = response.status
                except:
                    status_code = 404

                logger.error(f'[{id}] - Error getting data from {url}, status code {status_code}!\n{e}')

                results = {
                    'id': id,
                    'url': url,
                    'timestamp': str(datetime.datetime.now(tz=self.default_tz)),
                    'timezone': self.default_tz.zone,
                    'status_code': status_code
                }
        
        return results
    
    async def get_items_info(self):

        
        try:
            logger.info(f'Starting async information extraction from items!')
            data = await self.parse_item_json(prefix='subcategory')
            adjusted_data = [
                {
                    'id': item['subcategory_id'], 
                    'urls': [url for url in item['subcategory_final_url']]
                }
                for item in data
                if all(
                    [
                        item['subcategory_final_url'] != None, 
                        item['subcategory_final_url'] != []
                    ]
                )
            ]

            results = []
            async with aiohttp.ClientSession(headers = self.headers) as session:
                tasks = [
                    self.__get_item_async_info(session = session, id = row['id'], url = url)
                    for row in adjusted_data
                    for url in row['urls']
                ]
                response = await asyncio.gather(*tasks)
                results.extend(response)

            logger.info(f'Succesfully loaded informations from {len(results)} items!')

            return results
        except Exception as e:
            logger.error(f'Error when tried to load information from items!\n{e}')
            return f'Erro: {e}'