import requests
import asyncio, aiohttp
from random_user_agent.user_agent import UserAgent
import json
import datetime, pytz
import polars as pl
import polars.selectors as cs
from dataclasses import dataclass
from ...utils.literals import URLTYPE

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

            return json_response
        except Exception as e:
            return f'Erro: {e}'
        
    def parse_base_json(self):
        '''
            Parse the JSON response to extract department, category, and subcategory information along with URLs.

            Args:
                self: Instance of the CotoScraperTotalItems class.

            Returns:
                list: A list of dictionaries containing department, category, subcategory details, and URLs.
        '''
        
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

        except:
                try:
                    status_code = response.status
                except Exception as e:
                    status_code = f'Erro: {e}'

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
            
            return results
        except Exception as e:
            return f'Erro: {e}'
        

    async def parse_item_json(self, prefix: URLTYPE = 'department') -> list[dict]:
        '''
            Parse the JSON response to extract the total number of items for each department, category, or subcategory.

            Args:
                prefix: department, category or subcategory

            Returns:
                list: A list of dictionaries containing the id, name, url, extended url, number of items, and other metadata.
        '''
        
        
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

                results.append(item)
            
            except:
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
        
        return results