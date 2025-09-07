import json
import os
import re
from .literals import JSONMODE, READMODE, FOLDERTYPE
import datetime, pytz
# from pathlib import Path

def flatten_json(json: json = None, search_key = None, search_item = None, list_length = None):
    
    results = {}
    global search_path
    search_path = {'path':[], 'list_len':[]}

    def flatten(x, name='['):
        if type(x) is dict:
            # Iterando sobre todas as keys do dicionário
            for a in x:
                flatten(x[a], name + a + '][')
        elif type(x) is list:
            # Iterando sobre todos os elementos da lista
            i = 0
            if list_length: search_path.update({'path': x, 'list_len': len(x)}) if len(x) >= list_length else search_path
            for a in x:
                flatten(a, name + str(i) + '][')
                i += 1
        else:
            results[name[:-1]] = x

    flatten(json)
    
    if search_key:
        return {k:v for k, v in results.items() if search_key != None and search_key in k}
    elif search_item:
        return {k:v for k, v in results.items() if search_item != None and search_item in results[k]}
    elif list_length:
        return search_path
    else:
        return results

def loader(file: json = None, file_path: str = None, file_name: str = None, mode: JSONMODE = 'w'):
    '''
        Function to load json files

        Args:
            file: json object
            file_path: path to save the json file
            file_name: name of the json file
            mode: file mode, default is 'w' (write)

        Returns:
            str: success message or error message
    '''

    dt_str = str(datetime.datetime.now(tz=pytz.timezone('America/Sao_Paulo')).date())
    file_path = file_path if file_path else './'
    file_name = file_name if file_name else 'data'
    file_path_final = f'{file_path}/{dt_str}_{file_name}.json'

    try:
        with open(file_path_final, mode) as f:
            try:
                f.write(data)
            except:
                data = json.dumps(obj=file, ensure_ascii=False, indent=4)
                f.write(data)
            
    except Exception as e:
        return f'Error: {e}'
    
def reader(file_folder: str = None, read_mode: READMODE = 'last date', folder_type: FOLDERTYPE = 'departments'):
    '''
        Function to read json files

        Args:
            file_folder: path to read the json files
            read_mode: read mode, default is 'last date'

        Returns:
            json: json object or error message
    '''

    DATE_REGEX = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
    list_files = os.listdir(file_folder) if file_folder else os.listdir('./')
    last_date = [re.compile(DATE_REGEX).match(file) for file in list_files]
    last_date.sort(reverse=True)

    try:
        if read_mode == 'last date':
            file_path = f'{file_folder}/{last_date[0]}_{folder_type}.json'

            with open(file_path, 'r') as f:
                data = json.load(f)

        else:
            data = []
            for file in list_files:
                with open(f'{file_folder}/{file}', 'r') as f:
                    data.append(json.load(f))

        return data
    
    except Exception as e:
        return f'Error: {e}'