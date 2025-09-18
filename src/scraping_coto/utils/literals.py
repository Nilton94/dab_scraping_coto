from typing import Literal

LOGLEVEL=Literal['debug','info','warning','error','critical']
JSONMODE=Literal['r','rb','w','wb']
URLTYPE=Literal['department','category','subcategory']
FOLDERTYPE=Literal['departments','categories','subcategories','items']
READMODE=Literal['all','last date']