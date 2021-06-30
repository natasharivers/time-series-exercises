import pandas as pd 
import requests 
import os

##################### GERMANY ENERGY FUNCTION #####################

def get_germany_data():
    '''
    This function creates a csv of germany energy data if one does not exist
    if one already exists, it uses the existing csv 
    and brings it into pandas as dataframe
    '''
    if os.path.isfile('opsd_germany_daily.csv'):
        df = pd.read_csv('opsd_germany_daily.csv', index_col=0)
    
    else:
        url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
        df = pd.read_csv(url)
        df.to_csv('opsd_germany_daily.csv')

    return df

############################# ITEMS FUNCTION #########################

def items_df():
    '''
    This function pulls in items data from the url provided
    and returns all pages in items as a Pandas DataFrame
    '''
    #create an empty items list
    items_list = []
    #grab data from the url
    url = "https://python.zach.lol/api/v1/items"

    #create the response
    response = requests.get(url)
    data = response.json()
    #identify keys desired
    n = data['payload']['max_page']

    #create for loop to pull in all pages available
    for i in range(1, n+1):
        new_url = url+ '?page=' + str(i)
        response = requests.get(new_url)
        data = response.json()
        page_items = data['payload']['items']
        items_list += page_items

    #create Pandas df and assign it to variable 'items'  
    items = pd.DataFrame.from_dict(items_list)
    
    return items


############################# STORES FUNCTION #########################

def stores_df():
    '''
    This function pulls in stores data from the url provided
    and returns all pages in stores as a Pandas DataFrame
    '''
    #create an empty stores list
    stores_list = []
    #grab data from the url
    url = "https://python.zach.lol/api/v1/stores"

    #create the response
    response = requests.get(url)
    data = response.json()
    #identify keys desired
    n = data['payload']['max_page']

    #create for loop to pull in all pages available
    for i in range(1, n+1):
        new_url = url+ '?page=' + str(i)
        response = requests.get(new_url)
        data = response.json()
        page_stores = data['payload']['stores']
        stores_list += page_stores
    
    #create Pandas df and assign it to variable 'stores'  
    stores = pd.DataFrame.from_dict(stores_list)
    
    return stores


############################# SALES FUNCTION #########################

def sales_df():
    '''
    This function pulls in sales data from the url provided
    and returns all pages in sales as a Pandas DataFrame
    '''
    #create an empty sales list
    sales_list = []
    #grab data from the url
    url = "https://python.zach.lol/api/v1/sales"

    #create the response
    response = requests.get(url)
    data = response.json()
    #identify keys desired
    n = data['payload']['max_page']

    #create for loop to pull in all pages available
    for i in range(1, n+1):
        new_url = url+ '?page=' + str(i)
        response = requests.get(new_url)
        data = response.json()
        page_sales = data['payload']['sales']
        sales_list += page_sales

    #create Pandas df and assign it to variable 'sales'
    sales = pd.DataFrame.from_dict(sales_list)
        
    return sales

############################# MERGE DATA FUNCTION #########################

def merge_data():
    '''
    This function merges items, stores, and sales dataframes
    and returns the completed merged df
    '''
    #identify the variables (from previous functions)
    items = items_df()
    stores = stores_df()
    sales = sales_df()

    #rename column to be merged on
    sales = sales.rename(columns={'item':'item_id'})
    #merge sales and items on 'item_id'
    merged_df = pd.merge(sales, items, on="item_id")

    #rename column to be merged on
    stores = stores.rename(columns={'store_id':'store'})
    #merge stores to already merged df on store column
    complete_df = pd.merge(merged_df, stores, on="store")

    return completed_df