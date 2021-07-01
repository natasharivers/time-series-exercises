import pandas as pd 
import requests 
import os
import numpy as np

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
    sales_df = pd.DataFrame.from_dict(sales_list)

    #create a csv from that data
    sales_df.to_csv('sales.csv')

    return sales_df

############################ Store CSV Function ##############################

def sales_df_file():
    if os.path.isfile('sales.csv'):
        df = pd.read_csv('sales.csv', index_col=0)
    
    else:
        df = sales_df()
        df.to_csv('sales.csv')
    
    return df

############################# Data to CSV ####################################

def items_csv():
    '''
    This function stores the grocery items locally as a .csv
    '''
    items = items_df()
    return items.to_csv('items.csv')

def stores_csv():
    '''
    This function stores the grocery stores locally as a .csv
    '''
    stores = stores_df()
    return stores.to_csv('stores.csv')


############################# MERGE DATA FUNCTION #########################

def all_store_data():
    '''
    This function uses a csv file of merged items, stores, and sales dataframes if one exists
    if one does not exist, it is created
    and returns the completed merged df
    '''
    if os.path.isfile('allstoredata.csv'):
        df = pd.read_csv('allstoredata.csv', index_col=0)
    
    else:
        items_df = pd.read_csv('items.csv')
        stores_df = pd.read_csv('stores.csv')
        sales_df = pd.read_csv('sales.csv')
        sales_stores_df = pd.merge(sales_df, stores_df, left_on='store', right_on='store_id', how='left')
        sales_stores_items_df = pd.merge(sales_stores_df, items_df, left_on='item', right_on='item_id', how='left')

        sales_stores_items_df.to_csv('allstoredata.csv')

        return sales_stores_items_df