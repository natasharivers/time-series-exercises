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

#################################################################

######################### Helper function used in create big_df ########################################

def get_df(name):
    """
    This function takes in the string
    'items', 'stores', or 'sales' and
    returns a df containing all pages and
    creates a .csv file for future use.
    """
    base_url = 'https://python.zach.lol'
    api_url = base_url + '/api/v1/'
    response = requests.get(api_url + name)
    data = response.json()
    
    # create list from 1st page
    my_list = data['payload'][name]
    
    # loop through the pages and add to list
    while data['payload']['next_page'] != None:
        response = requests.get(base_url + data['payload']['next_page'])
        data = response.json()
        my_list.extend(data['payload'][name])
    
    # Create DataFrame from list
    df = pd.DataFrame(my_list)
    
    # Write DataFrame to csv file for future use
    df.to_csv(name + '.csv')
    return df

############################# MERGE DATA FUNCTION #########################

def merged_data():
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

############################# STORE DATA FUNCTION #########################

def get_store_data():
    """
    This function checks for csv files
    for items, sales, stores, and big_df 
    if there are none, it creates them.
    It returns one big_df of merged dfs.
    """
    # check for csv files or create them
    if os.path.isfile('items.csv'):
        items_df = pd.read_csv('items.csv', index_col=0)
    else:
        items_df = get_df('items')
        
    if os.path.isfile('stores.csv'):
        stores_df = pd.read_csv('stores.csv', index_col=0)
    else:
        stores_df = get_df('stores')
        
    if os.path.isfile('sales.csv'):
        sales_df = pd.read_csv('sales.csv', index_col=0)
    else:
        sales_df = get_df('sales')
        
    if os.path.isfile('big_df.csv'):
        df = pd.read_csv('big_df.csv', index_col=0)
        return df
    else:
        # merge all of the DataFrames into one
        df = pd.merge(sales_df, stores_df, left_on='store', right_on='store_id').drop(columns={'store'})
        df = pd.merge(df, items_df, left_on='item', right_on='item_id').drop(columns={'item'})

        # write merged DateTime df with all data to directory for future use
        df.to_csv('big_df.csv')
        return df