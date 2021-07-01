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

#######################################################################################
    ##ACQUIRE FUNCTIONS
############################# ITEMS ACQUIRE FUNCTION #########################

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


############################# STORES ACQUIRE FUNCTION #########################

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


############################# SALES ACQUIRE FUNCTION #########################

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

#######################################################################################
## CSV FUNCTIONS
############################ STORE CSV  ##############################

def sales_csv():
    if os.path.isfile('sales.csv'):
        df = pd.read_csv('sales.csv', index_col=0)
    
    else:
        df = sales_df()
        df.to_csv('sales.csv')
    
    return df

############################# ITEMS CSV ####################################

def items_csv():
    '''
    This function stores the grocery items locally as a .csv
    '''
    items = items_df()
    return items.to_csv('items.csv')

############################# STORES CSV ####################################


def stores_csv():
    '''
    This function stores the grocery stores locally as a .csv
    '''
    stores = stores_df()
    return stores.to_csv('stores.csv')


#######################################################################################
##MERGE FUNCTIONS
############################# NEW Groceries FUNCTION #########################

def new_data():
    '''
    This function takes in 3 seperate csv files
    and merges them together
    then returns a merged pandas dataframe with that data
    '''
    items_df = pd.read_csv('items.csv')
    stores_df = pd.read_csv('stores.csv')
    sales_df = pd.read_csv('sales.csv')
    sales_stores_df = pd.merge(sales_df, stores_df, left_on='store', right_on='store_id', how='left')
    sales_stores_items_df = pd.merge(sales_stores_df, items_df, left_on='item', right_on='item_id', how='left')
    sales_stores_items_df = sales_stores_items_df.drop(columns=['Unnamed: 0_x', 'Unnamed: 0_y', 'Unnamed: 0'])
    
    return sales_stores_items_df

############################# MERGE DATA FUNCTION #########################

def all_store_data():
    '''
    This function uses a csv file of merged store data
    if one does not exist, it is created
    and returns the completed merged df
    '''
    if os.path.isfile('allstoredata.csv'):
        df = pd.read_csv('allstoredata.csv', index_col=0)
    
    else:
        df = new_data()
        df.to_csv('allstoredata.csv')

    return df


