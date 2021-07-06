# tabular data manipulation
import numpy as np
import pandas as pd
# datetime utilities
from datetime import timedelta, datetime
# visualization
import matplotlib.pyplot as plt

# no yelling in the library
import warnings
warnings.filterwarnings("ignore")

# our acquire script
import acquire 

import requests 
import os

####################################################################################
################################## STORE FUNCTIONS #################################
####################################################################################


############################# CHANGE DTYPE FUNCTION #############################

def change_dtype():
    '''
    This function takes in a df then changes date dtypes
    and returns that as a new pandas dataframe with changed dtype
    '''
    df= acquire.all_store_data()

    #change data type on sale_date
    df.sale_date = df.sale_date.astype('datetime64[ns]')

    return df

############################# RESET INDEX  FUNCTION #############################

def reset_index():
    '''
    This function takes in a df then resets the index to sale_date
    and returns that as a new pandas dataframe with corrected index
    '''
    df= acquire.all_store_data()

    #reset sale_date as index
    df = df.set_index('sale_date').sort_index()

    return df

############################# ADD MONTH FUNCTION #############################

def create_month():
    '''
    This function takes in a df adds a new column for transaction month
    and returns that as a pandas dataframe with the new column
    '''
    df= acquire.all_store_data()

    #create new colum for month
    df['month'] = df.index.month_name()

    return df

############################# ADD WEEKDAY FUNCTION #############################

def create_weekday():
    '''
    This function takes in a df adds a new column for transaction weekday
    and returns that as a pandas dataframe with the new column
    '''
    df= acquire.all_store_data()

    #create new colum for weekday
    df['day_of_week'] = df.index.day_name()

    return df


############################# ADD SALES TOTAL FUNCTION #############################

def create_sales_total():
    '''
    This function takes in a df adds a new column for total sales 
    using sale amount and item price
    and returns that as a pandas dataframe with the new column
    '''
    df= acquire.all_store_data()

    #create new colum for total sales
    df['sales_total'] = df.sale_amount * df.item_price

    return df

############################# STORE PREP FUNCTION #############################

def prep_store():
    '''
    This function takes in a df and changes date dtypes, resets date as index,
    creates new columns for month, weekday, and total sales
    and returns that as a new pandas dataframe
    '''
    if os.path.isfile('prep_store.csv'):
        df = pd.read_csv('prep_store.csv', index_col=0)

        #assign variable df to acquire function
        df= acquire.all_store_data()

        #change data type on sale_date
        df.sale_date = df.sale_date.astype('datetime64[ns]')
        #reset sale_date as index
        df = df.set_index('sale_date').sort_index()

        #create new colum for month
        df['month'] = df.index.month_name()
        #create new colum for weekday
        df['day_of_week'] = df.index.day_name()
        #create new colum for sale total
        df['sales_total'] = df.sale_amount * df.item_price

    else:
        df = prep_store()
        df.to_csv('prep_store.csv')

    return df


####################################################################################
################################## GERMANY FUNCTIONS ###################################
####################################################################################

############################ GERMANY DTYPE FUNCTION #############################

def germany_dtypes():
    '''
    This function changes the datatype of Date to 'datetime64'
    and returns a pandas Dataframe with the corrected dtype
    '''
    df = acquire.get_germany_data()

    #change data type on Date
    df.Date = df.Date.astype('datetime64[ns]')

    return df

############################ GERMANY RESET INDEX FUNCTION #############################

def germany_index_reset():
    '''
    This function resets the index to Date
    and returns a pandas Dataframe with the index changed
    '''
    df = acquire.get_germany_data()

    #reset Date as index
    df = df.set_index('Date').sort_index()

    return df

############################ GERMANY MONTH FUNCTION #############################

def germany_month():
    '''
    This function creates a new column for month of record
    and returns a pandas Dataframe with the the new column
    '''
    df = acquire.get_germany_data()

    #create new colum for month
    df['month'] = df.index.month_name()

    return df

############################ GERMANY NULLS FUNCTION #############################

def germany_null():
    '''
    This function creates fills nulls with 0
    and returns a pandas Dataframe with the the new column
    '''
    df = acquire.get_germany_data()

    #fill nulls
    germany.fillna(0)

    return df

############################ GERMANY YEAR FUNCTION #############################

def germany_year():
    '''
    This function creates a new column for year of record
    and returns a pandas Dataframe with the the new column
    '''
    #create new colum for weekday
    df['year'] = df.index.year

    return df

############################ GERMANY PREP FUNCTION #############################

def prep_germany():
    '''
    This function takes in a df and changes date dtypes, resets date as index,
    creates new columns for month, year, fills null values
    and returns that as a new pandas dataframe
    '''
    if os.path.isfile('prep_germany.csv'):
        df = pd.read_csv('prep_germany.csv', index_col=0)
    
    else:
        #assign variable df to acquire function
        df= acquire.get_germany_data()

        #change data type on Date
        df.Date = df.Date.astype('datetime64[ns]')
        #reset Date as index
        df = df.set_index('Date').sort_index()

        #create new colum for month
        df['month'] = df.index.month_name()
        #create new colum for weekday
        df['year'] = df.index.year

        #fill nulls
        df = df.fillna(0)

        #create csv
        df.to_csv('prep_germany.csv')

    return df
