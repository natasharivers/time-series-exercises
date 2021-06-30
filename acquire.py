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

############################# GET STORE DATA FUNCTION #########################

def get_store_data(base_url, endpoint, filename):
    '''
    This function takes in a base url, endpoint and file name
    and uses an existing csv or creates a new one if one does not exist
    then it brings in all pages of a dataset and turns them into a pandas df
    '''

    #if a csv exists, use it as the df
    if os.path.isfile(filename):
        df = pd.read_csv(filename, index_col=0)
        return df
    
    #if a csv does not exist, create one
    else:

        #create an empty list
        key_list = []
        response = requests.get(base_url)
        data = response.json()
        
        #create an n with max page from the payload
        n = data['payload']['max_page']

        #loop to pull in all pages
        for i in range(1,n+1):
            new_url = base_url + '?page=' +str(i)
            response = requests.get(new_url)
            data = response.json()
            page_endpoint = data['payload'][endpoint]
            key_list += page_endpoint
    
        #turn that list into a dataframe
        df = pd.DataFrame(key_list)

    return df

