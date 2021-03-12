  
import pandas as pd 
import numpy as np 
import os
from env import host, user, password 
from sklearn.model_selection import train_test_split


#all reproducable functions used

#get_connect will retrieve user login info from env file to login to codeup data science database server
def get_connection(db, user=user, host=host, password=password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    
#function to acquire dataframe
def acquire_telco():
     if os.path.isfile('telco_churn.csv') == False:
         sql_query = '''
                        SELECT customer_id, monthly_charges, tenure, total_charges, contract_type_id
                        FROM customers
                        JOIN contract_types USING(`contract_type_id`)
                        WHERE contract_type_id = '3';
                    '''
         df = pd.read_sql(sql_query, get_connection('telco_churn'))
         df.to_csv('telco_churn.csv')
     else:
        df = pd.read_csv('telco_churn.csv', index_col=0)
     return df

def clean_telco(df):
    '''
    Takes in a df of telco customer monthly charges, tenure, total charges
    and cleans the data appropriately by ,
    and converting object data to numerical data
    as well as dropping cutomer_id and contract_type_id columns from the dataframe
    return: df, a cleaned pandas dataframe
    '''
    df.total_charges = df.total_charges.replace(r'^\s*$', np.nan, regex=True)
    df = df.fillna(0)
    df['total_charges'] = df['total_charges'].astype('float')
    df = df.drop(columns=['contract_type_id'])
    return df


def split_data(df):
    '''
    split our data,
    takes in a pandas dataframe
    returns: three pandas dataframes, train, test, and validate
    '''
    train_val, test = train_test_split(df, train_size=0.8, random_state=123)
    train, validate = train_test_split(train_val, train_size=0.7, random_state=123)
    return train, validate, test

#wrangle: acquire and prep data set
def wrangle_telco():
    '''
    wrangle_telco will read in our telco dataset as a pandas df,
    clean the data,
    split the data,
    return: train, validate, test sets of pandas dataframes from telco data
    stratified on total_charges
    
    '''
    df = clean_telco(acquire_telco())

    return split_data(df)