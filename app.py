import pandas as pd
import requests
import os
import json

from datetime import datetime, date, timedelta

def get_headers():
    """
    Get the headers for the request.

    Returns
    -------
    dict
        A dictionary with the headers.
    """
    ACCEPT = os.getenv('accept')
    CONTENT_TYPE = os.getenv('Content_Type')
    X_API_KEY = os.getenv('x_api_key')

    return {
        'accept': ACCEPT,
        'Content_Type': CONTENT_TYPE,
        'x-api-key': X_API_KEY
    }

def get_stores() -> pd.DataFrame:
    """
    Get all stores from the API.

    Returns
    -------
    tuple of DataFrame or None
        A tuple with the DataFrames of the stores, companies, timezones, and segments, or None if the request was not successful.
    """
    
    def get_columns_names() -> str:
        """
        Load and return column names from a JSON file.

        Returns
        -------
        dict
            A dictionary containing column name mappings loaded from a JSON file.
        """

        with open('json/columns.json', 'r') as f:
            columns_names = json.load(f)
        return columns_names
        
    def rename_columns(df_stores: pd.DataFrame, columns_names: str):
        """
        Rename columns of a DataFrame.

        Parameters
        ----------
        df_stores : pd.DataFrame
            DataFrame with the stores.
        columns_names : str
            A dictionary containing column name mappings loaded from a JSON file.

        Returns
        -------
        pd.DataFrame
            DataFrame with the renamed columns.
        """
        df_stores.rename(columns=columns_names['columns_stores'], inplace=True)
        
        return df_stores

    def divide_tables(df_stores: pd.DataFrame, columns_names: str):
        """
        Divide the stores DataFrame into four DataFrames: stores, companies, timezones, and segments.

        Parameters
        ----------
        df_stores : pd.DataFrame
            DataFrame with the stores.
        columns_names : str
            A dictionary containing column name mappings loaded from a JSON file.

        Returns
        -------
        tuple of DataFrame
            A tuple with the DataFrames of the stores, companies, timezones, and segments.
        """
        df_companies = df_stores[list(columns_names['columns_companies'].keys())].copy()
        df_companies.drop_duplicates(inplace=True)
        df_stores.drop(columns=[col for col in columns_names['columns_companies'].keys() if 'id' not in col], inplace=True)
        df_companies.rename(columns=columns_names['columns_companies'], inplace=True)

        df_timezones = df_stores[list(columns_names['columns_timezones'].keys())].copy()
        df_timezones.drop_duplicates(inplace=True)
        df_stores.drop(columns=[col for col in columns_names['columns_timezones'].keys() if 'id' not in col], inplace=True)
        df_timezones.rename(columns=columns_names['columns_timezones'], inplace=True)

        df_segments = df_stores[list(columns_names['columns_segments'].keys())].copy()
        df_segments.drop_duplicates(inplace=True)
        df_stores.drop(columns=[col for col in columns_names['columns_segments'].keys() if 'id' not in col], inplace=True)
        df_segments.rename(columns=columns_names['columns_segments'], inplace=True)
        
        return df_stores, df_companies, df_timezones, df_segments

    response = requests.get(url=f'https://api-flowix.before.com.br/api/v1/integracao/unidades', headers=get_headers())
    if response.status_code == 200:
        df_stores = pd.DataFrame(pd.json_normalize(response.json()['unidades']))
        columns_names = get_columns_names()
        df_stores = rename_columns(df_stores=df_stores, columns_names=columns_names)
        return divide_tables(df_stores=df_stores, columns_names=columns_names)
    return None

def get_visits(registration_date: str, company_id: int = 91):
    
    def get_columns_names() -> str:
        with open('json/columns.json', 'r') as f:
            columns_names = json.load(f)
        return columns_names
    
    def rename_columns(columns_names: str, df_visits: pd.DataFrame):
        df_visits.rename(columns=columns_names['columns_visits'], inplace=True)
        return df_visits
    
    print(company_id, registration_date)
    response = requests.get(url=f'https://api-flowix.before.com.br/api/v1/integracao/visitas/consolidado?empresa_id={company_id}&data={registration_date}', headers=get_headers())
    if response.status_code == 200:
        df_visits = pd.DataFrame(response.json()['visitas'])
        columns_names = get_columns_names()
        df_visits = rename_columns(columns_names=columns_names, df_visits=df_visits)
        return df_visits
    return None

    
def main():
    days = 3
    for day in range(days):
        registration_date = date.today() - timedelta(days=day)
        registration_date = registration_date.strftime('%Y-%m-%d')
        print(get_visits(registration_date=registration_date))
    
if __name__ == '__main__':
    main()