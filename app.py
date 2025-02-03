import pandas as pd
import requests
import os
import json

from datetime import date, timedelta

from sqlalchemy import create_engine, text, exc
from sqlalchemy.orm import sessionmaker

def get_engine_str() -> str:
    HOST = os.getenv('host')
    PORT = os.getenv('port')
    USER = os.getenv('user')
    PASSWORD = os.getenv('password')

    return f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}'

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
        df_stores.drop(columns=[col for col in df_stores.columns if col not in columns_names['columns_stores'].keys()], inplace=True)
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
        df_companies.rename(columns=columns_names['columns_companies'], inplace=True)
        df_stores.drop(columns=[col for col in columns_names['columns_companies'].keys() if 'id' not in col], inplace=True)

        df_timezones = df_stores[list(columns_names['columns_timezones'].keys())].copy()
        df_timezones.drop_duplicates(inplace=True)
        df_timezones.rename(columns=columns_names['columns_timezones'], inplace=True)
        df_stores.drop(columns=[col for col in columns_names['columns_timezones'].keys() if 'id' not in col], inplace=True)

        df_segments = df_stores[list(columns_names['columns_segments'].keys())].copy()
        df_segments.drop_duplicates(inplace=True)
        df_segments.rename(columns=columns_names['columns_segments'], inplace=True)
        df_stores.drop(columns=[col for col in columns_names['columns_segments'].keys() if 'id' not in col], inplace=True)
        
        df_brands = df_stores[list(columns_names['columns_brands'].keys())].copy()
        df_brands.drop_duplicates(inplace=True)
        df_brands.rename(columns=columns_names['columns_brands'], inplace=True)
        df_stores.drop(columns=[col for col in columns_names['columns_brands'].keys() if 'id' not in col], inplace=True)
        
        return {
            'companies': df_companies, 
            'timezones': df_timezones, 
            'segments': df_segments,
            'brands': df_brands,
            'stores': df_stores, 
        }

    def data_processing(df_stores: pd.DataFrame) -> pd.DataFrame:
        df_stores['cameras'] = str(df_stores['cameras'])
        df_stores['postal_code'] = pd.to_numeric(df_stores['postal_code'].str.replace('-', '')).round(0).astype(int)
        return df_stores

    response = requests.get(url=f'https://api-flowix.before.com.br/api/v1/integracao/unidades', headers=get_headers())
    if response.status_code == 200:
        df_stores = pd.DataFrame(pd.json_normalize(response.json()['unidades']))
        columns_names = get_columns_names()
        df_stores = rename_columns(df_stores=df_stores, columns_names=columns_names)
        df_stores = data_processing(df_stores=df_stores)
        return divide_tables(df_stores=df_stores, columns_names=columns_names)
    return pd.DataFrame()

def upsert_stores() -> None:
    """
    Upsert the stores, companies, timezones, and segments into the database.

    This function is a wrapper around get_stores() and the SQLAlchemy library.
    It fetches the stores data from the API, renames the columns, and divides the
    DataFrame into four DataFrames: stores, companies, timezones, and segments.
    Then, it upserts each DataFrame into the corresponding table in the database.

    The upsert is done by first inserting the DataFrames into temporary tables in
    the database, and then using SQL queries to upsert the data from the temporary
    tables into the final tables. The queries are constructed using the column names
    of the DataFrames, so the function is robust to changes in the structure of the
    API response.

    Finally, the function commits the changes and closes the database connection.
    """
    engine = create_engine(get_engine_str())    
    df = get_stores()
    for name, value in df.items():
        if not value.empty:
            Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
            session = Session()
            
            value.to_sql(name=f'temp_{name}', con=session.connection(), index=False, if_exists='replace', schema='dball')
        
            query = f"""
                INSERT INTO dbflowix.{name} ({', '.join(value.columns)})
                SELECT * FROM dball.temp_{name}
                ON DUPLICATE KEY UPDATE
                    {', '.join([f'{col} = VALUES({col})' for col in value.columns])};
            """
            
            with session.connection() as conn:
                conn.execute(text(query))
                conn.execute(text(f'DROP TABLE IF EXISTS dball.temp_{name}'))
                conn.commit()
            

def get_visits(registration_date: str, company_id: int = 91):
    
    def get_columns_names() -> str:
        with open('json/columns.json', 'r') as f:
            columns_names = json.load(f)
        return columns_names
    
    def rename_columns(columns_names: str, df_visits: pd.DataFrame):
        df_visits.drop(columns=[col for col in df_visits.columns if col not in columns_names['columns_visits'].keys()], inplace=True)
        df_visits.rename(columns=columns_names['columns_visits'], inplace=True)
        return df_visits
    
    def data_processing(df_visits):
        df_visits['id'] = pd.to_numeric(df_visits['registration_date'].str.replace('-', '')).round(0).astype(int)
        df_visits['registration_date'] = pd.to_datetime(df_visits['registration_date'])
        return df_visits
    
    response = requests.get(url=f'https://api-flowix.before.com.br/api/v1/integracao/visitas/consolidado?empresa_id={company_id}&data={registration_date}', headers=get_headers())
    if response.status_code == 200:
        df_visits = pd.DataFrame(response.json()['visitas'])
        columns_names = get_columns_names()
        df_visits = rename_columns(columns_names=columns_names, df_visits=df_visits)
        df_visits = data_processing(df_visits)
        return df_visits
    return pd.DataFrame()

def upsert_visits(registration_date: str) -> None:
    engine = create_engine(get_engine_str())
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    session = Session()
    
    df = get_visits(registration_date=registration_date)
    if not df.empty:
        df.to_sql(name='temp_visits', con=session.connection(), index=False, if_exists='replace', schema='dball')
        query = f"""
            INSERT INTO dbflowix.visits ({', '.join(df.columns)})
            SELECT * FROM dball.temp_visits
            ON DUPLICATE KEY UPDATE
                {', '.join(f'{col} = VALUES({col})' for col in df.columns)}            
        """
        with session.connection() as conn:
            conn.execute(text(query))
            conn.execute(text('DROP TABLE IF EXISTS dball.temp_visits'))
            conn.commit()
    
def main():
    upsert_stores()
    days = 3
    for day in range(days):
        registration_date = date.today() - timedelta(days=day)
        registration_date = registration_date.strftime('%Y-%m-%d')
        upsert_visits(registration_date=registration_date)
    
if __name__ == '__main__':
    main()