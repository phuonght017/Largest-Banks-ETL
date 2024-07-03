import requests
import bs4
import pandas as pd 
import sqlite3
import numpy as np
from datetime import datetime
# CONSTANT #
url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = "exchange_rate.csv"
output_csv_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"

# Code for ETL operations on Country-GDP data
# Importing the required libraries
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + ':' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(url)
    html_content = response.text 
    soup = bs4.BeautifulSoup(html_content, 'html.parser')
    table = soup.find_all('table')[0]
    df = pd.DataFrame(columns = table_attribs)
    rows = table.find_all('tr')
    rows = rows[1:]
    for row in rows:
        cells = row.find_all('td')
        bank_name = cells[1].text.replace("\n", "")
        market_cap = float(cells[2].text.replace("\n","").replace(",",""))
        df = pd.concat([df, pd.DataFrame([{"Name":bank_name, "MC_USD_Billion": market_cap}])], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    df = pd.read_sql(query_statement, sql_connection)
    return df 

# ETL PIPELINE #
log_progress("Preliminaries complete. Initiating ETL process")

extracted_data = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

transformed_data = transform(extracted_data,csv_path)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(transformed_data, output_csv_path)
log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")
load_to_db(transformed_data, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

query1 = "SELECT * FROM Largest_banks"
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query3 = "SELECT Name from Largest_banks LIMIT 5"
run_query(query1,sql_connection)
run_query(query2,sql_connection)
run_query(query3,sql_connection)
