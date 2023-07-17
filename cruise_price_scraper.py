import requests
import re
import pandas as pd
import os
import datetime as dt
import argparse
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from bs4 import BeautifulSoup as soup


# =============================================================================
# parse arguments for different environments
# =============================================================================
parser = argparse.ArgumentParser()

parser.add_argument('--Role',
                    help='Snowflake Role',
                    default='')

parser.add_argument('--Warehouse',
                    help='Snowflake Warehouse',
                    default='')

parser.add_argument('--Database',
                    help='Snowflake Database',
                    default='')

parser.add_argument('--Schema',
                    help='Snowflake Schema',
                    default='')

# assign arguments to vars
args = parser.parse_args()
snowflake_role = args.Role
snowflake_warehouse = args.Warehouse
snowflake_database = args.Database
snowflake_schema = args.Schema    


# connect to snowflake                            
def get_snowflake_connection():
    if os.getenv('SNOWFLAKE_USERNAME'):
        conn = snowflake.connector.connect(
             account        =  ''
            ,authenticator  = ''
            ,user           = os.getenv('SNOWFLAKE_USERNAME')
            ,password       = os.getenv('SNOWFLAKE_PASSWORD')
            ,autocommit     = True
        )
    else:
        conn = snowflake.connector.connect(
             account        = ''
            ,authenticator  = ''
            ,user           = os.getenv('USERNAME')
            ,autocommit     = True
        )
    return conn


# write dataframe to snowflake table
def df_write_pandas(conn, tbl, frame):
    '''1) import: from snowflake.connector.pandas_tools import write_pandas
        write full dataframe to pandas vs. doing row-wise
       2) connect outside function to keep alive
       3) args to snowflake need to be in caps (tbl, etc.) 
        '''
    # create a local frame copy
    write_frame = frame.copy()
    
    # add a write_time
    if 'write_time' not in write_frame.columns:
        write_frame['write_time'] = str(dt.datetime.now())
    
    # convert to upper case for Snowflake match
    write_frame.columns = [x.upper() for x in write_frame.columns]
    
    # convert datetimes to strings for Snowflake acceptance
    write_frame[list(write_frame.select_dtypes('datetime'))] = \
        write_frame.select_dtypes('datetime').apply(lambda x: x.astype(str))
    
    # write dataframe and return status
    success, nchunks, nrows, _ = write_pandas(conn, write_frame, tbl)
    return success, nchunks, nrows


def get_last_page(dest_code):
    r = requests.get(
        'https://www.cruisewatch.com/find/cruise?search_destinations%5B0%5D'\
            f'={dest_code}', verify=False)
    s = soup(r.content, 'html.parser')
    try:
        pgs = s.findAll('li', {'class':'hidden-xs'})
        last_pg = int(pgs[1].text)
        last_offset = (last_pg-1)*10
    except:
        pgs = s.findAll('a', {'class':'page'})
        last_pg = int(pgs[-1].text)
        last_offset = (last_pg-1)*10
    return last_offset    


def get_page_data(offset, dest_code):
    r = requests.get(
        'https://www.cruisewatch.com/find/cruise?search_destinations%5B0%5D'\
            f'={dest_code}&offset={offset}', verify=False)
    s = soup(r.content, 'html.parser')
    return s


def process_single_page_data(s):
    # get and process sail dates
    sail_date = s.findAll('div', {'class':'tabs'})

    # price loop list initiation
    prices = s.findAll('div', {'class':'tab-content tabs-small'})

    # line name list initiation
    line_name = s.findAll('span', {'class':re.compile(r'cl-logos.*')})

    # ship name initiation
    ship_names = s.findAll('span', {'class':'search-result tour-name'})
    
    for cruise in range(0, len(sail_date)):
        # clean up returned date data
        sail_dt_string = sail_date[cruise].text.split('\xa0')
        sail_dt_string = [x.strip() for x in sail_dt_string]
        bad_dt_chars = ['SAIL DATE:', '', '-']
        sail_dt_list = [d for d in sail_dt_string if d not in bad_dt_chars]
        
        # add clean dates to lists for holding
        start_dt = sail_dt_list[0]
        end_dt = sail_dt_list[1] 
        sail_start.append(start_dt)
        sail_end.append(end_dt)
        
        # clean up returned price data
        prices_string = prices[cruise].text
        prices_string = prices_string.split('\n')
        bad_px_chars = ['', 'Book Now']
        sail_px_list = [p for p in prices_string if p not in bad_px_chars and (
            p.startswith('$') or p.startswith('not'))]
        
        # add prices to lists for holding
        try:
            interior_px.append(sail_px_list[0])
        except:
            interior_px.append('NA')
        
        try:
            ocean_view_px.append(sail_px_list[1])
        except:
            ocean_view_px.append('NA')
            
        try:   
            balcony_px.append(sail_px_list[2])
        except:
            balcony_px.append('NA')
            
        try:
            suite_px.append(sail_px_list[3])
        except:
            suite_px.append('NA')
    
        # get name of line
        try:
            name = re.sub(
                r'<span class="cl-logos |<|>|/span>|"', '', str(
                    line_name[cruise]))
            line_name_list.append(name)
        except:
            line_name_list.append('NA')
    
        # get name of ship
        try:
            ship_name = ship_names[cruise].text
            ship_name_list.append(ship_name)
        except:
            ship_name_list.append('NA')


def clean_dataframe(df):
        # add nights column to dataframe
        s = df['end_dt'].str.split(pat='\n', expand=True)
        df['end_dt'] = s[0]
        df['nights'] = (
            pd.to_datetime(df['end_dt'], errors='coerce', format="%b %d, %Y") - pd.to_datetime(
              df['start_dt'], errors='coerce', format="%b %d, %Y"))
        df['nights'] = [x.days for x in df.nights]
        
        # convert numeric columns
        df.replace({r'\$':''}, regex = True, inplace = True)
        numeric_cols = ['interior_px', 'ocean_view_px', 'balcony_px',
                        'suite_px']
        for n in numeric_cols:
            df[n] = pd.to_numeric(df[n], errors = 'coerce')
        
        # convert date columns
        date_cols = ['start_dt', 'end_dt']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce', format="%b %d, %Y")
        
        # add destination to df for tracking
        df['destination'] = dest
        
        # create a write_date column for filtering duplicate runs
        df['write_date'] = dt.date.today()

        return df

def retrieve_price(series):
  return [x.split(' ')[0].replace('not', 'NA') for x in series]
    
    
def query_current_timing():
    # find latest date grouped by type to filter database writes to new info
    date_filter = pd.read_sql(
    f""" 
    SELECT coalesce(max(write_time)::date, '1/1/1990'::date)
    FROM {snowflake_database}.{snowflake_schema}.CRUISEWATCH

    """, conn).values[0][0]
    
    return date_filter


# =============================================================================
# main script
# =============================================================================
if __name__ == '__main__':
    
    # establish connection and choose default variables
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    cur.execute(f'USE ROLE "{snowflake_role}"')
    cur.execute(f'USE WAREHOUSE "{snowflake_warehouse}"')
    cur.execute(f'USE DATABASE "{snowflake_database}"')
    cur.execute(f'USE SCHEMA "{snowflake_schema}"')

    # initialize empty variables
    dic = {}
    sail_start = []
    sail_end = []
    interior_px = []
    ocean_view_px = []
    balcony_px = []
    suite_px = []
    line_name_list = []
    ship_name_list = []
    
    # create dictionary destinations / cruisewatch codes to search
    dest_dic = {
        'alaska': 20,
        'mediterranean':7,
        'caribbean':1
                }
    dest_result_dic = {}
    
    # destination codes
    for dest,code in dest_dic.items():
        
        # find last offset value for loop
        last_offset = get_last_page(code)
    
        # loop to last offset and scrape values from each page
        for o in list(range(0, last_offset +10, 10)):
            s = get_page_data(o, code)
            process_single_page_data(s)
            print(o, len(line_name_list), len(ship_name_list), flush = True)
    
        # create dataframe from combined page results
        df = pd.DataFrame({
            'line':line_name_list,
            'ship':ship_name_list,
            'start_dt':sail_start,
            'end_dt': sail_end,
            'interior_px':retrieve_price(interior_px),
            'ocean_view_px':retrieve_price(ocean_view_px),
            'balcony_px':retrieve_price(balcony_px),
            'suite_px':retrieve_price(suite_px)
            })
        
        # clean up dataframe and convert coltypes
        df = clean_dataframe(df)
        
        # add dataframe to result dic for particular destination running
        dest_result_dic[dest] = df
        
    # create output df for all destinations combined
    output_df = pd.concat(dest_result_dic, ignore_index=True)
    
    # query for latest write time in db
    write_time_filter = query_current_timing()
    
    # write data to snowflake if there is anything new to add
    output_df = output_df[output_df['write_date'] > write_time_filter]
    
    if len(output_df) > 0:
        df_write_pandas(conn,
                        tbl = '',
                        frame = output_df)
    else:
        print('No new data to add')
