import datetime
import pandas as pd
import datetime as dt
import os
import camelot
import re
import urllib.request
import glob
import tabula
import snowflake.connector

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
    
    if len(write_frame.index) != 0:
    # write dataframe and return status
        success, nchunks, nrows, _ = write_pandas(conn, write_frame, tbl)
        return success, nchunks, nrows
    else:
        print("No new data to write.")
  
    
def download_hist_report(year):
    os.chdir(r'U:\fire_tankers\imsr_depot_temp')
    date_rng = pd.date_range(f'{year}-01-01', f'{year}-12-31', freq = 'D')
    fmt_dates = [x.strftime("%Y%m%d")+'IMSR.pdf' for x in date_rng]
    base =f'https://famprod.nwcg.gov/batchout/IMSRS_from_1990_to_2022/{year}/'
    for d in fmt_dates:
        try:
            urllib.request.urlretrieve(base+d, d)
            print(f'downloaded {base+d}')
        except:
            print(f'no record for {base+d}')
            pass


def get_full_file_list(folder = r'U:\fire_tankers\imsr_depot_temp'):
    file_list = glob.glob(folder+'/*.pdf')
    return file_list
    

def find_page_camelot(pdf_file_nm):
    '''go to imsr fire archives to get old files - p is the pdf - index tbls:
        https://www.nifc.gov/nicc/incident-information/imsr'''
    
    # download specified document
    doc = camelot.read_pdf(pdf_file_nm,
                         pages='4-end',
                         flavor='stream')
    
    # find the page I want by searching for year-to-date marker on table
    page = 0
    for pg in doc:
        pg = pg.df
        # get title from the table to make sure it is the right one
        top_row = pg.head(1)
        # split into values
        pg_title = [x for x in top_row.values[0] if x != ''][0]
        # look for the text we want from the target page
        if 'year-to-date' in pg_title.lower():
            tgt_page = page
            break
        else:
            page+=1
    
    return tgt_page


def extract_report_data_t(pdf_file_nm, pg):
    '''go to imsr fire archives to get old files - p is the pdf - index tbls:
        https://www.nifc.gov/nicc/incident-information/imsr'''
    
    # download specified document
    ytd_df = tabula.read_pdf(pdf_file_nm,
                         pages=f'{pg+4}',
                         stream = True)[0]
    
    # drop blank columns
    ytd_df.dropna(axis =1, how = 'all', inplace = True)
    
    # clean column names for the cropped table (originals were cut off)
    column_names = ['geo_area', 'value', 'bia', 'blm', 'fws', 'nps', 'stot',
                    'usfs', 'total']
    
    # apply the column names to the dataframes
    ytd_df.columns = column_names
    
    # name the frames with their origin data source
    ytd_df['timing'] = 'ytd'
    
    # get the file date from the file name and append to a column
    file_dt = re.findall(r'(\d{8})(IMSR.pdf)', pdf_file_nm)[0][0]
    file_date_label = dt.date(
        int(file_dt[0:4]),
        int(file_dt[4:6]),
        int(file_dt[6:8]),
        )
    
    # assign file date to frame
    ytd_df['rept_date'] = file_date_label
    
    # forward fill the geo area
    ytd_df['geo_area'].fillna(method = 'ffill', inplace = True)
    
    return ytd_df


# =============================================================================
# run and write result to dataframe
# =============================================================================
if __name__ == '__main__':
    
    # establish connection and choose default variables
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    cur.execute(f'USE ROLE "_"')
    cur.execute(f'USE WAREHOUSE "_"')
    cur.execute(f'USE DATABASE "_"')
    cur.execute(f'USE SCHEMA "_"')
   
    # download contents of ftp site using year (2015+) and put in wd
    download_hist_report(2018)
    
    # retrieve all the files from the wd
    file_list= get_full_file_list()
    
    # loop and create db tables of the output
    tabula_dic = {}
    for f in file_list:
        try:
            print(f)
            # camelot
            pg = find_page_camelot(f)
            #tabula
            temp_t = extract_report_data_t(f, pg)
            tabula_dic[f] = temp_t
        except:
            tabula_dic[f] = None
    
    # create dataframe from scraped reports
    df = pd.concat(tabula_dic, ignore_index= True)
    
            
