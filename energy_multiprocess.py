import pandas as pd
import re
import datetime as dt
from pandas.tseries.offsets import MonthEnd
import numpy as np
from dateutil.relativedelta import relativedelta as rd
import os
import concurrent.futures
import pyodbc
from pandas import ExcelWriter

# imports from query file
from rystad_api_fetch_query import basin, api_df, plays_api_numbers

def dbconnect():
    'connection string'
    return conn

def master_query(conn, plays_api_numbers):
    all_api_numbers = []
    for subp, apis in plays_api_numbers.items():
        all_api_numbers.extend(apis)
        
    df = pd.read_sql_query(f'''
        ---bring in data from 'WellInfo' table
        SELECT 
             wi.[API Number] as api_number
            ,wi.[Year] as year
            ,wi.[Month] as month
            
        ---bring in data from 'Production3Stream' table
            ,prod.[Production Forecast Group] as production_forecast_group
            
            ,max(CASE WHEN prod.[OilAndGasGroup]  = 'O' THEN 'Light Oil' 
                WHEN prod.[OilAndGasGroup] = 'D' THEN 'Dry Gas' 
                WHEN prod.[OilAndGasGroup] = 'N' THEN 'NGL'ELSE 'NA' END) as OilAndGasGroup
            
            ,sum(prod.[production3stream]) as production3stream
            
        ---bring in data from 'WellHeader' table
            ,max(wh.[Completion Date]) as completion_date
            ,max(wh.[Production Start Date]) as production_start_date
            ,max(wh.[Reported Production Months]) as reported_production_months
            ,max(wh.[Lateral Length]) as lateral_length
            ,max(wh.[Horizontal Spacing]) as horizontal_spacing
            ,max(wh.[Distance Closest Well 2D (Feet)]) as distance_closest_well_2d_feet
            ,max(wh.[Distance Closest Well 3D (Feet)]) as distance_closest_well_3d_feet
            ,max(wh.[Estimated Well Gas Hyperbolic Factor]) as estimated_well_gas_hyperbolic_factor
            ,max(wh.[Estimated Well Oil Hyperbolic Factor]) as estimated_well_oil_hyperbolic_factor
            ,max(wh.[Estimated Well Gas Initial Decline]) as estimated_well_gas_initial_decline
            ,max(wh.[Estimated Well Oil Initial Decline]) as estimated_well_oil_initial_decline
            ,max(wh.[Estimated Well Gas Peak Production]) as estimated_well_gas_peak_production
            ,max(wh.[Estimated Well Oil Peak Production]) as estimated_well_oil_peak_production
            ,max(wh.[Proppant (Thousand pounds)]) as proppant_lbs
            ,max(wh.[Fracturing Liquid Volume]) as fracturing_liquid_volume

          FROM [ShaleWellCube].[WellInfo] wi

          --- join wellinfo and production3stream table
          JOIN [ShaleWellCube].[Production3Stream] prod
              ON wi.[API Number] = prod.[API Number]
              AND wi.[Month] = prod.[Month]
              AND wi.[Year] = prod.[Year]

          --- join wellinfo and wellheader table
          JOIN [ShaleWellCube].[WellHeader] wh
              ON wh.[API Number] = prod.[API Number]

           WHERE wi.[Year] >= 2015
            AND prod.[OilAndGasGroup] in ('O', 'D', 'N')
            AND wi.[API Number] in {tuple(all_api_numbers)}
        
        GROUP BY
            wi.[API Number] 
            ,wi.[Year] 
            ,wi.[Month] 
            ,prod.[Production Forecast Group]
            ,prod.[OilAndGasGroup]

        ORDER BY 
            wi.[API Number], [Year], [Month]
                               ''', conn)
    
    # clean up columns
    df.columns = [re.sub(r'\s', '_', x).lower() for x in df.columns]
    df.columns = [re.sub(r'[()]', '', x).lower() for x in df.columns]
    api_df.columns = [re.sub(r'\s', '_', x).lower() for x in api_df.columns]
    api_df.columns = [re.sub(r'[()]', '', x).lower() for x in api_df.columns]
    
    df.dropna(subset = ['api_number'], inplace = True)

    # merge the subplays into master dataframe
    df = pd.merge(df, api_df,
             left_on = 'api_number',
             right_on = 'api_number')
    
    # clean up date columns
    df[['production_start_date', 'completion_date']] = df[
        ['production_start_date', 'completion_date']].apply(pd.to_datetime)
    
    df['prod_start_year'] = df['production_start_date'].dt.year
    return df


def loop_args(df):
    # create list of dataframes
    api_group = dict(tuple(df.groupby(['api_number', 'oilandgasgroup'])))
    args = [(k[0], api_group[k], k[1]) for k in api_group.keys()]
    return args


def get_arps_vals(df):
    if c == 'Light Oil':
        ip = max(df['production3stream']/30)
        decl =  max(df['estimated_well_oil_initial_decline'])
        hyp =  max(df['estimated_well_oil_hyperbolic_factor'])
    elif c == 'Dry Gas' or c == 'NGL':
        ip = max(df['production3stream']/30)
        decl = max(df['estimated_well_gas_initial_decline'])
        hyp = max(df['estimated_well_gas_hyperbolic_factor'])
    else:
        raise ValueError('no arps values found')
    return [ip, decl, hyp]


def month_serial(m):
    if  m == first_record_dt:
        mon = 0 
    else:
        mon = (pd.Timestamp(m) - pd.Timestamp(first_record_dt)) / np.timedelta64(1, 'M')
        mon = int(round(mon,0))
    return mon


def arps_model(row):
    if row['days_after_pk'] == 0:
        return row['rys_daily_prod_boe']
    else:
        return  arps_vals[0]*(
                1+(arps_vals[2]*arps_vals[1]*row['days_after_pk']))**(-1/arps_vals[2])


def fitter(api_number, df, c):
    try:
        # filter to one api number and fuel type
        df_fit = df.copy()
        
        # sort temp dataframe for loop
        df_fit.sort_values(['year', 'month'], inplace = True)
        
        # test to see if df is empty
        if df_fit.empty:
            print('empty_df '+ api_number)
        
        # test to see if df has enough months to do analysis
        if max(df_fit['reported_production_months']) < 5:
            print('empty_df '+ api_number)
        
        # calc days and cumulative days for model 
        df_fit['record_date'] = df_fit.apply(
            lambda row: dt.date(row['year'], row['month'], 1),
            axis = 1)
            
        # collect production start and ip dates for timing, and ARPS inputs for model
        prod_start_dt = max(df_fit.production_start_date)
        df_fit['prod_start_year'] = prod_start_dt.year
        last_curve_dt = max(df_fit.record_date)
        first_record_dt = min(df_fit.record_date)
        
        # find peak production date based on when the max production actually occured
        peak_prod_dt = df_fit[
            df_fit['production3stream'] == max(
                df_fit['production3stream'])]['record_date'].values[0]
        
        # get arps values based on the type of fuel chose in vars 
        arps_vals = get_arps_vals(df_fit)
        
        # get serial months operating for Tc
        df_fit['month_serial'] = df_fit['record_date'].apply(month_serial)
            
        # extend date series and concat to original frame
        last_actual_month = max(df_fit.month_serial)
        months_to_add = int(360 - last_actual_month)
        fcst_dates = list(pd.date_range(last_curve_dt, last_curve_dt + rd(months = months_to_add), 
                      freq = 'M'))
        
        # change new date series to first day of month convention
        tc = pd.DataFrame(fcst_dates, columns = ['record_date'])
        tc['record_date'] = tc['record_date'].apply(lambda x: (x + rd(days = 1)).date())
        
        # bring original and forecast frame together
        df_fit = pd.concat([df_fit, tc]).reset_index()
        
        # find days in month and cumulative days/months operating
        df_fit['days_in_mo'] = df_fit.apply(
            lambda row:(row['record_date'] + MonthEnd(1)).date() - row['record_date'],
                        axis =1)    
                
        # convert interval to integer days and get cumulative operating days 
        df_fit['days_in_mo'] = df_fit['days_in_mo'].dt.days + 1
        df_fit['days_applied'] = np.where(df_fit['record_date'] <= peak_prod_dt, 0, 
                                          df_fit['days_in_mo'])
        
        # cumulative sum of days after peak prod date
        df_fit['days_after_pk'] = df_fit['days_applied'].cumsum()
        
        # reapply serial month function
        df_fit['month_serial'] = df_fit['record_date'].apply(month_serial)
        
        # convert rystad prod to daily to check against model run
        df_fit['rys_daily_prod_boe'] = df_fit['production3stream'] / df_fit['days_in_mo']
        
        # run model and convert rystad production to daily for check
        df_fit['arps_decline'] = df_fit.apply(arps_model, axis = 1)
        
        # make a column for actual and fcst combo (take any rystad # before using arps)
        df_fit['actual_boe_pl_arps'] = np.where(
            pd.notnull(df_fit['production_forecast_group']),
            df_fit['rys_daily_prod_boe'],
            df_fit['arps_decline']
                )
    
        # compute type curve per foot to average out at the end vs. other type curves
        df_fit['actual_boe_pl_arps_ft'] = df_fit['actual_boe_pl_arps'] / max(df_fit['lateral_length'])
        
        # forward fill production start year       
        df_fit['prod_start_year'].ffill(inplace = True)
        
        # add run to dictionary
        df_fit = df_fit[['api_number','month_serial', 'production_forecast_group',
                                  'rys_daily_prod_boe', 'arps_decline',
                                  'actual_boe_pl_arps', 'actual_boe_pl_arps_ft',
                                  'prod_start_year', 'oilandgasgroup', 'subplay']]
        
        # show our forecast rows vs. R/F markers in Rystad Data
        df_fit['production_forecast_group'].fillna('AP', inplace = True)
        
        # forward fill rows where empty after forecast extension
        df_fit[['api_number', 'oilandgasgroup', 'subplay']] = df_fit[
            ['api_number', 'oilandgasgroup', 'subplay']].fillna(method = 'ffill')
        
        return df_fit
    except:
        pass


def post_loop_all_tc(dic):
    all_tc = pd.concat(dic, ignore_index= True)
    
    # sort properly
    all_tc.sort_values(['api_number', 'subplay', 
                        'oilandgasgroup', 'month_serial'],
                       inplace = True)
    return all_tc


def wellcost_query(df, conn):
    q_api_nums = tuple([a for a in df['api_number'].unique() if a != pd.isnull(a) if a ])
    q_api_nums = q_api_nums[0:len(q_api_nums)]
    wellcost_df = pd.read_sql_query(f'''
    SELECT    
         wc.[API Number] as api_number
        ,sum(wc.[Well Cost (MUSD)]) as well_cost_m
    FROM [ShaleWellCube].[EconomicsCost] wc
    WHERE wc.[API Number] in {q_api_nums}
    GROUP BY wc.[API Number]''', conn)

    # add well cost to df
    df_summary = pd.merge(df, wellcost_df,
             on = 'api_number')
    
    df_summary = df_summary.rename(columns = {'Subplay':'subplay'})
    return df_summary    


# =============================================================================
# run analysis
# =============================================================================    
if __name__ == "__main__":
    # select working directory
    os.chdir(r'dir')
    
    # full script timer start
    start = dt.datetime.now()
    
    # connect to database
    conn = dbconnect()
    
    # run large query for all api_characteristics
    print('master query started:', dt.datetime.now())  
    df = master_query(conn, plays_api_numbers)
    print('master query done:', dt.datetime.now())   
    
    # group dataframe to create arguments for loop
    args = loop_args(df)
    
    # run the parallel process
    dic = {}
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fitter, *arg): (arg[0],arg[2]) for arg in args}
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            dic[name] = future.result()

    # concatenate all dictionary items into df and clean up results
    all_tc = post_loop_all_tc(dic)

    # timer end
    end = dt.datetime.now()
    print('total time:', end-start)
    print('total frames fit:', len(args))


