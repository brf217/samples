library(glue)
library(jsonlite)
library(httr)

api_key = key

eia_api = function(endpoint, series, api_key, freq){
  # form query using GET from httr package
  if (freq == 'weekly'){
    start_dt = '2017-01-01'}
  else {
    start_dt = '2017-01'}
  
  api_call = GET(glue(
    'https://api.eia.gov/v2/{endpoint}?',
    'api_key={api_key}',
    '&data[0]=value',
    '&facets[series][1]={series}',
    '&frequency={freq}',
    '&start={start_dt}'))
  
  # parse the response
  response_list = parse_json(api_call, simplifyVector = T)
  
  # retrieve dataframe from the results
  api_df = response_list$response$data
  
  # replace bad df column names with better ones
  names(api_df) = gsub('[-&$?%]', '_', names(api_df))
  
  return(api_df)
}

ng_storage = eia_api('natural-gas/stor/wkly/data/', 'NW2_EPG0_SWO_R48_BCF',
                   api_key,'weekly') 
