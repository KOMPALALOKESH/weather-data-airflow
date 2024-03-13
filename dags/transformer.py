import json
import pandas as pd
from pandas import DataFrame, json_normalize
import datetime as dt

def transform_weatherAPI(im_json: str):
    print(im_json)

    # string ==> json
    api_json = json.loads(im_json)  
    print(api_json)

    # normalize the contents (flatten to dataframe)
    df = json_normalize(api_json)
    print(df)
    # try:
    #     df = json_normalize(api_json)
    # except Exception as e:
    #     print("Error during normalization:", str(e))
    #     raise
    # print("Normalized DataFrame:", df)

    df['timestamp'] = df['location.localtime_epoch'].apply(lambda s : dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S+05:30'))

    # rename cols
    df = df.rename(columns={'location.name':'location',
                       'location.region':'region',
                       'current.temp_c':'temp_c',
                       'current.wind_kph':'wind_kph'})
    print(df)
    df = df.filter(['location','temp_c','wind_kph','timestamp','region'])  
    print(df)

    ex_json = DataFrame.to_json(df, orient='records')
    print(ex_json)
    return ex_json