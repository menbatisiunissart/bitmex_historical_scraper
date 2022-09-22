import gzip
from io import StringIO
from pandas import json_normalize, read_csv
from requests import Response

def define_file(date, channel, symbol=None):
    file = "{}_{}.csv".format(date, channel)
    if (symbol!=None):
        file = "{}_{}_{}.csv".format(date, channel, symbol)
    return file

def filter(bytes_data: bytes, file: str, symbol: str):
    s=str(bytes_data,'utf-8')
    data = StringIO(s)
    df_csv = read_csv(data)
    if (symbol!=None and  not df_csv.empty):
        print("Filtering data for {}".format(file), "symbol: {}".format(symbol))
        df_csv = df_csv.loc[df_csv['symbol'] == symbol]
    df_csv.to_csv(path_or_buf=file, index=False)

def process_response(response: Response, date_str: str, channel: str, symbol:str):
    file = define_file(date_str, channel, symbol)
    if channel == "funding":
        data = response.json()
        df = json_normalize(data)
        df.to_csv(path_or_buf=file, index=False)
    else:
        with open(date_str, 'wb') as fp:
            fp.write(response.content)

        with gzip.open(date_str, 'rb') as fp:
            data = fp.read()

        filter(data, file, symbol)