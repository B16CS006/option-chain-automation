import utils
import requests
import pandas as pd
from pandas import json_normalize
from datetime import datetime

###################################################################################################
# general functions

def strike_difference(records):
  strikes = sorted(records['strikePrices'])
  diff = strikes[1] - strikes[0]
  for i in range(2, len(strikes)):
    temp_diff = strikes[i] - strikes[i-1]
    if temp_diff < diff:
      diff = temp_diff
  return diff


def ATM_strike(records):
  return min(records['strikePrices'], key=lambda x: abs(x - records['underlyingValue']))

def near_by_strikeprices(records, count=3):
  strikes = sorted(records['strikePrices'])
  atm = ATM_strike(records)
  i = strikes.index(atm)
  return strikes[i-count:i+count+1]


def weighted_strikeprice(*args, **kwargs):
  if not args and not kwargs:
    raise Exception("Invalid argument to weighted_strikeprice function")
  records = kwargs.get('records', None)
  count = kwargs.get('count', None)
  low = kwargs.get("low", None)
  high = kwargs.get("high", None)
  if records:
    strikes = sorted(records['strikePrices'])
    atm = ATM_strike(records)
    if count:
      i = strikes.index(atm)
      return strikes[i-count], strikes[i+count]
  raise Exception("Invalid")


####################################################################################################
# DATA ANALYSIS

def __option_chain_pcr(df, key: str):
  return df["PE_" + key].sum()/df["CE_" + key].sum()

def option_chain_traded_volume_pcr(df):
  return __option_chain_pcr(df, "totalTradedVolume")

def option_chain_oi_pcr(df):
  return __option_chain_pcr(df, "openInterest")

def option_chain_oi_change_pcr(df):
  return __option_chain_pcr(df, "changeinOpenInterest")

####################################################################################################

def next_expiry(date_list: list):
  next_date =  next((sd for sd in sorted([datetime.strptime(d, "%d-%b-%Y").date() for d in date_list]) if sd >= datetime.now().date()), None)
  if next_date == None:
    raise Exception("Next Date not found")
  next_date_str = next_date.strftime("%d-%b-%Y")
  if next_date_str not in date_list:
    raise Exception("Next Date string conversion issue")
  return next_date_str

def download_nse_option_chain_indices(symbol):
  print(f"downloading {symbol} option chain data from NSE")
  url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
  headers = { "accept": "application/json", "accept-language": "en-US,en;q=0.9", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36" }
  response = requests.get(url, headers=headers)
  if response.status_code != 200:
    raise Exception(f"NSE data download failed with status code: {response.status_code}")
    return;
  return response.json()

def download_nse_option_chain_indices_records(symbol):
  return download_nse_option_chain_indices(symbol)["records"]

def convert_data_to_dataframe(data):
  return pd.DataFrame(data)

def convert_records_to_dataframe(records):
  df = convert_data_to_dataframe(records["data"])
  df["timestamp"] = records["timestamp"]
  return df

def unnest_pe_ce(df):
  unnested_pe = json_normalize(df["PE"])
  unnested_ce = json_normalize(df["CE"])
  df["underlying"] = unnested_pe["underlying"].loc[unnested_pe["underlying"].first_valid_index()] or unnested_ce["underlying"].loc[unnested_ce["underlying"].first_valid_index()]
  excluded_columns = ["strikePrice", "expiryDate", "underlying"]
  unnested_pe.drop(columns=excluded_columns, inplace=True)
  unnested_ce.drop(columns=excluded_columns, inplace=True)
  unnested_pe.columns = ["PE_" + col for col in unnested_pe.columns]
  unnested_ce.columns = ["CE_" + col for col in unnested_ce.columns]
  return pd.concat([df.reset_index(drop=True), unnested_pe.reset_index(drop=True), unnested_ce.reset_index(drop=True)], axis=1).drop(columns=["PE", "CE"])

def extract_data_between_strike(df, low: int, high: int):
  return df[(df["strikePrice"] <= high) & (df["strikePrice"] >= low)]

def extract_data_between_strike_count(records, df, count: int):
  low, high = weighted_strikeprice(records = records, count=count)
  return df[(df["strikePrice"] <= high) & (df["strikePrice"] >= low)]

def extract_data_of_expiry(df, expiry):
  return df[df["expiryDate"] == expiry]


def extract_interested_df(symbol, expiryDate=None, nse_records=None, count=None, low=None, high=None):
  if (isinstance(nse_records, type(None))):
    nse_records = download_nse_option_chain_indices_records(symbol)
  if (nse_records['data'][0]['PE']['underlying'] != symbol):
    raise Exception("Invalid Data, symbol not matching")
  df = unnest_pe_ce(convert_records_to_dataframe(nse_records))
  if (isinstance(expiryDate, type(None))):
    expiryDate = next_expiry(nse_records["expiryDates"])
  df_expiry = extract_data_of_expiry(df, expiryDate)
  if isinstance(count, int):
    low, high = weighted_strikeprice(records = nse_records, count=count)
  if isinstance(low, int) and isinstance(high, int):
    return extract_data_between_strike(df_expiry, low, high)
  raise Exception("Invalid")

def current_status(symbol, expiryDate=None, nse_records=None, count=2):
  if (isinstance(nse_records, type(None))):
    nse_records = download_nse_option_chain_indices_records(symbol)
  if (nse_records['data'][0]['PE']['underlying'] != symbol):
    raise Exception("Invalid Data, symbol not matching", symbol, nse_records['data'][0]['PE']['underlying'])
  print(f"Symbol: {symbol}")
  print(f"Current: {nse_records['underlyingValue']}")
  print(f"ATM: {ATM_strike(nse_records)}")
  print(f"Near By Strike Price: {near_by_strikeprices(nse_records, count=2)}")
  print(f"PCR oi change: {option_chain_oi_change_pcr(extract_interested_df(symbol, nse_records=nse_records, count=count))}")



if __name__ == "__main__":
  nse_records = download_nse_option_chain_indices_records("NIFTY")
  option_chain_oi_change_pcr(extract_interested_df("NIFTY", count=2))
  # option_chain_oi_change_pcr(extract_interested_df("NIFTY", low=23350, high=23550))
