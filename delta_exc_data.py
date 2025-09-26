import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import os

# --- Configuration ---
BASE_URL = "https://api.india.delta.exchange" 

# --- API Keys (not strictly needed for public OHLC data or top symbols) ---
# For public historical data (like candles) and fetching tickers, API key/secret
# are usually NOT required. They are only for authenticated actions
# (e.g., placing orders, getting balances).


def get_all_products(base_url: str) -> list:
    """
    Retrieves a list of all tradable products/symbols from Delta Exchange.

    Args:
        base_url (str): The base URL for the Delta Exchange API.

    Returns:
        list: A list of dictionaries, where each dictionary represents a product.
              Returns an empty list if an error occurs or no products are found.
    """
    endpoint = f"{base_url}/v2/products"
    try:
        print(f"Fetching all products from: {endpoint}...")
        response = requests.get(endpoint, timeout=10)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        json_response = response.json()

        if 'result' in json_response and isinstance(json_response['result'], list):
            products = json_response['result']
            print(f"Successfully fetched {len(products)} products.")
            return products
        else:
            print(f"Error: Unexpected response format for products. Received: {json_response}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Network or API error fetching products: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred fetching products: {e}")
        return []

def get_all_tickers(base_url: str) -> list:
    """
    Retrieves ticker data for all products from Delta Exchange.
    Ticker data includes 24-hour volume.

    Args:
        base_url (str): The base URL for the Delta Exchange API.

    Returns:
        list: A list of dictionaries, where each dictionary represents ticker data.
              Returns an empty list if an error occurs or no tickers are found.
    """
    endpoint = f"{base_url}/v2/tickers"
    try:
        print(f"Fetching all tickers from: {endpoint}...")
        response = requests.get(endpoint, timeout=10)
        response.raise_for_status()
        json_response = response.json()

        if 'result' in json_response and isinstance(json_response['result'], list):
            tickers = json_response['result']
            print(f"Successfully fetched {len(tickers)} tickers.")
            return tickers
        else:
            print(f"Error: Unexpected response format for tickers. Received: {json_response}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Network or API error fetching tickers: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred fetching tickers: {e}")
        return []

def get_top_n_symbols_by_volume(n: int = 10) -> pd.DataFrame:
    """
    Fetches all tickers, sorts them by 24-hour volume (turnover_usd), and returns the top N symbols.

    Args:
        n (int): The number of top symbols to retrieve. Default is 10.

    Returns:
        pd.DataFrame: A DataFrame containing the top N symbols with their relevant
                      ticker information, sorted by 24h turnover in USD descending.
                      Returns an empty DataFrame if no data or an error occurs.
    """
    tickers = get_all_tickers(BASE_URL)

    if not tickers:
        print("Could not retrieve any ticker data.")
        return pd.DataFrame()

    df = pd.DataFrame(tickers)
    
    VOLUME_COLUMN = 'turnover_usd' 
    
    if VOLUME_COLUMN not in df.columns:
        print(f"Error: '{VOLUME_COLUMN}' column not found in ticker data. Cannot sort by volume.")
        print("Available ticker columns:", df.columns.tolist())
        return pd.DataFrame()

    df[VOLUME_COLUMN] = pd.to_numeric(df[VOLUME_COLUMN], errors='coerce')
    df.dropna(subset=[VOLUME_COLUMN], inplace=True)
    df_sorted = df.sort_values(by=VOLUME_COLUMN, ascending=False)
    
    top_n_symbols = df_sorted.head(n).copy()
    
    desired_cols = ['symbol', VOLUME_COLUMN, 'volume', 'last_price', 'close', 'open', 'high', 'low']
    existing_cols = [col for col in desired_cols if col in top_n_symbols.columns]
    
    return top_n_symbols[existing_cols]


def get_ohlc_data_paginated(
    symbol: str,
    resolution: str,
    start_time_dt: datetime,
    end_time_dt: datetime,
    limit_per_request: int = 2000 # Max limit per API call for Delta's candles endpoint
) -> pd.DataFrame:
    """
    Retrieves OHLC (candlestick) data for a given symbol and time range from Delta Exchange,
    handling pagination and returning a Pandas DataFrame.

    Args:
        symbol (str): The trading symbol (e.g., "BTCUSD").
        resolution (str): The candle timeframe (e.g., "1m", "1h", "1d").
        start_time_dt (datetime): The start datetime object (UTC-aware).
        end_time_dt (datetime): The end datetime object (UTC-aware).
        limit_per_request (int): Max number of candles to request per API call (default 2000).

    Returns:
        pd.DataFrame: A DataFrame with OHLCV data, or an empty DataFrame if an error occurs or no data.
                      Columns: 'time' (datetime), 'open', 'high', 'low', 'close', 'volume'.
    """
    all_candles = []
    
    overall_start_timestamp = int(start_time_dt.timestamp())
    overall_end_timestamp = int(end_time_dt.timestamp())

    print(f"\n--- Starting Paginated OHLC Data Fetch for {symbol} ({resolution}) ---")
    print(f"  Requested Range: {start_time_dt.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_time_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  (Note: Delta Exchange historical data is generally available from March 30, 2020 onwards.)")

    current_chunk_start_timestamp = overall_start_timestamp

    while current_chunk_start_timestamp <= overall_end_timestamp:
        try:
            print(f"  Fetching chunk from {datetime.fromtimestamp(current_chunk_start_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}...")
            
            params = {
                'symbol': symbol,
                'resolution': resolution,
                'start': current_chunk_start_timestamp,
                'end': overall_end_timestamp,
                'limit': limit_per_request
            }

            response = requests.get(
                f"{BASE_URL}/v2/history/candles", 
                params=params, 
                timeout=10 # Set a timeout for the request
            )
            response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
            
            json_response = response.json()
            
            if 'result' in json_response and isinstance(json_response['result'], list):
                candles_page = json_response['result']
            else:
                print(f"  Error: Unexpected response format. Expected 'result' key with a list. Received: {json_response}. Breaking fetch.")
                break

            if not candles_page:
                print("  No more candles found in this segment or within the valid range. Ending fetch.")
                break

            all_candles.extend(candles_page)
            print(f"  Fetched {len(candles_page)} candles in this chunk. Total so far: {len(all_candles)}")
            
            last_candle_time = candles_page[-1]['time']
            
            if last_candle_time >= overall_end_timestamp:
                print("  Reached or surpassed the overall end time. Ending fetch.")
                break

            current_chunk_start_timestamp = last_candle_time + 1
            
            if len(candles_page) < limit_per_request:
                print("  Less than limit received, likely fetched all candles in the remaining range. Ending fetch.")
                break

            time.sleep(0.1) 

        except requests.exceptions.RequestException as e:
            print(f"  Network or API error during pagination: {e}")
            break
        except Exception as e:
            print(f"  An unexpected error occurred during pagination: {e}")
            break

    if all_candles:
        df = pd.DataFrame(all_candles)
        df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
        df.drop_duplicates(subset=['time'], inplace=True)
        df.sort_values(by='time', inplace=True)
        
        df = df[(df['time'] >= start_time_dt) & (df['time'] <= end_time_dt)].copy()
        
        output_df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
        return output_df
    
    return pd.DataFrame()


def get_user_input_symbol(available_symbols: set) -> str:
    """
    Prompts the user to input a trading symbol and validates it.
    """
    while True:
        user_symbol = input("\nEnter the trading symbol (e.g., BTCUSD, ETHUSD): ").strip().upper()
        if user_symbol in available_symbols:
            return user_symbol
        else:
            print(f"Error: '{user_symbol}' is not a valid or currently listed symbol. Please try again.")
            print(f"Available symbols count: {len(available_symbols)}")
            if len(available_symbols) > 0:
                print("Some examples:", list(available_symbols)[:5]) # Show a few examples

def get_user_input_date_range() -> tuple[datetime, datetime]:
    """
    Prompts the user to input a start and end year and returns corresponding datetime objects.
    """
    while True:
        try:
            start_year = int(input("Enter the START year (e.g., 2020): "))
            end_year = int(input("Enter the END year (e.g., 2024): "))

            if not (1900 <= start_year <= datetime.now().year and 1900 <= end_year <= datetime.now().year):
                print(f"Error: Years must be between 1900 and the current year ({datetime.now().year}).")
                continue
            
            if start_year > end_year:
                print("Error: Start year cannot be after end year.")
                continue

            # Delta Exchange historical data generally available from March 30, 2020 onwards.
            # Give a warning if the requested range starts before this.
            if start_year < 2020: # Or more specifically 2020, 3, 30
                print("\nWarning: Delta Exchange historical data generally starts from March 30, 2020.")
                print("         You may not get data for years before this date.")
                # Adjust start_date_obj to March 30, 2020 if the user enters a much earlier year
                if start_year < 2020:
                     print("         Adjusting start to 2020-03-30 for maximum available history.")
                     # We still set it to the user's start_year, but this warns them
                     # The actual API call will get data from 2020-03-30 if available.
                start_date_obj = datetime(start_year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            else:
                start_date_obj = datetime(start_year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            
            end_date_obj = datetime(end_year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

            # Ensure end_date_obj doesn't exceed current time to avoid requesting future data
            current_utc_time = datetime.now(timezone.utc)
            if end_date_obj > current_utc_time:
                print(f"Warning: End date ({end_date_obj.year}) is in the future. Adjusting to end of current day ({current_utc_time.strftime('%Y-%m-%d')}).")
                end_date_obj = current_utc_time.replace(hour=23, minute=59, second=59, microsecond=0)

            return start_date_obj, end_date_obj
        
        except ValueError:
            print("Invalid input. Please enter a valid year (e.g., 2023).")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


# --- Main execution block ---
if __name__ == "__main__":
    
    print("\n--- Delta Exchange OHLC Data Fetcher ---")

    # 1. Get list of all available symbols for validation
    all_products = get_all_products(BASE_URL)
    available_symbols_set = {p['symbol'] for p in all_products} if all_products else set()

    if not available_symbols_set:
        print("Could not retrieve list of available symbols. Exiting.")
        exit()

    # 2. Get symbol input from user
    target_symbol = get_user_input_symbol(available_symbols_set)
    target_resolution = "1d" # Fixed to daily candles as per earlier discussions

    # 3. Get date range input from user
    start_date_obj, end_date_obj = get_user_input_date_range()

    print(f"\n--- Preparing to fetch OHLC data for {target_symbol} ({target_resolution}) ---")
    print(f"  Requested range: {start_date_obj.strftime('%Y-%m-%d')} to {end_date_obj.strftime('%Y-%m-%d')}")

    ohlc_df = get_ohlc_data_paginated(
        target_symbol,
        target_resolution,
        start_date_obj,
        end_date_obj
    )

    if not ohlc_df.empty:
        print(f"\n--- Retrieved OHLC Data for {target_symbol} ({target_resolution}) ---")
        print(f"Total candles fetched: {len(ohlc_df)}")
        print("\nOHLC data (first 5 rows):")
        print(ohlc_df.head())
        print("\nOHLC data (last 5 rows):")
        print(ohlc_df.tail())
        
        print(f"\nActual data starts: {ohlc_df['time'].min()}")
        print(f"Actual data ends:   {ohlc_df['time'].max()}")

        # --- Save to CSV ---
        start_str = start_date_obj.strftime('%Y%m%d')
        end_str = end_date_obj.strftime('%Y%m%d')
        output_filename = f"{target_symbol}_{target_resolution}_{start_str}_{end_str}_ohlc.csv"
        
        try:
            ohlc_df.to_csv(output_filename, index=False)
            print(f"\n--- Data successfully saved to {output_filename} ---")
        except Exception as e:
            print(f"\nError saving data to CSV: {e}")

    else:
        print(f"\nFailed to retrieve any OHLC data for {target_symbol} for the range {start_date_obj.year}-{end_date_obj.year}.")
        print("Please check:")
        print("1. API availability and your internet connection.")
        print("2. That the symbol was actively traded during the requested period.")
        print("3. That Delta Exchange holds historical data for this symbol/range (remember data generally starts March 30, 2020).")
        print("   If you selected a very old year, try a more recent range (e.g., 2023-2024).")