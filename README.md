# Data_Extracton
A code that fetches desired data form delta exchange 
# 📊 Delta Exchange Data Extraction

A Python tool to fetch **cryptocurrency OHLCV data** (candlesticks) from the [Delta Exchange API](https://docs.delta.exchange/).  
This project retrieves daily OHLC data for a user-specified trading pair (e.g., `BTCUSD`) over a chosen date range and saves it as a **CSV file**.

---

## 🚀 Features
- Fetches all available trading symbols from Delta Exchange.
- Validates user input against the current list of active products.
- Retrieves **OHLCV (Open, High, Low, Close, Volume)** data.
- Handles **API pagination** automatically.
- Saves the extracted data as a **CSV** for further analysis.
- Includes warnings for unavailable date ranges (data available only from March 30, 2020 onwards).

---

## 🛠️ Tech Stack
- **Python 3.x**
- [requests](https://pypi.org/project/requests/) → for API calls  
- [pandas](https://pypi.org/project/pandas/) → for data handling and CSV export  

---

## 📦 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/delta-extraction.git
   cd delta-extraction
