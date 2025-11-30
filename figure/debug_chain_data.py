import pandas as pd
import os

def debug_data():
    csv_path = r'd:\postgresql\crypto-data\snowflake_export\table__CHAIN_TVL.csv'
    
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    print("Loading data...")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    print("Columns:", df.columns.tolist())
    print("\nFirst 5 rows raw:")
    print(df[['CHAIN_NAME', 'TVL', 'TIMESTAMP']].head())
    
    print("\nData Types:")
    print(df.dtypes)
    
    # Attempt parsing
    print("\nParsing columns...")
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
    df['TVL'] = pd.to_numeric(df['TVL'], errors='coerce')
    
    print("\nData Types after parsing:")
    print(df.dtypes)
    
    print("\nNull values after parsing:")
    print(df[['CHAIN_NAME', 'TVL', 'TIMESTAMP']].isnull().sum())
    
    print("\nTVL Stats:")
    print(df['TVL'].describe())
    
    # Check latest snapshot logic
    df = df.dropna(subset=['TVL', 'TIMESTAMP'])
    df = df.sort_values('TIMESTAMP')
    latest_snapshot = df.groupby('CHAIN_NAME').tail(1)
    latest_snapshot = latest_snapshot.sort_values('TVL', ascending=False)
    
    print("\nTop 5 Chains by TVL (Latest Snapshot):")
    print(latest_snapshot[['CHAIN_NAME', 'TVL', 'TIMESTAMP']].head())
    
    print(f"\nTotal TVL: {latest_snapshot['TVL'].sum()}")

if __name__ == "__main__":
    debug_data()
