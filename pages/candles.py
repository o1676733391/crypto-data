import streamlit as st
import pandas as pd

def main():
    st.title("OHLCV Candle Charts")
    df = pd.read_csv("snowflake_export/table__CANDLES_DAILY.csv")
    st.dataframe(df)
    symbols = df['SYMBOL'].unique()
    selected_symbol = st.selectbox("Select Symbol", symbols)
    df_selected = df[df['SYMBOL'] == selected_symbol]
    st.line_chart(df_selected.set_index('CANDLE_TIME')[['OPEN','HIGH','LOW','CLOSE','VOLUME']])

if __name__ == "__main__":
    main()
