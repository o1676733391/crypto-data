import streamlit as st
import pandas as pd

def main():
    st.title("Technical Analysis Dashboard")
    df = pd.read_csv("snowflake_export/table__TECHNICAL_INDICATORS.csv")
    st.dataframe(df)
    symbols = df['SYMBOL'].unique()
    selected_symbol = st.selectbox("Select Symbol", symbols)
    df_selected = df[df['SYMBOL'] == selected_symbol]
    st.line_chart(df_selected.set_index('EXCHANGE_TS')[['RSI_14','MACD','MACD_SIGNAL']])

if __name__ == "__main__":
    main()
