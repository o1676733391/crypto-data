import streamlit as st
import pandas as pd

def main():
    st.title("Blockchain Chain Dominance")
    df = pd.read_csv("snowflake_export/table__CHAIN_TVL.csv")
    st.dataframe(df)
    chains = df['CHAIN_NAME'].unique()
    selected_chain = st.selectbox("Select Chain", chains)
    df_selected = df[df['CHAIN_NAME'] == selected_chain]
    st.line_chart(df_selected.set_index('TIMESTAMP')['TVL'])

if __name__ == "__main__":
    main()
