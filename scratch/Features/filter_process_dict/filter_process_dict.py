_=""" Filter table dataframe into Dictionary as process with tasks """
with st.expander("fitler to dict"):
    df_dic=filter_df.set_index('parameter').to_dict(orient='index')
    st.write(df_dic)