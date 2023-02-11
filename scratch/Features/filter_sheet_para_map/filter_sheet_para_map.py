
def paramter_map(test_df,filter_parameter_df):
    columns=test_df.columns.to_list()
    #st.write(columns)
    columns_r=[x.replace("_", " ") for x in columns]
    #st.write(columns_r)

    match_sheet_parameter={}
    for parameter in filter_parameter_df:
        arr=[]
        for x in columns_r:
            if parameter in x:
                arr.append(x)
                #st.write(sheet_param)
        match_sheet_parameter[parameter]=[x.replace(" ", "_") for x in arr]
    return match_sheet_parameter