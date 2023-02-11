# concate with respect to specific columns
pd.concat([df1.set_index('A'),df2.set_index('A')], axis=1, join='inner').reset_index()

# to sum columns in dataframe
df['variance'] = df[['budget','actual']].sum(axis=1)