import pandas as pd
import janitor

url = "https://raw.githubusercontent.com/rahultejannavar/rawdata_indiaschool/refs/heads/main/Test%202%20-%20Sheet1.csv"
df = pd.read_csv(url, quoting=3)

df.iloc[:, 0] = df.iloc[:, 0].str.lstrip('"')
df.iloc[:, -1] = df.iloc[:, -1].str.rstrip('"')
df = df.clean_names(remove_special='"')

df_long = pd.melt(df, id_vars = "state_ut")
df_long[['level', 'category']] = df_long['variable'].str.split('_', expand=True)
