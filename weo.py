import pandas as pd

country = pd.read_csv("clean/weo-simple.csv") 
economics = pd.read_excel("data/weo-2025-04-full.xls", engine='xlrd')
economics = pd.read_csv("data/weo-2025-04-full.xls", sep='\t')
