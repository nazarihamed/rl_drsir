import pandas as pd
import setting

file = setting.PATH_TO_FILES +'net_info.csv'
df = pd.read_csv(file)
df.delay = 0
df.pkloss = 0

print(df)