import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.pyplot import style
style.use('ggplot')

web_status = {'Day':[1,2,3,4,5,6],
        'Visitors':[43,53,34,45,64,34],
        'Bounce_Rate':[65,72,62,64,54,66]}

df = pd.DataFrame(web_status)

# print(df)
# print(df.head())
# print(df.tail())
# print(df.tail(2))

# print(df.set_index('Day'))
# print(df.head())

# df2 = df.set_index('Day')
df.set_index('Day',inplace=True)
# print(df.head())

# print(df['Visitors'])
# print(df.Visitors)

print(df[['Visitors','Bounce_Rate']])
