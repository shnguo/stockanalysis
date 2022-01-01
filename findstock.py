
import pandas as pd
from pprint import pprint
pd.options.display.max_rows = None
pd.options.display.max_columns = None
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

df_result = pd.read_csv('./data/stock_result_thread.csv', index_col=0,dtype={'代码':str})
df_result['今日主力净流入-净占比'].fillna(-1,inplace=True)
df_result['今日主力净流入-净占比'] = df_result['今日主力净流入-净占比'].replace('-',-1)
df_result['今日主力净流入-净占比'] = df_result['今日主力净流入-净占比'].astype('float64')
#print(df_result[df_result.名称=='新时达'].T)
df_result2 = df_result[(df_result.MA5.round(2) >= df_result.MA5_1.round(2))
                       & (df_result.MA10.round(2) >= df_result.MA10_1.round(2)) &
                       (df_result.MA20.round(2) >= df_result.MA20_1.round(2)) &
                       (df_result.MA30.round(2) >= df_result.MA30_1.round(2)) &
                       (df_result.MA60.round(2) >= df_result.MA60_1.round(2)) & (df_result.涨跌幅>0.0)&(df_result['今日主力净流入-净占比']>0.0)].sort_values(
                           [
                               'MA60_score',
                               'MA30_score',
                               'MA20_score',
                               'MA10_score',
                               'MA5_score',
                           ],
                           ascending=False).head(100)
print('today:')
df_result2.rename(columns={'今日主力净流入-净占比':'主力'},inplace=True)
pprint(df_result2[[
    '名称','涨跌幅','最新价','主力', 'MA60_score', 'MA30_score', 'MA20_score', 'MA10_score', 'MA5_score',
    'MA5', 'MA5_1', 'MA10', 'MA10_1', 'MA20', 'MA20_1', 'MA30', 'MA30_1',
    'MA60', 'MA60_1'
]])



df_result3 = df_result[(df_result.MA5 >= df_result.MA5_1)
                       & (df_result.MA10 >= df_result.MA10_1) &
                       (df_result.MA20 >= df_result.MA20_1) &
                       (df_result.MA30 >= df_result.MA30_1) &
                       (df_result.MA60 >= df_result.MA60_1) & (df_result.涨跌幅>0.0) &(df_result['今日主力净流入-净占比']>0.0)].sort_values(
                           [
                               'T_MA60_score',
                               'T_MA30_score',
                               'T_MA20_score',
                               'T_MA10_score',
                               'T_MA5_score',
                               '涨跌幅'
                           ],
                           ascending=False).head(20)


