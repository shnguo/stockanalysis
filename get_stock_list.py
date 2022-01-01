import akshare as ak
import pandas as pd
stock_zh_a_spot_df = ak.stock_zh_a_spot_em()
#print(stock_zh_a_spot_df.head())
stock_individual_fund_flow_rank_df = ak.stock_individual_fund_flow_rank(indicator="今日")
df_result = pd.merge(stock_zh_a_spot_df,stock_individual_fund_flow_rank_df,how='left',left_on='代码',right_on='代码',suffixes=('', '_y'))
print(df_result.head(3).T)
df_result.to_csv('./data/stock_list.csv')
