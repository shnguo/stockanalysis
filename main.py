
import pandas as pd
import akshare as ak
import matplotlib.dates as mdates
from MyTT import *
import threading
from datetime import datetime
from time import sleep
_today = datetime.now().strftime('%Y-%m-%d')
print(_today)
stock_list_df = pd.read_csv('./data/stock_list.csv', index_col=0,dtype={'代码':str})
#print(stock_list_df.head())
stock_list_df['a1'] = 0.0001
stock_list_df['MA5'] = 0.0001
stock_list_df['MA10'] = 0.0001
stock_list_df['MA20'] = 0.0001
stock_list_df['MA30'] = 0.0001
stock_list_df['MA60'] = 0.0001
stock_list_df['MA5_1'] = 0.0001
stock_list_df['MA10_1'] = 0.0001
stock_list_df['MA20_1'] = 0.0001
stock_list_df['MA30_1'] = 0.0001
stock_list_df['MA60_1'] = 0.0001
stock_list_df['MA5_score'] = 0.0001
stock_list_df['MA10_score'] = 0.0001
stock_list_df['MA20_score'] = 0.0001
stock_list_df['MA30_score'] = 0.0001
stock_list_df['MA60_score'] = 0.0001
stock_list_df['T_MA5_score'] = 0.0001
stock_list_df['T_MA10_score'] = 0.0001
stock_list_df['T_MA20_score'] = 0.0001
stock_list_df['T_MA30_score'] = 0.0001
stock_list_df['T_MA60_score'] = 0.0001
a1_index = stock_list_df.columns.get_loc('a1')
MA5_index = stock_list_df.columns.get_loc('MA5')
MA10_index = stock_list_df.columns.get_loc('MA10')
MA20_index = stock_list_df.columns.get_loc('MA20')
MA30_index = stock_list_df.columns.get_loc('MA30')
MA60_index = stock_list_df.columns.get_loc('MA60')
MA5_1_index = stock_list_df.columns.get_loc('MA5_1')
MA10_1_index = stock_list_df.columns.get_loc('MA10_1')
MA20_1_index = stock_list_df.columns.get_loc('MA20_1')
MA30_1_index = stock_list_df.columns.get_loc('MA30_1')
MA60_1_index = stock_list_df.columns.get_loc('MA60_1')
MA5_score_index = stock_list_df.columns.get_loc('MA5_score')
MA10_score_index = stock_list_df.columns.get_loc('MA10_score')
MA20_score_index = stock_list_df.columns.get_loc('MA20_score')
MA30_score_index = stock_list_df.columns.get_loc('MA30_score')
MA60_score_index = stock_list_df.columns.get_loc('MA60_score')
T_MA5_score_index = stock_list_df.columns.get_loc('T_MA5_score')
T_MA10_score_index = stock_list_df.columns.get_loc('T_MA10_score')
T_MA20_score_index = stock_list_df.columns.get_loc('T_MA20_score')
T_MA30_score_index = stock_list_df.columns.get_loc('T_MA30_score')
T_MA60_score_index = stock_list_df.columns.get_loc('T_MA60_score')
#print(stock_list_df.head())

lock = threading.Lock()


class MyThreading(threading.Thread):
    def __init__(self, func, arg):
        super(MyThreading, self).__init__()
        self.func = func
        self.arg = arg

    def run(self):
        self.func(*self.arg)


def calculate(row, index, a1_index, MA5_index, MA10_index, MA20_index,
              MA30_index, MA60_index, MA5_1_index, MA10_1_index, MA20_1_index,
              MA30_1_index, MA60_1_index, MA5_score_index, MA10_score_index,
              MA20_score_index, MA30_score_index, MA60_score_index,
              T_MA5_score_index, T_MA10_score_index, T_MA20_score_index,
              T_MA30_score_index, T_MA60_score_index, df_target, lk):
    print(row['名称'])
    df= ak.stock_zh_a_hist(symbol=row['代码'], period="daily", start_date="20200101")
    df.columns = [
        'date', 'open', 'close', 'high', 'low', 'volume', 'gmv', 'amplitude',
        'up_down', 'rise_fall', 'turnover'
    ]
    df.date = pd.to_datetime(df.date)
    df.set_index(df.date, inplace=True)
    df.drop(labels='date', inplace=True, axis=1)
    #print(df.tail(5))
    CLOSE = df.close.values
    OPEN = df.open.values  #基础数据定义，只要传入的是序列都可以
    HIGH = df.high.values
    LOW = df.low.values

    #print( df.index[-1]==datetime.strptime('2021-12-24','%Y-%m-%d'))
    if df.index[-1].day!=datetime.today().day:
        print('not open day')
        CLOSE= np.append(CLOSE,row['最新价'])
        OPEN =  np.append(OPEN,row['今开'])
        HIGH =  np.append(HIGH,row['最高'])
        LOW = np.append(LOW,row['最低'])


    Var1 = EMA(HHV(HIGH, 500), 21)
    Var2 = EMA(HHV(HIGH, 250), 21)
    Var3 = EMA(HHV(HIGH, 90), 21)
    Var4 = EMA(LLV(LOW, 500), 21)
    Var5 = EMA(LLV(LOW, 250), 21)
    Var6 = EMA(LLV(LOW, 90), 21)
    Var7 = EMA((Var4 * 0.96 + Var5 * 0.96 + Var6 * 0.96 + Var1 * 0.558 +
                Var2 * 0.558 + Var3 * 0.558) / 6, 21)
    Var8 = EMA((Var4 * 1.25 + Var5 * 1.23 + Var6 * 1.2 + Var1 * 0.55 +
                Var2 * 0.55 + Var3 * 0.65) / 6, 21)
    Var9 = EMA((Var4 * 1.3 + Var5 * 1.3 + Var6 * 1.3 + Var1 * 0.68 +
                Var2 * 0.68 + Var3 * 0.68) / 6, 21)
    VarA = EMA((Var7 * 3 + Var8 * 2 + Var9) / 6 * 1.738, 21)
    VarB = REF(LOW, 1)
    VarC = SMA(ABS(LOW - VarB), 3, 1) / SMA(MAX(LOW - VarB, 0), 3, 1) * 100
    VarD = EMA(IF(CLOSE * 1.35 <= VarA, VarC * 10, VarC / 10), 3)
    VarE = LLV(LOW, 30)
    VarF = HHV(VarD, 30)
    Var10 = IF(MA(CLOSE, 58), 1, 0)
    zijin = MA(IF(LOW <= VarE, (VarD + VarF * 2) / 2, 0), 3) / 618 * Var10
    tmp = IF(zijin > 0, zijin * 1.2, 0)

    MA5 = MA(CLOSE, 5)
    MA10 = MA(CLOSE, 10)
    MA20 = MA(CLOSE, 20)
    MA30 = MA(CLOSE, 30)
    MA60 = MA(CLOSE, 60)
    MA5_1 = REF(MA5, 1)
    MA10_1 = REF(MA10, 1)
    MA20_1 = REF(MA20, 1)
    MA30_1 = REF(MA30, 1)
    MA60_1 = REF(MA60, 1)
    lk.acquire()
    df_target.iloc[index, a1_index] = tmp[-1]
    df_target.iloc[index, MA5_index] = MA5[-1]
    df_target.iloc[index, MA10_index] = MA10[-1]
    df_target.iloc[index, MA20_index] = MA20[-1]
    df_target.iloc[index, MA30_index] = MA30[-1]
    df_target.iloc[index, MA60_index] = MA60[-1]
    df_target.iloc[index, MA5_1_index] = MA5_1[-1]
    df_target.iloc[index, MA10_1_index] = MA10_1[-1]
    df_target.iloc[index, MA20_1_index] = MA20_1[-1]
    df_target.iloc[index, MA30_1_index] = MA30_1[-1]
    df_target.iloc[index, MA60_1_index] = MA60_1[-1]
    df_target.iloc[index, MA5_score_index] = 5 if MA5[-1] >= LOW[-1] and MA5[
        -1] <= HIGH[-1] else 0
    df_target.iloc[index, MA10_score_index] = 10 if MA10[-1] >= LOW[
        -1] and MA10[-1] <= HIGH[-1] else 0
    df_target.iloc[index, MA20_score_index] = 20 if MA20[-1] >= LOW[
        -1] and MA20[-1] <= HIGH[-1] else 0
    df_target.iloc[index, MA30_score_index] = 30 if MA30[-1] >= LOW[
        -1] and MA30[-1] <= HIGH[-1] else 0
    df_target.iloc[index, MA60_score_index] = 60 if MA60[-1] >= LOW[
        -1] and MA60[-1] <= HIGH[-1] else 0
    df_target.iloc[index, T_MA5_score_index] = 5 if MA5[-1] >= CLOSE[
        -1] and MA5[-1] <= CLOSE[-1] * 1.1 else 0
    df_target.iloc[index, T_MA10_score_index] = 10 if MA10[-1] >= CLOSE[
        -1] and MA10[-1] <= CLOSE[-1] * 1.1 else 0
    df_target.iloc[index, T_MA20_score_index] = 20 if MA20[-1] >= CLOSE[
        -1] and MA20[-1] <= CLOSE[-1] * 1.1 else 0
    df_target.iloc[index, T_MA30_score_index] = 30 if MA30[-1] >= CLOSE[
        -1] and MA30[-1] <= CLOSE[-1] * 1.1 else 0
    df_target.iloc[index, T_MA60_score_index] = 60 if MA60[-1] >= CLOSE[
        -1] and MA60[-1] <= CLOSE[-1] * 1.1 else 0
    lk.release()


thread_pool = []
for index, row in list(stock_list_df.iterrows())[:]:
    thread_pool.append(
        MyThreading(func=calculate,
                    arg=(row, index, a1_index, MA5_index, MA10_index,
                         MA20_index, MA30_index, MA60_index, MA5_1_index,
                         MA10_1_index, MA20_1_index, MA30_1_index,
                         MA60_1_index, MA5_score_index, MA10_score_index,
                         MA20_score_index, MA30_score_index, MA60_score_index,
                         T_MA5_score_index, T_MA10_score_index,
                         T_MA20_score_index, T_MA30_score_index,
                         T_MA60_score_index, stock_list_df, lock)))
for _th in thread_pool:
    _th.start()
    sleep(0.04)
for _th in thread_pool:
    _th.join()
stock_list_df.to_csv('./data/stock_result_thread.csv')
print(stock_list_df.head())

