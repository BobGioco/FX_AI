def bollinger_bands(data, time_label="Time", close_label="Close",window_size=20):
    """Bollinger bands
    data -> pandas data frame
    time_label & close_label -> columns names in data - default: time_label="Time", close_label="Close"
    window_size -> period of moving avg, default is 20 prior records
    
    >>> df[20:26]
                        Time     Open    Close
    20      2010-01-01 05:00  1.43237  1.43268
    21      2010-01-01 05:15  1.43275  1.43277
    22      2010-01-01 05:30  1.43214  1.43268
    23      2010-01-01 05:45  1.43253  1.43303
    24      2010-01-01 06:00  1.43283  1.43334
    25      2010-01-01 06:15  1.43331  1.43326
    
    >>> df=bollinger_bands(df,time_label="Time", close_label="Close")
    >>> df[20:26]
    
                        Time     Open    Close  upper_BL_band  lower_BL_band
    20      2010-01-01 05:00  1.43237  1.43268       1.433183       1.432235
    21      2010-01-01 05:15  1.43275  1.43277       1.433185       1.432235
    22      2010-01-01 05:30  1.43214  1.43268       1.433176       1.432231
    23      2010-01-01 05:45  1.43253  1.43303       1.433211       1.432223
    24      2010-01-01 06:00  1.43283  1.43334       1.433309       1.432177
    25      2010-01-01 06:15  1.43331  1.43326       1.433368       1.432152
    """
    num_of_std=2
    data_bands=data[["Time","Close"]].copy()
    data_bands['rolling_mean'] = data_bands['Close'].rolling(window=window_size).mean()
    data_bands['rolling_std'] = data_bands['Close'].rolling(window=window_size).std()
    data_bands['upper_BL_band'] = data_bands['rolling_mean']  + (data_bands['rolling_std']*num_of_std)
    data_bands['lower_BL_band'] = data_bands['rolling_mean']  - (data_bands['rolling_std']*num_of_std)
    
    data=data.merge(data_bands[[time_label,'upper_BL_band','lower_BL_band']],how="left",on=time_label)
    return data

def psar(data, time_label='Time',ohlc_labels=['Open','High','Low','Close'],iaf = 0.02, maxaf = 0.2):
    """Parabolic SAR
    data -> pandas data frame
    time_label -> name of the column with timestamp - default='Time'
    ohlc -> labels names for open, high, low and close - default=['Open','High','Low','Close']
    iaf -> Acceleration Factor determines the sensitivity of the SAR
    maxaf -> Maximum Acceleration Factor
    
    >>> df[15:21]
                        Time     Open     High      Low    Close
    15      2010-01-01 03:45  1.43284  1.43293  1.43181  1.43209
    16      2010-01-01 04:00  1.43218  1.43302  1.43182  1.43273
    17      2010-01-01 04:15  1.43263  1.43294  1.43204  1.43223
    18      2010-01-01 04:30  1.43239  1.43302  1.43197  1.43282
    19      2010-01-01 04:45  1.43284  1.43295  1.43220  1.43236
    20      2010-01-01 05:00  1.43237  1.43294  1.43199  1.43268
    
    >>> df=psar(df, time_label='Time',ohlc_labels=['Open','High','Low','Close'])
    
    >>> df[15:21]
                        Time     Open     High      Low    Close      psar
    15      2010-01-01 03:45  1.43284  1.43293  1.43181  1.43209  1.433060
    16      2010-01-01 04:00  1.43218  1.43302  1.43182  1.43273  1.431810
    17      2010-01-01 04:15  1.43263  1.43294  1.43204  1.43223  1.431810
    18      2010-01-01 04:30  1.43239  1.43302  1.43197  1.43282  1.431820
    19      2010-01-01 04:45  1.43284  1.43295  1.43220  1.43236  1.431844
    20      2010-01-01 05:00  1.43237  1.43294  1.43199  1.43268  1.431868
    """
    
    barsdata=data[[time_label]+ohlc_labels].copy()
    length = len(barsdata)
    dates = list(barsdata['Time'])
    high = list(barsdata['High'])
    low = list(barsdata['Low'])
    close = list(barsdata['Close'])
    psar = close[0:len(close)]
    psarbull = [None] * length
    psarbear = [None] * length
    bull = True
    af = iaf
    ep = low[0]
    hp = high[0]
    lp = low[0]
    for i in range(2,length):
        if bull:
            psar[i] = psar[i - 1] + af * (hp - psar[i - 1])
        else:
            psar[i] = psar[i - 1] + af * (lp - psar[i - 1])
        reverse = False
        if bull:
            if low[i] < psar[i]:
                bull = False
                reverse = True
                psar[i] = hp
                lp = low[i]
                af = iaf
        else:
            if high[i] > psar[i]:
                bull = True
                reverse = True
                psar[i] = lp
                hp = high[i]
                af = iaf
        if not reverse:
            if bull:
                if high[i] > hp:
                    hp = high[i]
                    af = min(af + iaf, maxaf)
                if low[i - 1] < psar[i]:
                    psar[i] = low[i - 1]
                if low[i - 2] < psar[i]:
                    psar[i] = low[i - 2]
            else:
                if low[i] < lp:
                    lp = low[i]
                    af = min(af + iaf, maxaf)
                if high[i - 1] > psar[i]:
                    psar[i] = high[i - 1]
                if high[i - 2] > psar[i]:
                    psar[i] = high[i - 2]
        if bull:
            psarbull[i] = psar[i]
        else:
            psarbear[i] = psar[i]
            
    barsdata['psar']=psar
    data=data.merge(barsdata[[time_label,'psar']],how="left",on=time_label)
    return data


def keltner_channels(data, time_label='Time',ohlc_labels=['Open','High','Low','Close'],window_size=20):
    """Keltner channels
    data -> pandas data frame
    time_label -> name of the column with timestamp - default='Time'
    ohlc -> labels names for open, high, low and close - default=['Open','High','Low','Close']
    window_size -> period of moving avg, default is 20 prior records
    
    >>> df[20:26]
                        Time     Open     High      Low    Close
    20      2010-01-01 05:00  1.43237  1.43294  1.43199  1.43268
    21      2010-01-01 05:15  1.43275  1.43294  1.43211  1.43277
    22      2010-01-01 05:30  1.43214  1.43298  1.43210  1.43268
    23      2010-01-01 05:45  1.43253  1.43306  1.43214  1.43303
    24      2010-01-01 06:00  1.43283  1.43335  1.43269  1.43334
    25      2010-01-01 06:15  1.43331  1.43332  1.43268  1.43326
    
    >>> df=keltner_channels(df, time_label='Time',ohlc_labels=['Open','High','Low','Close'])
    >>> df[20:26]    
                        Time     Open  ...  upper_Keltner_channel  lower_Keltner_channel
    20      2010-01-01 05:00  1.43237  ...               1.434548               1.430887
    21      2010-01-01 05:15  1.43275  ...               1.434522               1.430923
    22      2010-01-01 05:30  1.43214  ...               1.434511               1.430926
    23      2010-01-01 05:45  1.43253  ...               1.434549               1.430947
    24      2010-01-01 06:00  1.43283  ...               1.434518               1.431091
    25      2010-01-01 06:15  1.43331  ...               1.434490               1.431206
    """
    def atr(data_atr,roll):
        temp_atr=data_atr.copy()
        length = len(data_atr)
        multiplier=2/(roll+1)
        if length>roll or length!=roll:
            temp_atr['HL']=temp_atr['High']-temp_atr['Low']
            temp_atr['HCp']=abs(temp_atr['High']-temp_atr['Close'].shift(1))
            temp_atr['LCp']=abs(temp_atr['Low']-temp_atr['Close'].shift(1))
            temp_atr['TR']=temp_atr[['HL','HCp','LCp']].max(axis=1)
            lst=[]
            for i in range(0,length):
                if i < (roll-1):
                    lst.append(np.nan)
                elif i==(roll-1):
                    lst.append(np.mean(temp_atr['TR'][0:roll]))
                else:
                    #lst.append((lst[-1]*(roll-1)+temp_atr['TR'][i])/roll)
                    lst.append((temp_atr['TR'][i]-lst[-1])*multiplier+lst[-1])
        del(temp_atr)    
        return lst#temp_atr[['Time','ATR']]
    
    def ema(data_ema,roll):
        temp_ema=data_ema.copy()
        length=len(data_ema)
        if length>roll and length!=roll:
            multiplier=2/(roll+1)
            lst=[]
            for i in range(0,length):
                if i < (roll-1):
                    lst.append(np.nan)
                elif i == (roll-1):
                    lst.append(np.mean(temp_ema['Close'][0:roll]))
                elif i>(roll-1):
                    lst.append((multiplier*(temp_ema['Close'][i]-lst[-1]))+lst[-1])

        temp_ema['EMA']=lst
        return lst#temp_ema[['Time','EMA']]
    
    data_kelt= data[[time_label]+ohlc_labels].copy()
    #atr_df=atr(temp_kelt,int(roll/2))
    data_kelt['ATR']=atr(data_kelt,int(window_size/2))
    data_kelt['EMA']=ema(data_kelt,window_size)
    data_kelt['basic_Keltner_channel']=data_kelt['EMA']
    data_kelt['upper_Keltner_channel']=data_kelt['EMA'] + (2 * data_kelt['ATR'])
    data_kelt['lower_Keltner_channel']=data_kelt['EMA'] - (2 * data_kelt['ATR'])
    
    data=data.merge(data_kelt[[time_label,'basic_Keltner_channel','upper_Keltner_channel','lower_Keltner_channel']],how="left",on=time_label)
    return data

def rs_index(data, time_label="Time", close_label="Close", window_size=14):
    """RS Index
    data -> pandas data frame
    time_label & close_label -> columns names in data - default: time_label="Time", close_label="Close"
    window_size -> period of moving avg, default is 14 prior records
    
    >>> df[20:26]
                        Time     Open    Close
    20      2010-01-01 05:00  1.43237  1.43268
    21      2010-01-01 05:15  1.43275  1.43277
    22      2010-01-01 05:30  1.43214  1.43268
    23      2010-01-01 05:45  1.43253  1.43303
    24      2010-01-01 06:00  1.43283  1.43334
    25      2010-01-01 06:15  1.43331  1.43326
    
    >>> df=keltner_channels(df, time_label="Time", close_label="Close")
    >>> df[20:26]    
                        Time    Close        RSI
    20      2010-01-01 05:00  1.43268  48.553719
    21      2010-01-01 05:15  1.43277  53.043478
    22      2010-01-01 05:30  1.43268  46.043165
    23      2010-01-01 05:45  1.43303  53.791469
    24      2010-01-01 06:00  1.43334  55.580866
    25      2010-01-01 06:15  1.43326  55.454545
    """
    rsi_data=data[[time_label,close_label]].copy()
    rsi_data['Change']=rsi_data[close_label].diff(1)
    rsi_data['Gain']=rsi_data['Change'][(rsi_data['Change']>0)==True]
    rsi_data['Loss']=abs(rsi_data['Change'][(rsi_data['Change']>0)==False])
    rsi_data['Gain'].fillna(0,inplace=True)
    rsi_data['Loss'].fillna(0,inplace=True)
    rsi_data['Avg_Gain']=rsi_data['Gain'].rolling(window=window_size).mean()
    rsi_data['Avg_Loss']=rsi_data['Loss'].rolling(window=window_size).mean()
    rsi_data['RS']=rsi_data['Avg_Gain']/rsi_data['Avg_Loss']
    rsi_data['RSI']=100-(100/(1+(rsi_data['Avg_Gain']/rsi_data['Avg_Loss'])))
    
    data=data.merge(rsi_data[[time_label,'RSI']],how="left",on=time_label)
    return data
