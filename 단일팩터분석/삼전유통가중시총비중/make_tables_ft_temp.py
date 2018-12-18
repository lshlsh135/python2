# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 17:21:58 2018

@author: 지형범
"""
import pandas as pd
import numpy as np
def Make_Tables():
    wealth = net_wealth_1
    
    
    wealth = pd.merge(pd.DataFrame(wealth),kospi200_day,left_index=True, right_index=True, how='inner')
    datetime_wealth=pd.DataFrame(pd.to_datetime(wealth.index))
    wealth = pd.DataFrame(wealth, index = datetime_wealth[0])
    
    wealth_m = wealth.resample("BM").ffill()
    
    net_range_gross_rtn = wealth_m.pct_change()+1
    net_range_gross_rtn.iloc[0,:]=1
    net_range_gross_rtn['excess_daily_rtn'] = net_range_gross_rtn[net_range_gross_rtn.columns[0]] - net_range_gross_rtn['PRC']
    net_range_gross_rtn['excess_daily_rtn_cum'] = net_range_gross_rtn['excess_daily_rtn'].cumsum().fillna(0)
    net_range_gross_rtn = net_range_gross_rtn.sort_index(ascending=False)
    month_list = [12,24,36,60,len(net_range_gross_rtn)-1]
    ticker = ['Monthly_AV','Monthly_STD','IR','MDD','Win_Rate']
    save_data = pd.DataFrame(data = np.zeros((len(ticker),len(month_list))), index = ticker, columns = month_list)
    for i in month_list:
        locals()['m_{}'.format(i)] = net_range_gross_rtn.iloc[:i,2]
        save_data.loc['Monthly_AV',i] = locals()['m_{}'.format(i)].mean()
        save_data.loc['Monthly_STD',i] = locals()['m_{}'.format(i)].std()
        save_data.loc['IR',i] = save_data.loc['Monthly_AV',i]/save_data.loc['Monthly_STD',i] * np.sqrt(12)
#        save_data.loc['monthly_av',i] = locals()['m_{}'.format(i)].mean()
        save_data.loc['Win_Rate',i] = locals()['m_{}'.format(i)][locals()['m_{}'.format(i)]>0].count() / len(locals()['m_{}'.format(i)])
        
        locals()['m_{}'.format(i)] = pd.DataFrame(locals()['m_{}'.format(i)].sort_index())
        locals()['m_{}'.format(i)]['excess_daily_rtn_cum'] = locals()['m_{}'.format(i)].cumsum().fillna(0)
        
        wealth_temp = wealth[wealth.index>=locals()['m_{}'.format(i)].index[0]]
        wealth_temp = wealth_temp.pct_change().fillna(0)
        wealth_temp['excess_daily_rtn'] = wealth_temp[wealth_temp.columns[0]] - wealth_temp['PRC']
        wealth_temp['excess_daily_rtn_cum'] = wealth_temp['excess_daily_rtn'].cumsum().fillna(0)
        wealth_temp = wealth_temp['excess_daily_rtn_cum']
        wealth_temp = wealth_temp.reset_index()
        wealth_temp['dd'] = 0
        for p in range(len(wealth_temp)):
            wealth_temp.loc[p,'dd'] = wealth_temp.iloc[p,1] - max(wealth_temp.iloc[0:p+1,1])
        #기존의 mdd는 지수화했기 때문에 나눠주는데 이건 절대 초과수익률이기 때문에 차이를 본다.
        dd = wealth_temp.set_index(wealth_temp.columns[0])['dd'].fillna(0)
        save_data.loc['MDD',i] = min(dd)
    return save_data