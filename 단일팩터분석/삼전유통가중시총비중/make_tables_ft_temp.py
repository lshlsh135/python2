# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 17:21:58 2018

@author: 지형범
"""
import pandas as pd
def Make_Tables():
    wealth = net_wealth_1
    
    
    wealth = pd.merge(pd.DataFrame(wealth),kospi200_day,left_index=True, right_index=True, how='inner')
    datetime_wealth=pd.DataFrame(pd.to_datetime(wealth.index))
    wealth = pd.DataFrame(wealth, index = datetime_wealth[0])
    
    wealth_m = wealth.resample("BM").ffill()
    
    net_range_gross_rtn = wealth_m.pct_change()+1
    net_range_gross_rtn.iloc[0,:]=1
    net_range_gross_rtn['excess_daily_rtn'] = net_range_gross_rtn[net_range_gross_rtn.columns[0]] - net_range_gross_rtn['PRC']
#    net_range_gross_rtn['excess_daily_rtn_cum'] = net_range_gross_rtn['excess_daily_rtn'].cumsum().fillna(0)
    net_range_gross_rtn = net_range_gross_rtn.sort_index(ascending=False)
    month_list = [12,24,36,60]
    for i in month_list:
        locals()['m_{}'.format(i)] = net_range_gross_rtn.iloc[:i,2]
        locals()['m_{}'.format(i)].mean()
        locals()['m_{}'.format(i)].std()
    return net_range_gross_rtn.loc[:,'excess_daily_rtn_cum']