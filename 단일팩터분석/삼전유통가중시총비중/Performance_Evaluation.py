# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 15:24:40 2018

@author: 지형범
"""
import pandas as pd
import numpy as np
class Performance_Evaluation:
    def __init__(self, wealth, kospi_day, kospi200_day):
        self.wealth = pd.DataFrame(wealth)
        self.kospi_day = kospi_day
        self.kospi200_day = kospi200_day
        
    def Monthly_PF_EV(self):
        self.wealth = pd.merge(self.wealth,self.kospi200_day,left_index=True, right_index=True, how='inner')

        net_daily_gross_rtn = self.wealth.pct_change()+1
        net_daily_gross_rtn.iloc[0,:]=1
        net_daily_gross_rtn['excess_daily_rtn'] = net_daily_gross_rtn['rtn_d_cum'] - net_daily_gross_rtn['PRC']
        
        datetime=pd.DataFrame(pd.to_datetime(net_daily_gross_rtn.index)) # 월말 날짜를 뽑기위해 datetime으로 바꿔줌
        net_daily_gross_rtn = pd.DataFrame(net_daily_gross_rtn, index = datetime[0]) # n_w의 index를 python의 date type로 바꿔줌
        net_daily_gross_rtn = net_daily_gross_rtn['excess_daily_rtn']+1
        
        year_data=list(net_daily_gross_rtn.index.year.unique())
        month_data=list(net_daily_gross_rtn.index.month.unique())
        month_data.sort()
        
        for p in range(1,2):
            locals()['save_data_{}'.format(p)] = pd.DataFrame(data = np.zeros((len(year_data),len(month_data))), index = year_data, columns = month_data)
            save_data_temp = pd.DataFrame(net_daily_gross_rtn)
            
            for pp in year_data:
                for ppp in month_data:
                    if len(save_data_temp[(save_data_temp.index.year == pp)&(save_data_temp.index.month == ppp)]) != 0:
                        print(pp,ppp)
                        locals()['save_data_{}'.format(p)].loc[pp,ppp] = save_data_temp[(save_data_temp.index.year == pp)&(save_data_temp.index.month == ppp)].prod().values[0]
        return locals()['save_data_{}'.format(p)]
    
    def Monthly_Winning_ratio(self,save_data):
        self.save_data = save_data
        win_data = pd.Series()

        for i in range(len(self.save_data.columns)):
            win_data = pd.concat([win_data,self.save_data.iloc[:,i]])    
        
        win_data = pd.DataFrame(win_data)
        win_data = win_data[win_data.iloc[:,0]!=0]
        win_rate = round(len(win_data[win_data.iloc[:,0]>1])/len(win_data),2)
        
        return win_rate