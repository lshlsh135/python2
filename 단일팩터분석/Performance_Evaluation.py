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
        net_daily_gross_rtn['excess_daily_rtn'] = net_daily_gross_rtn[net_daily_gross_rtn.columns[0]] - net_daily_gross_rtn['PRC']
        
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
    
    def traditional_mdd(self):
        self.wealth = self.wealth.reset_index()
        self.wealth['dd'] = 0
        for i in range(len(self.wealth)):
            self.wealth.loc[i,'dd'] = self.wealth.iloc[i,1] / max(self.wealth.iloc[0:i+1,1])-1
        
        dd = self.wealth.set_index(self.wealth.columns[0])['dd']
           
        return(dd)
        
    def new_drawdown(self) :
        self.wealth = pd.DataFrame(self.wealth)
        ddd = pd.DataFrame(data = np.zeros(shape = (self.wealth.shape[0], self.wealth.shape[1])), index = self.wealth.index, columns = [self.wealth.columns])
        
        self.wealth = self.wealth.pct_change()
        
        
#        wealth =  pd.DataFrame(wealth)
        self.wealth[np.isnan(self.wealth)] = 0
        
        for j in range(0, self.wealth.shape[1]):
            
            if (self.wealth.iloc[0, j] > 0) :
                ddd.iloc[0, j] = 0
            else :
                ddd.iloc[0, j] = self.wealth.iloc[0, j]
                
            for i in range(1 , len(self.wealth)):
                temp_ddd = (1+ddd.iloc[i-1, j]) * (1+self.wealth.iloc[i, j]) -1
                if (temp_ddd > 0) :
                    ddd.iloc[i, j] = 0
                else:
                    ddd.iloc[i, j] = temp_ddd
        
        return(ddd)
            
    def daily_excess_rtn_cumsum(self):
         
        self.wealth = pd.merge(pd.DataFrame(self.wealth),self.kospi200_day,left_index=True, right_index=True, how='inner')
        
        net_daily_gross_rtn = self.wealth.pct_change()+1
        net_daily_gross_rtn.iloc[0,:]=1
        net_daily_gross_rtn['excess_daily_rtn'] = net_daily_gross_rtn[net_daily_gross_rtn.columns[0]] - net_daily_gross_rtn['PRC']
        net_daily_gross_rtn['excess_daily_rtn_cum'] = net_daily_gross_rtn['excess_daily_rtn'].cumsum().fillna(0)
        
        return net_daily_gross_rtn.loc[:,'excess_daily_rtn_cum']
        
    def Sharpe_Ratio(self):
        daily_gross_rtn = self.wealth.pct_change()+1
        sharpe_ratio = np.sqrt(252)*(daily_gross_rtn[1:]-1).mean() / (daily_gross_rtn[1:]-1).std()
        
        return sharpe_ratio
        
        
    def Make_Tables(self,month_list):
        self.wealth = pd.merge(pd.DataFrame(self.wealth),self.kospi200_day,left_index=True, right_index=True, how='inner')
        datetime_wealth=pd.DataFrame(pd.to_datetime(self.wealth.index))
        self.wealth = pd.DataFrame(self.wealth, index = datetime_wealth[0])
        self.month_list = month_list
        wealth_m = self.wealth.resample("BM").ffill()
        
        net_range_gross_rtn = wealth_m.pct_change()+1
        net_range_gross_rtn.iloc[0,:]=1
        net_range_gross_rtn['excess_daily_rtn'] = net_range_gross_rtn[net_range_gross_rtn.columns[0]] - net_range_gross_rtn['PRC']
        net_range_gross_rtn['excess_daily_rtn_cum'] = net_range_gross_rtn['excess_daily_rtn'].cumsum().fillna(0)
        net_range_gross_rtn = net_range_gross_rtn.sort_index(ascending=False)
#        month_list = [12,24,36,60]
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
            
            wealth_temp = self.wealth[self.wealth.index>=locals()['m_{}'.format(i)].index[0]]
            wealth_temp = wealth_temp.pct_change().fillna(0)
            wealth_temp['excess_daily_rtn'] = wealth_temp[wealth_temp.columns[0]] - wealth_temp['PRC']
            wealth_temp['excess_daily_rtn_cum'] = wealth_temp['excess_daily_rtn'].cumsum().fillna(0)
            wealth_temp = wealth_temp['excess_daily_rtn_cum']
            wealth_temp = wealth_temp.reset_index()
            wealth_temp['dd'] = 0
            for p in range(len(wealth_temp)):#기존의 mdd는 지수화했기 때문에 나눠주는데 이건 절대 초과수익률이기 때문에 차이를 본다.
                wealth_temp.loc[p,'dd'] = wealth_temp.iloc[p,1] - max(wealth_temp.iloc[0:p+1,1])
            
            dd = wealth_temp.set_index(wealth_temp.columns[0])['dd'].fillna(0)
            save_data.loc['MDD',i] = min(dd)
        return save_data
            
            
            
            