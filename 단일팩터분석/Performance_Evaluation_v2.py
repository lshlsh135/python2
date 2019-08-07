# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:16:35 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 15:24:40 2018

@author: 지형범
"""
import pandas as pd
import numpy as np
import copy
class Performance_Evaluation:
    def __init__(self, wealth, kospi_day, kospi200_day):
        self.wealth = pd.DataFrame(wealth)
        self.kospi_day = kospi_day
        self.kospi200_day = kospi200_day
        
    def Monthly_PF_EV(self): # BM대비 초과수익률이군..... 문제는 BM이 없는 경우네 ㅋ
        wealth_ = pd.merge(self.wealth,self.kospi200_day,left_index=True, right_index=True, how='inner')
    
        net_daily_gross_rtn = wealth_.pct_change()+1
        net_daily_gross_rtn.iloc[0,:]=1
        for i in range(1,6):
            net_daily_gross_rtn['excess_daily_rtn_{}'.format(i)] = (net_daily_gross_rtn[net_daily_gross_rtn.columns[i-1]] - net_daily_gross_rtn['PRC'])+1
        
        datetime=pd.DataFrame(pd.to_datetime(net_daily_gross_rtn.index)) # 월말 날짜를 뽑기위해 datetime으로 바꿔줌
        net_daily_gross_rtn = pd.DataFrame(net_daily_gross_rtn, index = datetime[0]) # n_w의 index를 python의 date type로 바꿔줌
    
        
        year_data=list(net_daily_gross_rtn.index.year.unique())
        month_data=list(net_daily_gross_rtn.index.month.unique())
        month_data.sort()
        
        for p in range(1,6):
            locals()['save_data_{}'.format(p)] = pd.DataFrame(data = np.zeros((len(year_data),len(month_data))), index = year_data, columns = month_data)
            save_data_temp = pd.DataFrame(net_daily_gross_rtn['excess_daily_rtn_{}'.format(p)])
            
            for pp in year_data:
                for ppp in month_data:
                    if len(save_data_temp[(save_data_temp.index.year == pp)&(save_data_temp.index.month == ppp)]) != 0:
                        print(pp,ppp)
                        locals()['save_data_{}'.format(p)].loc[pp,ppp] = save_data_temp[(save_data_temp.index.year == pp)&(save_data_temp.index.month == ppp)].prod().values[0]
        
            locals()['save_data_{}'.format(p)] = locals()['save_data_{}'.format(p)].replace(0,np.nan)
            locals()['save_data_{}'.format(p)].loc[:,'excess_month_avg'] = locals()['save_data_{}'.format(p)].mean(axis=1)
            locals()['save_data_{}'.format(p)].loc['excess_year_avg',:] = locals()['save_data_{}'.format(p)].mean(axis=0)

        save_data = pd.DataFrame()
        for i in range(1,6):
            save_data = pd.concat([save_data,locals()['save_data_{}'.format(i)]],axis=0)
           
    
        return save_data, net_daily_gross_rtn
    
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
        wealth_ = self.wealth.reset_index()
        wealth_['dd'] = 0
        for i in range(len(wealth_)):
            wealth_.loc[i,'dd'] = wealth_.iloc[i,1] / max(wealth_.iloc[0:i+1,1])-1
        
        dd = wealth_.set_index(wealth_.columns[0])['dd']
           
        return(dd)
        
    def new_drawdown(self) :
        wealth_ = pd.DataFrame(self.wealth)
        ddd = pd.DataFrame(data = np.zeros(shape = (wealth_.shape[0], wealth_.shape[1])), index = wealth_.index, columns = [wealth_.columns])
        
        wealth_ = wealth_.pct_change()
        
        
#        wealth =  pd.DataFrame(wealth)
        wealth_[np.isnan(wealth_)] = 0
        
        for j in range(0, wealth_.shape[1]):
            
            if (wealth_.iloc[0, j] > 0) :
                ddd.iloc[0, j] = 0
            else :
                ddd.iloc[0, j] = wealth_.iloc[0, j]
                
            for i in range(1 , len(wealth_)):
                temp_ddd = (1+ddd.iloc[i-1, j]) * (1+wealth_.iloc[i, j]) -1
                if (temp_ddd > 0) :
                    ddd.iloc[i, j] = 0
                else:
                    ddd.iloc[i, j] = temp_ddd
        
        return(ddd)
            
    def daily_excess_rtn_cumsum(self):
         
        wealth_ = pd.merge(pd.DataFrame(self.wealth),self.kospi200_day,left_index=True, right_index=True, how='inner')
        
        net_daily_gross_rtn = wealth_.pct_change()+1
        net_daily_gross_rtn.iloc[0,:]=1
        net_daily_gross_rtn['excess_daily_rtn'] = net_daily_gross_rtn[net_daily_gross_rtn.columns[0]] - net_daily_gross_rtn['PRC']
        net_daily_gross_rtn['excess_daily_rtn_cum'] = net_daily_gross_rtn['excess_daily_rtn'].cumsum().fillna(0)
        
        return net_daily_gross_rtn.loc[:,'excess_daily_rtn_cum']
        
    def Sharpe_Ratio(self):
        daily_gross_rtn = self.wealth.pct_change()+1
        sharpe_ratio = np.sqrt(252)*(daily_gross_rtn[1:]-1).mean() / (daily_gross_rtn[1:]-1).std()
        
        return sharpe_ratio
        
        
    def Make_Tables(self,month_list):
        wealth_ = pd.merge(pd.DataFrame(self.wealth),self.kospi200_day,left_index=True, right_index=True, how='inner')
        datetime_wealth=pd.DataFrame(pd.to_datetime(wealth_.index))
        wealth_ = pd.DataFrame(wealth_, index = datetime_wealth[0])
        self.month_list = month_list
        wealth_m = wealth_.resample("BM").ffill()
        
        net_range_gross_rtn = wealth_m.pct_change()+1
        net_range_gross_rtn.iloc[0,:]=1
        
        for i in range(1,6):
                net_range_gross_rtn['excess_daily_rtn_{}'.format(i)] = net_range_gross_rtn[net_range_gross_rtn.columns[i-1]] - net_range_gross_rtn['PRC']
                net_range_gross_rtn['excess_daily_rtn_cum_{}'.format(i)] = net_range_gross_rtn['excess_daily_rtn_{}'.format(i)].cumsum().fillna(0)
        
        
        net_range_gross_rtn = net_range_gross_rtn.sort_index(ascending=False)
    #        month_list = [12,24,36,60]
        ticker = ['Monthly_AV','Monthly_STD','IR','MDD','Win_Rate']
    
        for p in range(1,6):
            locals()['save_data_{}'.format(p)] = pd.DataFrame(data = np.zeros((len(ticker),len(month_list))), index = ticker, columns = month_list)
    
            for i in month_list:
    #            locals()['m_{}'.format(i)] = net_range_gross_rtn.loc[:,['excess_daily_rtn_cum'+'_'+str(p)]]
                locals()['m_{}'.format(i)] = net_range_gross_rtn.loc[:,p]
                locals()['m_{}'.format(i)] = locals()['m_{}'.format(i)][:i]
                locals()['save_data_{}'.format(p)].loc['Monthly_AV',i] = locals()['m_{}'.format(i)].mean()-1
                locals()['save_data_{}'.format(p)].loc['Monthly_STD',i] = locals()['m_{}'.format(i)].std()
                locals()['save_data_{}'.format(p)].loc['IR',i] = locals()['save_data_{}'.format(p)].loc['Monthly_AV',i]/locals()['save_data_{}'.format(p)].loc['Monthly_STD',i] * np.sqrt(12)
        #        locals()['save_data_{}'.format(p)].loc['monthly_av',i] = locals()['m_{}'.format(i)].mean()
                locals()['save_data_{}'.format(p)].loc['Win_Rate',i] = locals()['m_{}'.format(i)][locals()['m_{}'.format(i)]>1].count() / len(locals()['m_{}'.format(i)])
                
                
                locals()['m_{}'.format(i)] = pd.DataFrame(locals()['m_{}'.format(i)].sort_index())
                locals()['m_{}'.format(i)]['excess_daily_rtn_cum'] = locals()['m_{}'.format(i)].cumsum().fillna(0)
                
                wealth_temp = wealth_[wealth_.index>=locals()['m_{}'.format(i)].index[0]]
                wealth_temp = wealth_temp.pct_change().fillna(0)
                wealth_temp['excess_daily_rtn'] = wealth_temp[wealth_temp.columns[p-1]] - wealth_temp['PRC']
                wealth_temp['excess_daily_rtn_cum'] = wealth_temp['excess_daily_rtn'].cumsum().fillna(0)
                wealth_temp = wealth_temp['excess_daily_rtn_cum']
                wealth_temp = wealth_temp.reset_index()
                wealth_temp['dd'] = 0
                for z in range(len(wealth_temp)):#기존의 mdd는 지수화했기 때문에 나눠주는데 이건 절대 초과수익률이기 때문에 차이를 본다.
                    wealth_temp.loc[z,'dd'] = wealth_temp.iloc[z,1] - max(wealth_temp.iloc[0:z+1,1])
                
                dd = wealth_temp.set_index(wealth_temp.columns[0])['dd'].fillna(0)
                locals()['save_data_{}'.format(p)].loc['MDD',i] = min(dd)
                
        save_data = pd.DataFrame()
        for i in range(1,6):
            save_data = pd.concat([save_data,locals()['save_data_{}'.format(i)]],axis=0)
        
        return save_data
            
            
            
            