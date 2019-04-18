# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 17:46:49 2019

@author: 지형범
"""

month_list = [12,24,36,60,211]
def Make_Tables(self,month_list):
    wealth_ = pd.merge(pd.DataFrame(d),kospi200_day,left_index=True, right_index=True, how='inner')
    datetime_wealth=pd.DataFrame(pd.to_datetime(wealth_.index))
    wealth_ = pd.DataFrame(wealth_, index = datetime_wealth[0])
    month_list = month_list
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
    return save_data
        
            
            