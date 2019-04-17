# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 17:24:36 2019

@author: 지형범
"""

def Monthly_PF_EV(self):
    wealth_ = pd.merge(wealth,kospi200_day,left_index=True, right_index=True, how='inner')

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
    
    
    save_data = pd.DataFrame()
    for i in range(1,6):
        save_data = pd.concat([save_data,locals()['save_data_{}'.format(i)]],axis=0)

    
    
    return save_data

