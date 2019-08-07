# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 17:57:06 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 13:28:05 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jul  5 14:43:00 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 16:23:52 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jun 26 14:56:51 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Mon May 20 16:48:39 2019

@author: 지형범
"""

import pandas as pd
import cx_Oracle
import numpy as np
import copy
#이거 두개 반드시 선언!
cx0=cx_Oracle.makedsn("localhost",1521,"xe")
connection = cx_Oracle.connect("lshlsh135","2tkdgns2",cx0) #이게 실행이 안될때가 있는데

float_cap_data = pd.concat([pd.read_sql("""select trd_date,gicode,float_cap from kospi_20181130 """,con=connection),pd.read_sql("""select trd_date,gicode,float_cap from kosdaq_20181130 """,con=connection)],axis=0,ignore_index=True).drop_duplicates()
float_cap_data = float_cap_data.rename(columns={'TRD_DATE':'trd_date','GICODE':'gicode','FLOAT_CAP':'float_cap'})



daily_return = pd.concat([pd.read_sql("""select * from kospi_daily_stock_20181130 """,con=connection),pd.read_sql("""select * from kosdaq_daily_stock_20181130 """,con=connection)],axis=0,ignore_index=True).drop_duplicates()
daily_return = daily_return[daily_return['ADJ_PRC_D'].notnull()] # 메모리 사용량을 줄이기 위해서 실행
daily_return = daily_return.rename(columns={'GICODE':'gicode','TRD_DATE':'trd_date'})

kospi_day = pd.read_sql("""select * from kospi_day_prc_20181130""",con=connection) # 코스피 일별 종가
kospi_day.set_index('TRD_DATE',inplace=True)
kospi_day = kospi_day.rename(columns={'PRC':'prc','TRD_DATE':'trd_date'})
day_date = kospi_day.reset_index()
day_date = day_date.rename(columns={'TRD_DATE':'trd_date'})
daily_date=pd.DataFrame(daily_return.groupby('trd_date').count().reset_index()['trd_date'])

raw_data = pd.read_csv('dg_csv_kospikosdaq_sales_20190627.csv',engine='python')
raw_data = raw_data[raw_data['iskospi200']==1]
raw_data = pd.merge(raw_data,float_cap_data,how='outer',on=['trd_date','gicode'])
raw_data['adj_bps_shift'] = raw_data.groupby(['gicode'])['adj_bps'].shift(1)
raw_data['sales_shift'] = raw_data.groupby(['gicode'])['sales'].shift(1)
raw_data['adj_prc_shift'] = raw_data.groupby(['gicode'])['adj_prc'].shift(-1)
raw_data['float_cap_shift'] = raw_data.groupby(['gicode'])['float_cap'].shift(-1)
#raw_data['bps_shift'] = raw_data.groupby(['gicode'])['bps'].shift(1)

#rebalancing_date = pd.DataFrame(raw_data['trd_date'].drop_duplicates())
rebalancing_date = pd.read_excel('rebalancing_date_quaterly.xlsx')
rebalancing_date = rebalancing_date.astype(str)

raw_data = pd.merge(rebalancing_date,raw_data,how='left',on='trd_date') # 리밸런싱 날짜에 맞는데이터가 rolling standared deviation 구할 때 필요하기 때문에 merge 활용 

raw_data['sales_pct_chg'] = raw_data.groupby('gicode')['sales'].apply(lambda x: x.pct_change())
raw_data['sales_rolling_std'] = raw_data.groupby('gicode')['sales_pct_chg'].apply(lambda x: x.rolling(8).std())
#a = raw_data.head(10000)
#a = raw_data[(raw_data['co_nm']=='삼성전자')|(raw_data['co_nm']=='SK하이닉스')]




#raw_data['sales_surprise'] = abs((raw_data['sales'] - raw_data['sales_consen'])/raw_data['sales_consen'])
factor = ['1/pbr']

for i in factor: # 5분위를 저장해야 하기 때문에 모든 변수를 5개씩 선언해준다.
    locals()['end_wealth_{}'.format(i)] = 1 # 포트폴리오의 시작 wealth = 1
    locals()['turno_{}'.format(i)] = 0 # 가장 처음 리밸런싱을 잡기 위한 변수
    locals()['wealth_{}'.format(i)] = list() # 매 리밸런싱때의 wealth 변화를 list로 저장
    locals()['wealth_num_{}'.format(i)] = 0 # 리밸런싱 할때마다 wealth의 리스트 row가 증가하기 때문에 같이 늘려주는 변수
    locals()['turnover_day_{}'.format(i)] = pd.DataFrame(np.zeros(shape = (daily_date.shape[0], daily_date.shape[1])),index = daily_date['trd_date'])
result = dict()

start_n = 14
col_length = len(rebalancing_date)-1
z = pd.DataFrame()
#stock_num = 20
for n in range(start_n,col_length-2): 
#   
        first_data = raw_data[raw_data['trd_date']==rebalancing_date.iloc[n,0]] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
        first_data = first_data[first_data['iskospi200']==1]
        
        second_data = raw_data[raw_data['trd_date']==rebalancing_date.iloc[n+1,0]]
        second_data = second_data[second_data['iskospi200']==1]
        second_data = pd.DataFrame(second_data.loc[:,['trd_date','gicode']])
        first_data = pd.merge(first_data,second_data,how='outer',on='gicode')
        first_data = first_data[(first_data['trd_date_x'].notnull())&(first_data['trd_date_y'].notnull())]
#        first_data['div_yield']=first_data['adj_dps_shift']/first_data['adj_prc']
        first_data['1/pbr'] = first_data['adj_bps_shift']/first_data['adj_prc']
        first_data['float_weights'] = first_data['float_cap']/first_data['float_cap'].sum()
#        first_data = first_data.loc[:,~first_data.columns.duplicated()] # 중복 columns를 제거한다
        
#        third_data = raw_data[(raw_data['trd_date']==rebalancing_date.iloc[n-1,0])|(raw_data['trd_date']==rebalancing_date.iloc[n-2,0])|(raw_data['trd_date']==rebalancing_date.iloc[n-3,0])&(raw_data['iskospi200']==1)]
#        third_data = pd.DataFrame(third_data.loc[:,['trd_date','gicode','sales_surprise']])
#        third_data2 = third_data.groupby('gicode').count().reset_index()
#        third_data2 = third_data2[third_data2['sales_surprise']==3]
#        third_data3 = pd.merge(third_data,third_data2,on='gicode')
#        third_data3 = third_data3.groupby('gicode').sum().reset_index()
#        first_data = pd.merge(first_data,third_data3,how='outer',on='gicode')
       
        
        
        
        
        
#        first_data = first_data[(first_data['1/pbr'].notnull())&(first_data['sales_rolling_std'].notnull())]








#        first_data['sales_surprise'] = first_data['sales_surprise_x'] + first_data['sales_surprise']
#        first_data = first_data.rename(columns={'trd_date_y_x':'trd_date_y'})
        
        
        try:
            reb_next_day = day_date.loc[day_date.loc[day_date['trd_date']==rebalancing_date.iloc[n+1,0]].index[0]+1,'trd_date'] # 리밸런싱 다음날까지의 가격데이터가 필요하다!!
            rtn_d_need=daily_return[(daily_return['trd_date']<=reb_next_day)&(daily_return['trd_date']>rebalancing_date.iloc[n,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
        except:
            rtn_d_need=daily_return[(daily_return['trd_date']<=rebalancing_date.iloc[n+1,0])&(daily_return['trd_date']>rebalancing_date.iloc[n,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
        
        factor2 = copy.deepcopy(factor)
        for i in factor2:
            locals()['data_{}'.format(i)] = first_data[first_data.loc[:,i].notnull()]
            locals()['data_{}_rnk'.format(i)] = locals()['data_{}'.format(i)].loc[:,i].rank(method='first',ascending = True) # 클수록 높은등수 -> z값이 큰 양수
            b = pd.DataFrame(data=(locals()['data_{}_rnk'.format(i)] - locals()['data_{}_rnk'.format(i)].mean()) / locals()['data_{}_rnk'.format(i)].std(ddof=1)) # 자유도 1
            locals()['q_{}'.format(i)] = pd.merge(first_data,b,left_index=True,right_index=True)
            locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)][locals()['q_{}'.format(i)][i+'_y'].notnull()]
            locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)][(locals()['q_{}'.format(i)][i+'_y']>=locals()['q_{}'.format(i)][i+'_y'].quantile(19/20))]
#            locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)].assign(rnk = locals()['q_{}'.format(i)].loc[:,'sales_rolling_std'].rank(method='first',ascending = True))
#            locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)][locals()['q_{}'.format(i)]['sales_rolling_std']<=locals()['q_{}'.format(i)]['sales_rolling_std'].quantile(1/2)]
#            locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)][(locals()['q_{}'.format(i)]['rnk']<=20)]
#            locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)][(locals()['q_{}'.format(i)][i+'_y']<=locals()['q_{}'.format(i)][i+'_y'].quantile(1/3))]
#            locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)][locals()['q_{}'.format(i)]['sales_surprise']<=locals()['q_{}'.format(i)]['sales_surprise'].quantile(1/3)]


#            locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)].assign(rnk = locals()['q_{}'.format(i)].loc[:,i+'_y'].rank(method='first',ascending = False))
            
#            locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)][(locals()['q_{}'.format(i)]['rnk']>1)&(locals()['q_{}'.format(i)]['rnk']<=stock_num+2)]
        
            if len(locals()['q_{}'.format(i)][locals()['q_{}'.format(i)]['gicode']=='A005930'])==0:
#                locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)][locals()['q_{}'.format(i)]['rnk']<=stock_num]
#                locals()['q_{}'.format(i)] = locals()['q_{}'.format(i)][locals()['q_{}'.format(i)]['rnk']<=stock_num+1]
                q_temp = first_data[first_data['gicode']=='A005930']
#                q_temp.loc[q_temp.loc[:,'FLOAT_WEIGHTS'].isnull(),'FLOAT_WEIGHTS'] = 0.22
                samsung_float_weights = q_temp.loc[:,'float_weights'].values[0]
                locals()['q_{}'.format(i)]['float_weights'] = (1 - samsung_float_weights)/len(locals()['q_{}'.format(i)])
                locals()['q_{}'.format(i)] = pd.concat([locals()['q_{}'.format(i)],q_temp])
            
            else:
            
                samsung_float_weights = locals()['q_{}'.format(i)][locals()['q_{}'.format(i)]['gicode']=='A005930']['float_weights'].values[0]
                locals()['q_{}'.format(i)].loc[locals()['q_{}'.format(i)]['gicode']!='A005930','float_weights'] = (1 - samsung_float_weights)/len(locals()['q_{}'.format(i)])
            print(len(locals()['q_{}'.format(i)]))
            z = pd.concat([z,locals()['q_{}'.format(i)][['trd_date_x','co_nm','1/pbr_x']]])
     
        
            rst_rtn_d=pd.merge(locals()['q_{}'.format(i)],rtn_d_need,how='inner',on='gicode') # 선택된 주식과 일별데이타 merge
            rst_rtn_d['rtn_d'] = rst_rtn_d.groupby('gicode')['ADJ_PRC_D'].apply(lambda x: x.pct_change()+1) # gross return으로 바꿔줌
#            rst_rtn_d.loc[(rst_rtn_d['trd_date']==rst_rtn_d.loc[0,'trd_date'])&(rst_rtn_d['gicode']=='A005930'),'rtn_d'] = locals()['end_wealth_{}'.format(i)]  * samsung_float_weights
            rst_rtn_d.loc[(rst_rtn_d['trd_date']==rst_rtn_d.loc[0,'trd_date']),'rtn_d'] = locals()['end_wealth_{}'.format(i)]   /len(locals()['q_{}'.format(i)])
            
            rst_rtn_d['rtn_d_cum']=rst_rtn_d.groupby('gicode')['rtn_d'].cumprod() # 각 주식별 누적수익률
            
           
            locals()['wealth_{}'.format(i)].append(rst_rtn_d.groupby('trd_date').sum()['rtn_d_cum']) # list로 쭈욱 받고
            
            locals()['end_wealth_{}'.format(i)] = locals()['wealth_{}'.format(i)][locals()['wealth_num_{}'.format(i)]][-1]
            locals()['wealth_num_{}'.format(i)]+=1
            
            
            
            if locals()['turno_{}'.format(i)] == 0:
                locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n,0]] = 1
                locals()['turno_{}'.format(i)]+= 1
            else:
                turnover_data_sum=pd.merge(rst_rtn_d[rst_rtn_d['trd_date']==rst_rtn_d.loc[0,'trd_date']],locals()['rst_rtn_d_past_{}'.format(i)][locals()['rst_rtn_d_past_{}'.format(i)]['trd_date']==rst_rtn_d.loc[0,'trd_date']],how='outer',on='gicode')
                turnover_data_sum = turnover_data_sum.replace(np.nan,0)  
                locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n,0]] = np.sum(abs(turnover_data_sum['rtn_d_cum_x']/np.sum(turnover_data_sum['rtn_d_cum_x'])
                -turnover_data_sum['rtn_d_cum_y']/np.sum(turnover_data_sum['rtn_d_cum_y'])))
            
            
            
            locals()['rst_rtn_d_past_{}'.format(i)] = rst_rtn_d    
            
for i in factor:
    
    locals()['wealth_{}'.format(i)] = pd.concat(locals()['wealth_{}'.format(i)]) # 맨 마지막에 리스트를 풀어서 시리즈로 만들어줌
    locals()['wealth_{}'.format(i)]=locals()['wealth_{}'.format(i)][~locals()['wealth_{}'.format(i)].index.duplicated(keep='first')] #index 중복제거 
    locals()['daily_gross_rtn_{}'.format(i)]=pd.DataFrame(locals()['wealth_{}'.format(i)].pct_change()+1) # wealth의 누적에서 일별 gross 수익률을 구함.
    locals()['daily_gross_rtn_{}'.format(i)][np.isnan(locals()['daily_gross_rtn_{}'.format(i)])] = 0             # 첫번째 수익률이 nan이기 떄문에 바꿔준다.
    locals()['turnover_day_{}'.format(i)] = locals()['turnover_day_{}'.format(i)].shift(1) * 0.005 # turnover 구한거를 리밸런싱 다음날에 반영해준다.
    locals()['sub_{}'.format(i)] = pd.merge(locals()['daily_gross_rtn_{}'.format(i)],
                                        locals()['turnover_day_{}'.format(i)],left_index=True,right_index=True)
    locals()['net_daily_gross_rtn_{}'.format(i)]=locals()['sub_{}'.format(i)].iloc[:,0]-locals()['sub_{}'.format(i)].iloc[:,1]
    locals()['net_daily_gross_rtn_{}'.format(i)][0] = 1 # 누적 wealth를 구하기 위해 첫날 수익률을 1이라고 가정.
    locals()['net_wealth_{}'.format(i)]=locals()['net_daily_gross_rtn_{}'.format(i)].cumprod()

