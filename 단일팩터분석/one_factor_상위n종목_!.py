
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 10:58:42 2018

결과에는 큰 차이가 업지만 groupby와 pct_change를 쓸때 워킹이 안되던 문제를 apply를 활용해서 해결했다.
rst_rtn_d['RTN_D'] = rst_rtn_d.groupby('GICODE')['ADJ_PRC_D'].pct_change() + 1
에서
rst_rtn_d['RTN_D'] = rst_rtn_d.groupby('GICODE')['ADJ_PRC_D'].apply(lambda x: x.pct_change()+1)

시가총액은 기존에 보통주 + 우선주를 사용햇는데 우선주 제외 시가총액으로 변경.

주식 데이터가 한달 후 없어진다면미리 알 수 있으므로 한달뒤 상폐 종목은 제외

@author: SH-NoteBook
"""



"""
Created on Tue Oct 17 13:27:31 2017


29개의 wics 섹터 중에서 1년 - 1개월 누적수익률 상위 15섹터를 고른 후에, 
그 섹터에 포함되는 종목중에서 factor들의 outlier들을 winsorize 해주고
섹터별로 z_score을 구한다음 +-3 보정해준다.

주식가격에서 직접 수익률을 구해서 쓰는것과 일별수익률을 받아서 쓰는것에는 오차가 있다...


@author: SH-NoteBook
"""
import numpy as np
import pandas as pd
from drawdown import drawdown
import copy

class QVGSM_VALUE:
 
    def __init__(self,stock_num,raw_data,rebalancing_date,kospi_day,daily_return,gross_col_loc,profit_col_loc,value_col_loc,cpi_data,oscore,momentum):
       stock_num = 20
       day_date = kospi_day.reset_index()
       factor = 'div_yield'
       col_length = len(rebalancing_date)-1 #rebalancing_date의 길이는 66이다. range로 이렇게 하면 0부터 65까지 66개의 i 가 만들어진다. -1을 해준건 실제 수익률은 -1개가 생성되기 때문.
       daily_date=pd.DataFrame(daily_return.groupby('TRD_DATE').count().reset_index()['TRD_DATE'])
       for i in range(1,2): # 5분위를 저장해야 하기 때문에 모든 변수를 5개씩 선언해준다.
            locals()['end_wealth_{}'.format(i)] = 1 # 포트폴리오의 시작 wealth = 1
            locals()['turno_{}'.format(i)] = 0 # 가장 처음 리밸런싱을 잡기 위한 변수
            locals()['wealth_{}'.format(i)] = list() # 매 리밸런싱때의 wealth 변화를 list로 저장
            locals()['wealth_num_{}'.format(i)] = 0 # 리밸런싱 할때마다 wealth의 리스트 row가 증가하기 때문에 같이 늘려주는 변수
            locals()['turnover_day_{}'.format(i)] = pd.DataFrame(np.zeros(shape = (daily_date.shape[0], daily_date.shape[1])),index = daily_date['TRD_DATE'])

    def QVGSM(self):
        
        for n in range(20,col_length): 
            if rebalancing_date.iloc[n,0][5:7] =='02':
                n-=1
    
                first_data = raw_data[raw_data['TRD_DATE']==rebalancing_date.iloc[n,0]] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                first_data = first_data[first_data['ISKOSPI200']==1]
                second_data = raw_data[raw_data['TRD_DATE']==rebalancing_date.iloc[n+1,0]]
                second_data = second_data[second_data['ISKOSPI200']==1]
                second_data = pd.DataFrame(second_data.loc[:,['TRD_DATE_OLD','GICODE']])
                first_data = pd.merge(first_data,second_data,how='outer',on='GICODE')
                first_data = first_data[(first_data['TRD_DATE_OLD_x'].notnull())&(first_data['TRD_DATE_OLD_y'].notnull())]
                first_data['MARKET_CAP_COM'] = first_data['MARKET_CAP_COM_2LEAD']
                first_data['ADJ_NI_12M_FWD'] = first_data['ADJ_NI_12M_FWD_2LEAD']
                first_data['NI_12M_FWD'] = first_data['NI_12M_FWD_2LEAD']
#                first_data = first_data[first_data['MARKET_CAP_COM']>100000000000]
                
                first_data['div_yield']=first_data['CASH_DIV_COM_Y']/first_data['MARKET_CAP_COM']
                first_data['1/per']= first_data['ADJ_NI_12M_FWD']/first_data['MARKET_CAP_COM']
                first_data['1/pbr'] = first_data['EQUITY']/first_data['MARKET_CAP_COM']
                
                first_data = first_data[first_data[factor].notnull()]
                first_data = first_data.assign(rnk = first_data.loc[:,factor].rank(method='first',ascending = False))
                q_1 = first_data[first_data['rnk']<=stock_num]
                try:
                    reb_next_day = day_date.loc[day_date.loc[day_date['TRD_DATE']==rebalancing_date.iloc[n+2,0]].index[0]+1,'TRD_DATE'] # 리밸런싱 다음날까지의 가격데이터가 필요하다!!
                    rtn_d_need=daily_return[(daily_return['TRD_DATE']<=reb_next_day)&(daily_return['TRD_DATE']>rebalancing_date.iloc[n+1,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
                except:
                    rtn_d_need=daily_return[(daily_return['TRD_DATE']<=rebalancing_date.iloc[n+2,0])&(daily_return['TRD_DATE']>rebalancing_date.iloc[n+1,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
                
                for i in range(1,2):
                    rst_rtn_d=pd.merge(locals()['q_{}'.format(i)],rtn_d_need,how='inner',on='GICODE') # 선택된 주식과 일별데이타 merge
                    rst_rtn_d['rtn_d'] = rst_rtn_d.groupby('GICODE')['ADJ_PRC_D'].apply(lambda x: x.pct_change()+1) # gross return으로 바꿔줌
                    rst_rtn_d.loc[(rst_rtn_d['TRD_DATE_y']==rst_rtn_d.loc[0,'TRD_DATE_y']),'rtn_d'] = locals()['end_wealth_{}'.format(i)]  / len(locals()['q_{}'.format(i)])
                    rst_rtn_d['rtn_d_cum']=rst_rtn_d.groupby('GICODE')['rtn_d'].cumprod() # 각 주식별 누적수익률
                    
                   
                    locals()['wealth_{}'.format(i)].append(rst_rtn_d.groupby('TRD_DATE_y').sum()['rtn_d_cum']) # list로 쭈욱 받고
                    
                    locals()['end_wealth_{}'.format(i)] = locals()['wealth_{}'.format(i)][locals()['wealth_num_{}'.format(i)]][-1]
                    locals()['wealth_num_{}'.format(i)]+=1
                    
                    
                    
                    if locals()['turno_{}'.format(i)] == 0:
                        locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n+1,0]] = 1
                        locals()['turno_{}'.format(i)]+= 1
                    else:
                        turnover_data_sum=pd.merge(rst_rtn_d[rst_rtn_d['TRD_DATE_y']==rst_rtn_d.loc[0,'TRD_DATE_y']],locals()['rst_rtn_d_past_{}'.format(i)][locals()['rst_rtn_d_past_{}'.format(i)]['TRD_DATE_y']==rst_rtn_d.loc[0,'TRD_DATE_y']],how='outer',on='GICODE')
                        turnover_data_sum = turnover_data_sum.replace(np.nan,0)  
                        locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n+1,0]] = np.sum(abs(turnover_data_sum['rtn_d_cum_x']/np.sum(turnover_data_sum['rtn_d_cum_x'])
                        -turnover_data_sum['rtn_d_cum_y']/np.sum(turnover_data_sum['rtn_d_cum_y'])))
                    
                    
                    
                    locals()['rst_rtn_d_past_{}'.format(i)] = rst_rtn_d
                
 

                
            else:
                first_data = raw_data[raw_data['TRD_DATE']==rebalancing_date.iloc[n,0]] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                first_data = first_data[first_data['ISKOSPI200']==1]
                
                second_data = raw_data[raw_data['TRD_DATE']==rebalancing_date.iloc[n+1,0]]
                second_data = second_data[second_data['ISKOSPI200']==1]
                second_data = pd.DataFrame(second_data.loc[:,['TRD_DATE_OLD','GICODE']])
                first_data = pd.merge(first_data,second_data,how='outer',on='GICODE')
                first_data = first_data[(first_data['TRD_DATE_OLD_x'].notnull())&(first_data['TRD_DATE_OLD_y'].notnull())]
                first_data['div_yield']=first_data['CASH_DIV_COM_Y']/first_data['MARKET_CAP_COM']
                first_data['1/per']= first_data['ADJ_NI_12M_FWD']/first_data['MARKET_CAP_COM']
                first_data['1/pbr'] = first_data['EQUITY']/first_data['MARKET_CAP_COM']
               
                first_data = first_data[first_data[factor].notnull()]
                first_data = first_data.assign(rnk = first_data.loc[:,factor].rank(method='first',ascending = False))
                q_1 = first_data[first_data['rnk']<=stock_num]
                        
                try:
                    reb_next_day = day_date.loc[day_date.loc[day_date['TRD_DATE']==rebalancing_date.iloc[n+1,0]].index[0]+1,'TRD_DATE'] # 리밸런싱 다음날까지의 가격데이터가 필요하다!!
                    rtn_d_need=daily_return[(daily_return['TRD_DATE']<=reb_next_day)&(daily_return['TRD_DATE']>rebalancing_date.iloc[n,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
                except:
                    rtn_d_need=daily_return[(daily_return['TRD_DATE']<=rebalancing_date.iloc[n+1,0])&(daily_return['TRD_DATE']>rebalancing_date.iloc[n,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
                
                for i in range(1,2):
                    rst_rtn_d=pd.merge(locals()['q_{}'.format(i)],rtn_d_need,how='inner',on='GICODE') # 선택된 주식과 일별데이타 merge
                    rst_rtn_d['rtn_d'] = rst_rtn_d.groupby('GICODE')['ADJ_PRC_D'].apply(lambda x: x.pct_change()+1) # gross return으로 바꿔줌
                    rst_rtn_d.loc[(rst_rtn_d['TRD_DATE_y']==rst_rtn_d.loc[0,'TRD_DATE_y']),'rtn_d'] = locals()['end_wealth_{}'.format(i)]  / len(locals()['q_{}'.format(i)])
                    rst_rtn_d['rtn_d_cum']=rst_rtn_d.groupby('GICODE')['rtn_d'].cumprod() # 각 주식별 누적수익률
                    
                   
                    locals()['wealth_{}'.format(i)].append(rst_rtn_d.groupby('TRD_DATE_y').sum()['rtn_d_cum']) # list로 쭈욱 받고
                    
                    locals()['end_wealth_{}'.format(i)] = locals()['wealth_{}'.format(i)][locals()['wealth_num_{}'.format(i)]][-1]
                    locals()['wealth_num_{}'.format(i)]+=1
                    
                    
                    
                    if locals()['turno_{}'.format(i)] == 0:
                        locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n,0]] = 1
                        locals()['turno_{}'.format(i)]+= 1
                    else:
                        turnover_data_sum=pd.merge(rst_rtn_d[rst_rtn_d['TRD_DATE_y']==rst_rtn_d.loc[0,'TRD_DATE_y']],locals()['rst_rtn_d_past_{}'.format(i)][locals()['rst_rtn_d_past_{}'.format(i)]['TRD_DATE_y']==rst_rtn_d.loc[0,'TRD_DATE_y']],how='outer',on='GICODE')
                        turnover_data_sum = turnover_data_sum.replace(np.nan,0)  
                        locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n,0]] = np.sum(abs(turnover_data_sum['rtn_d_cum_x']/np.sum(turnover_data_sum['rtn_d_cum_x'])
                        -turnover_data_sum['rtn_d_cum_y']/np.sum(turnover_data_sum['rtn_d_cum_y'])))
                    
                    
                    
                    locals()['rst_rtn_d_past_{}'.format(i)] = rst_rtn_d                
                
            
for i in range(1,2):

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
    
#    locals()['dd_port_{}'.format(i)] = drawdown(pd.DataFrame(locals()['net_wealth_{}'.format(i)].pct_change(1)))
##            return_final = np.product(return_data[return_data!=0].dropna(axis=1),axis=1)   # return_data[return_data!=0].dropna(axis=1) => 0인걸 nan으로 바꾸고 다시 nan을 버린다!
#    locals()['mdd_port_{}'.format(i)] = locals()['dd_port_{}'.format(i)].min()
#    
#    locals()['sharpe_{}'.format(i)] = np.sqrt(252)*(locals()['net_daily_gross_rtn_{}'.format(i)][1:]-1).mean() / (locals()['net_daily_gross_rtn_{}'.format(i)][1:]-1).std()
net_wealth = pd.DataFrame()
for i in range(1,2):
    net_wealth = pd.concat([net_wealth,locals()['wealth_{}'.format(i)]],axis=1)     

net_wealth.columns = range(1,2)


net_wealth.to_pickle(str(factor))

