# -*- coding: utf-8 -*-
"""
Created on Mon Aug  5 16:34:30 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 14:07:08 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 08:46:06 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 15:57:56 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 15:45:47 2019

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
MATKET_CAP 대신 MARKET_CAP_COM(보통주 시가총액)사용하기로 함.
"""


# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 10:58:42 2018

결과에는 큰 차이가 업지만 groupby와 pct_change를 쓸때 워킹이 안되던 문제를 apply를 활용해서 해결했다.
rst_rtn_d['RTN_D'] = rst_rtn_d.groupby('GICODE')['ADJ_PRC_D'].pct_change() + 1
에서
rst_rtn_d['RTN_D'] = rst_rtn_d.groupby('GICODE')['ADJ_PRC_D'].apply(lambda x: x.pct_change()+1)



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
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd
#from drawdown import drawdown
import copy
from Performance_Evaluation_v2 import Performance_Evaluation
class QVGSM_VALUE:
 
    def __init__(self,raw_data,rebalancing_date,kospi_day,daily_return,wics_big,wics_mid,factor,universe):
        raw_data = raw_data
        daily_return = daily_return
        rebalancing_date = rebalancing_date
        day_date = kospi_day.reset_index()
        factor = 'FIR'
        uni = '코스피200'
        col_length = len(rebalancing_date)-1 #rebalancing_date의 길이는 66이다. range로 이렇게 하면 0부터 65까지 66개의 i 가 만들어진다. -1을 해준건 실제 수익률은 -1개가 생성되기 때문.
        daily_date=pd.DataFrame(daily_return.groupby('TRD_DATE').count().reset_index()['TRD_DATE'])
        wics_mid = wics_mid
        wics_big = wics_big
        if factor == 'CFOA': # 코스닥이 49부터 가능..
            start_n = 49
        elif factor == 'GPOA':
            start_n = 21
        else:
            start_n = 20
    
    def set_universe(first_data,cap_bottom,cap_ceil):
#        first_data = first_data
        if uni == "코스피200" :
            first_data = first_data[first_data['ISKOSPI200']==1]
        elif uni == "코스피":
            first_data = first_data[(first_data['CAP_SIZE']==1)|(first_data['CAP_SIZE']==2)|(first_data['CAP_SIZE']==3)]

        elif uni == "코스닥":
            first_data = first_data[(first_data['ISKOSDAQ']=='KOSDAQ')]
            
        elif uni == "코스피+코스닥":
            first_data = first_data[(first_data['CAP_SIZE']==1)|(first_data['CAP_SIZE']==2)|(first_data['CAP_SIZE']==3)|(first_data['ISKOSDAQ']=='KOSDAQ')]
            
        elif uni == "코스피중소형":
            first_data = first_data[(first_data['CAP_SIZE']==2)|(first_data['CAP_SIZE']==3)]

        elif uni == "코스피+코스닥중소형":
            first_data = first_data[(first_data['MARKET_CAP_COM']>100000000000)&(first_data['MARKET_CAP_COM']<1000000000000)]
         
            
        if cap_bottom != "":
            first_data = first_data[first_data['MARKET_CAP_COM']>=cap_bottom]

        if cap_ceil != "":
            first_data = first_data[first_data['MARKET_CAP_COM']<=cap_ceil]

        return first_data
        
    def set_factors(first_data): # 2매달 바뀌는 친구들은 제외
        if factor == '1/per':
            first_data[factor] = first_data['ADJ_NI_12M_FWD']/first_data['MARKET_CAP_COM']
        elif factor == '1/pbr':
            first_data[factor] = first_data['EQUITY']/first_data['MARKET_CAP_COM']
        elif factor == 'div_yield':
            first_data[factor]=first_data['CASH_DIV_COM']/first_data['MARKET_CAP_COM']
        elif factor == 'ROE':
            first_data[factor]=first_data['NI']/first_data['EQUITY']
        elif factor == 'ROA':
            first_data[factor]=first_data['NI']/first_data['ASSET']
        elif factor == 'size':
            first_data[factor]=first_data['MARKET_CAP_COM']
        elif factor == 'CFOA':
            first_data[factor]=first_data['CFO_TTM'] / first_data['ASSET']
        elif factor == 'GPOA':
            first_data[factor]=first_data['GROSS_PROFIT_TTM'] / first_data['ASSET']

#raw_data['GPOA'] = raw_data['GROSS_PROFIT_TTM']/raw_data['ASSET']
            
        first_data = first_data[first_data[factor].notnull()]
        return first_data

   
    def QVGSM(self,cap_bottom,cap_ceil):
        
#        for i in range(1,6):
#            locals()['wics_mid_df_{}'.format(i)]=pd.DataFrame(data = np.zeros((wics_mid.shape[0],0)),index = wics_mid['WICS_MID'])
#
#        for i in range(1,6):
#            locals()['wics_big_df_{}'.format(i)]=pd.DataFrame(data = np.zeros((wics_big.shape[0],0)),index = wics_big['WICS_BIG'])
#      

        for i in range(1,6): # 5분위를 저장해야 하기 때문에 모든 변수를 5개씩 선언해준다.
            locals()['end_wealth_{}'.format(i)] = 1 # 포트폴리오의 시작 wealth = 1
            locals()['turno_{}'.format(i)] = 0 # 가장 처음 리밸런싱을 잡기 위한 변수
            locals()['wealth_{}'.format(i)] = list() # 매 리밸런싱때의 wealth 변화를 list로 저장
            locals()['wealth_num_{}'.format(i)] = 0 # 리밸런싱 할때마다 wealth의 리스트 row가 증가하기 때문에 같이 늘려주는 변수
            locals()['turnover_day_{}'.format(i)] = pd.DataFrame(np.zeros(shape = (daily_date.shape[0], daily_date.shape[1])),index = daily_date['TRD_DATE'])
            locals()['excess_rtn_sum_{}'.format(i)] = list()
    
        



        for n in range(40,col_length): 
#        for n in range(20,21): 
            if rebalancing_date.iloc[n,0][5:7] =='02':
                n-=1
    
                first_data = raw_data[raw_data['TRD_DATE']==rebalancing_date.iloc[n,0]] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                first_data = set_universe(first_data,cap_bottom,cap_ceil)
                                
                first_data['MARKET_CAP_COM'] = first_data['MARKET_CAP_COM_2LEAD']
                first_data['ADJ_NI_12M_FWD'] = first_data['ADJ_NI_12M_FWD_2LEAD']
                first_data['NI_12M_FWD'] = first_data['NI_12M_FWD_2LEAD']
#                first_data = first_data[first_data['MARKET_CAP']>100000000000]
                
                #과거의 8ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n-1,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-3,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-6,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-9,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-12,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-15,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-18,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-21,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==8]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n-1,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fifth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_8ma'})
                 
                #과거의 4ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n-1,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-3,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-6,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-9,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==4]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n-1,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fourth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_4ma'})
                fifth_data = pd.merge(fifth_data,fourth_data,how='inner',on=['TRD_DATE','GICODE','CO_NM','ADJ_PRC'])
                
                #과거의 2ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n-1,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-3,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==2]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n-1,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fourth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_2ma'})
                fifth_data = pd.merge(fifth_data,fourth_data,how='inner',on=['TRD_DATE','GICODE','CO_NM','ADJ_PRC'])
                
               
                
                #현재의 8ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-3,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-6,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-9,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-12,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-15,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-18,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-21,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==8]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fifth_data_ = fourth_data.rename(columns={'ROE_x_y':'ROE_8ma_new'})
                 
                #현재의 4ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-3,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-6,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-9,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==4]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fourth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_4ma_new'})
                fifth_data_ = pd.merge(fifth_data_,fourth_data,how='inner',on=['TRD_DATE','GICODE','CO_NM','ADJ_PRC'])
                
                #현재의 2ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-3,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==2]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fourth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_2ma_new'})
                fifth_data_ = pd.merge(fifth_data_,fourth_data,how='inner',on=['TRD_DATE','GICODE','CO_NM','ADJ_PRC'])
                
                
                sixth_data = pd.merge(fifth_data,fifth_data_,how='inner',on=['GICODE','CO_NM'])
                               
                
                
                final_data = pd.merge(sixth_data,first_data[['GICODE','ROE']],how='left',on='GICODE')
                final_data['return'] = final_data['ADJ_PRC_y']/final_data['ADJ_PRC_x']
                final_data = final_data[final_data['return'].notnull()].reset_index(drop=True)
                final_data = final_data[final_data['ROE'].notnull()]  
                model = LinearRegression(fit_intercept=True)
                model = model.fit(np.array(final_data[['ROE_2ma','ROE_4ma','ROE_8ma']]).reshape(-1,3),np.array(final_data['return']).reshape(-1,1))
                model.coef_
                model.intercept_
                y_new = pd.DataFrame(model.predict(np.array(final_data[['ROE_2ma_new','ROE_4ma_new','ROE_8ma_new']]).reshape(-1,3)))
                final_data['FIR'] = y_new
                first_data = final_data
                

                for i in range(1,6):
                    locals()['q_{}'.format(i)]=first_data[(first_data[factor]>first_data[factor].quantile(1-0.2*(i)))
                                                        &(first_data[factor]<=first_data[factor].quantile(1 - 0.2*(i-1)))]
                for i in range(5,6):
                    locals()['q_{}'.format(i)]=first_data[(first_data[factor]>=first_data[factor].quantile(1-0.2*(i)))
                                                        &(first_data[factor]<=first_data[factor].quantile(1 - 0.2*(i-1)))]
                
                
#                for i in range(1,6):
#                    locals()['wics_mid_df_{}'.format(i)] = pd.merge(locals()['wics_mid_df_{}'.format(i)],pd.DataFrame(data = locals()['q_{}'.format(i)].groupby('WICS_MID').count().iloc[:,0]).rename(columns={'TRD_DATE':n}),left_index=True,right_index=True,how='left')            
##                a = pd.merge(wics_mid_df,pd.DataFrame(data = q_1.groupby('WICS_MID').count().iloc[:,0]).rename(columns={'TRD_DATE':n}),left_index=True,right_index=True,how='left')            
#                for i in range(1,6):
#                    locals()['wics_big_df_{}'.format(i)] = pd.merge(locals()['wics_big_df_{}'.format(i)],pd.DataFrame(data = locals()['q_{}'.format(i)].groupby('WICS_BIG').count().iloc[:,0]).rename(columns={'TRD_DATE':n}),left_index=True,right_index=True,how='left')            

                try:
                    reb_next_day = day_date.loc[day_date.loc[day_date['TRD_DATE']==rebalancing_date.iloc[n+2,0]].index[0]+1,'TRD_DATE'] # 리밸런싱 다음날까지의 가격데이터가 필요하다!!
                    rtn_d_need=daily_return[(daily_return['TRD_DATE']<=reb_next_day)&(daily_return['TRD_DATE']>rebalancing_date.iloc[n+1,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
                except:
                    rtn_d_need=daily_return[(daily_return['TRD_DATE']<=rebalancing_date.iloc[n+2,0])&(daily_return['TRD_DATE']>rebalancing_date.iloc[n+1,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
                
                
                
                for i in range(1,6):
                    rst_rtn_d=pd.merge(locals()['q_{}'.format(i)],rtn_d_need,how='inner',on='GICODE') # 선택된 주식과 일별데이타 merge
                    rst_rtn_d['rtn_d'] = rst_rtn_d.groupby('GICODE')['ADJ_PRC_D'].apply(lambda x: x.pct_change()+1) # gross return으로 바꿔줌
#                    rst_rtn_d['rtn_d_excess'] = rst_rtn_d['rtn_d']-1 # 수익률의 단순 합을 구하기 위해서 만듬
                    rst_rtn_d.loc[(rst_rtn_d['TRD_DATE']==rst_rtn_d.loc[0,'TRD_DATE']),'rtn_d'] = locals()['end_wealth_{}'.format(i)]  / len(locals()['q_{}'.format(i)])
                    rst_rtn_d['rtn_d_cum']=rst_rtn_d.groupby('GICODE')['rtn_d'].cumprod() # 각 주식별 누적수익률
                    
#                    locals()['excess_rtn_sum_{}'.format(i)].append(rst_rtn_d.groupby('TRD_DATE').mean()['rtn_d_excess'])
                    locals()['wealth_{}'.format(i)].append(rst_rtn_d.groupby('TRD_DATE').sum()['rtn_d_cum']) # list로 쭈욱 받고
                    
                    locals()['end_wealth_{}'.format(i)] = locals()['wealth_{}'.format(i)][locals()['wealth_num_{}'.format(i)]][-1]
                    locals()['wealth_num_{}'.format(i)]+=1
                    
                    
                    
                    if locals()['turno_{}'.format(i)] == 0:
                        locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n+1,0]] = 1
                        locals()['turno_{}'.format(i)]+= 1
                    else:
                        turnover_data_sum=pd.merge(rst_rtn_d[rst_rtn_d['TRD_DATE']==rst_rtn_d.loc[0,'TRD_DATE']],locals()['rst_rtn_d_past_{}'.format(i)][locals()['rst_rtn_d_past_{}'.format(i)]['TRD_DATE']==rst_rtn_d.loc[0,'TRD_DATE']],how='outer',on='GICODE')
                        turnover_data_sum = turnover_data_sum.replace(np.nan,0)  
                        locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n+1,0]] = np.sum(abs(turnover_data_sum['rtn_d_cum_x']/np.sum(turnover_data_sum['rtn_d_cum_x'])
                        -turnover_data_sum['rtn_d_cum_y']/np.sum(turnover_data_sum['rtn_d_cum_y'])))
                    
                    
                    
                    locals()['rst_rtn_d_past_{}'.format(i)] = rst_rtn_d
                print(rebalancing_date.iloc[n+1,0])
 

                
            else:
                
                first_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n,0])]
                first_data = set_universe(first_data,cap_bottom,cap_ceil)
#                first_data = set_factors(first_data).loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE']]
                
                #과거의 8ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n-1,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-3,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-6,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-9,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-12,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-15,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-18,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-21,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==8]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n-1,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fifth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_8ma'})
                 
                #과거의 4ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n-1,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-3,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-6,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-9,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==4]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n-1,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fourth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_4ma'})
                fifth_data = pd.merge(fifth_data,fourth_data,how='inner',on=['TRD_DATE','GICODE','CO_NM','ADJ_PRC'])
                
                #과거의 2ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n-1,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-1-3,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==2]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n-1,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fourth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_2ma'})
                fifth_data = pd.merge(fifth_data,fourth_data,how='inner',on=['TRD_DATE','GICODE','CO_NM','ADJ_PRC'])
                
               
                
                #현재의 8ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-3,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-6,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-9,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-12,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-15,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-18,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-21,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==8]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fifth_data_ = fourth_data.rename(columns={'ROE_x_y':'ROE_8ma_new'})
                 
                #현재의 4ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-3,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-6,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-9,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==4]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fourth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_4ma_new'})
                fifth_data_ = pd.merge(fifth_data_,fourth_data,how='inner',on=['TRD_DATE','GICODE','CO_NM','ADJ_PRC'])
                
                #현재의 2ma 구하기
                second_data = raw_data[(raw_data['TRD_DATE']==rebalancing_date.iloc[n,0])|
                        (raw_data['TRD_DATE']==rebalancing_date.iloc[n-3,0])] # rebalanging할 날짜에 들어있는 모든 db data를 받아온다.
                second_data = set_universe(second_data,cap_bottom,cap_ceil)
#                second_data = set_factors(second_data)
                a = second_data.groupby('GICODE').count()['ROE'].reset_index()
                third_data = pd.merge(second_data,a,how='left',on='GICODE')
                third_data = third_data[third_data['ROE_y']==2]
                a = third_data.groupby('GICODE')['ROE_x'].mean().reset_index()
                
                fourth_data = pd.merge(third_data,a,how='left',on='GICODE')
                fourth_data = fourth_data[fourth_data['TRD_DATE']==rebalancing_date.iloc[n,0]].loc[:,['TRD_DATE','GICODE','CO_NM','ADJ_PRC','ROE_x_y']]
                fourth_data = fourth_data.rename(columns={'ROE_x_y':'ROE_2ma_new'})
                fifth_data_ = pd.merge(fifth_data_,fourth_data,how='inner',on=['TRD_DATE','GICODE','CO_NM','ADJ_PRC'])
                
                
                sixth_data = pd.merge(fifth_data,fifth_data_,how='inner',on=['GICODE','CO_NM'])
                               
                
                
                final_data = pd.merge(sixth_data,first_data[['GICODE','ROE']],how='left',on='GICODE')
                final_data['return'] = final_data['ADJ_PRC_y']/final_data['ADJ_PRC_x']
                final_data = final_data[final_data['return'].notnull()].reset_index(drop=True)
                final_data = final_data[final_data['ROE'].notnull()]  
                model = LinearRegression(fit_intercept=True)
                model = model.fit(np.array(final_data[['ROE_2ma','ROE_4ma','ROE_8ma']]).reshape(-1,3),np.array(final_data['return']).reshape(-1,1))
                model.coef_
                model.intercept_
                y_new = pd.DataFrame(model.predict(np.array(final_data[['ROE_2ma_new','ROE_4ma_new','ROE_8ma_new']]).reshape(-1,3)))
                final_data['FIR'] = y_new
                first_data = final_data
                

                
                for i in range(1,6):
                    locals()['q_{}'.format(i)]=first_data[(first_data[factor]>first_data[factor].quantile(1-0.2*(i)))
                                                        &(first_data[factor]<=first_data[factor].quantile(1 - 0.2*(i-1)))]
                for i in range(5,6):
                    locals()['q_{}'.format(i)]=first_data[(first_data[factor]>=first_data[factor].quantile(1-0.2*(i)))
                                                        &(first_data[factor]<=first_data[factor].quantile(1 - 0.2*(i-1)))]
                
#                for i in range(1,6):
#                    locals()['wics_mid_df_{}'.format(i)] = pd.merge(locals()['wics_mid_df_{}'.format(i)],pd.DataFrame(data = locals()['q_{}'.format(i)].groupby('WICS_MID').count().iloc[:,0]).rename(columns={'TRD_DATE':n}),left_index=True,right_index=True,how='left')            
##                a = pd.merge(wics_mid_df,pd.DataFrame(data = q_1.groupby('WICS_MID').count().iloc[:,0]).rename(columns={'TRD_DATE':n}),left_index=True,right_index=True,how='left')            
#                for i in range(1,6):
#                    locals()['wics_big_df_{}'.format(i)] = pd.merge(locals()['wics_big_df_{}'.format(i)],pd.DataFrame(data = locals()['q_{}'.format(i)].groupby('WICS_BIG').count().iloc[:,0]).rename(columns={'TRD_DATE':n}),left_index=True,right_index=True,how='left')            

                try:
                    reb_next_day = day_date.loc[day_date.loc[day_date['TRD_DATE']==rebalancing_date.iloc[n+1,0]].index[0]+1,'TRD_DATE'] # 리밸런싱 다음날까지의 가격데이터가 필요하다!!
                    rtn_d_need=daily_return[(daily_return['TRD_DATE']<=reb_next_day)&(daily_return['TRD_DATE']>rebalancing_date.iloc[n,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
                except:
                    rtn_d_need=daily_return[(daily_return['TRD_DATE']<=rebalancing_date.iloc[n+1,0])&(daily_return['TRD_DATE']>rebalancing_date.iloc[n,0])] # 리밸런싱날부터 다음 리밸런싱날까지의 일별 데이타
                

                for i in range(1,6):
                    rst_rtn_d=pd.merge(locals()['q_{}'.format(i)],rtn_d_need,how='inner',on='GICODE') # 선택된 주식과 일별데이타 merge
                    rst_rtn_d['rtn_d'] = rst_rtn_d.groupby('GICODE')['ADJ_PRC_D'].apply(lambda x: x.pct_change()+1) # gross return으로 바꿔줌
#                    rst_rtn_d['rtn_d_excess'] = rst_rtn_d['rtn_d']-1
                    rst_rtn_d.loc[(rst_rtn_d['TRD_DATE']==rst_rtn_d.loc[0,'TRD_DATE']),'rtn_d'] = locals()['end_wealth_{}'.format(i)]  / len(locals()['q_{}'.format(i)])
                    rst_rtn_d['rtn_d_cum']=rst_rtn_d.groupby('GICODE')['rtn_d'].cumprod() # 각 주식별 누적수익률
                    
#                    locals()['excess_rtn_sum_{}'.format(i)].append(rst_rtn_d.groupby('TRD_DATE').mean()['rtn_d_excess'])
                    locals()['wealth_{}'.format(i)].append(rst_rtn_d.groupby('TRD_DATE').sum()['rtn_d_cum']) # list로 쭈욱 받고
                    
                    locals()['end_wealth_{}'.format(i)] = locals()['wealth_{}'.format(i)][locals()['wealth_num_{}'.format(i)]][-1]
                    locals()['wealth_num_{}'.format(i)]+=1
                    
                    
                    
                    if locals()['turno_{}'.format(i)] == 0:
                        locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n,0]] = 1
                        locals()['turno_{}'.format(i)]+= 1
                    else:
                        turnover_data_sum=pd.merge(rst_rtn_d[rst_rtn_d['TRD_DATE']==rst_rtn_d.loc[0,'TRD_DATE']],locals()['rst_rtn_d_past_{}'.format(i)][locals()['rst_rtn_d_past_{}'.format(i)]['TRD_DATE']==rst_rtn_d.loc[0,'TRD_DATE']],how='outer',on='GICODE')
                        turnover_data_sum = turnover_data_sum.replace(np.nan,0)  
                        locals()['turnover_day_{}'.format(i)].loc[rebalancing_date.iloc[n,0]] = np.sum(abs(turnover_data_sum['rtn_d_cum_x']/np.sum(turnover_data_sum['rtn_d_cum_x'])
                        -turnover_data_sum['rtn_d_cum_y']/np.sum(turnover_data_sum['rtn_d_cum_y'])))
                    
                    
                    
                    locals()['rst_rtn_d_past_{}'.format(i)] = rst_rtn_d                
                print(rebalancing_date.iloc[n,0])
            
        for i in range(1,6):
        
            locals()['wealth_{}'.format(i)] = pd.concat(locals()['wealth_{}'.format(i)]) # 맨 마지막에 리스트를 풀어서 시리즈로 만들어줌
            locals()['wealth_{}'.format(i)]=locals()['wealth_{}'.format(i)][~locals()['wealth_{}'.format(i)].index.duplicated(keep='first')] #index 중복제거 
        #    locals()['excess_rtn_sum_{}'.format(i)] = pd.concat(locals()['excess_rtn_sum_{}'.format(i)])
        #    locals()['excess_rtn_sum_{}'.format(i)] = locals()['excess_rtn_sum_{}'.format(i)][locals()['excess_rtn_sum_{}'.format(i)].notnull()]
            locals()['daily_gross_rtn_{}'.format(i)]=pd.DataFrame(locals()['wealth_{}'.format(i)].pct_change()+1) # wealth의 누적에서 일별 gross 수익률을 구함.
            locals()['daily_gross_rtn_{}'.format(i)][np.isnan(locals()['daily_gross_rtn_{}'.format(i)])] = 0             # 첫번째 수익률이 nan이기 떄문에 바꿔준다.
            locals()['turnover_day_{}'.format(i)] = locals()['turnover_day_{}'.format(i)].shift(1) * 0.005 # turnover 구한거를 리밸런싱 다음날에 반영해준다.
            locals()['sub_{}'.format(i)] = pd.merge(locals()['daily_gross_rtn_{}'.format(i)],
                                                    locals()['turnover_day_{}'.format(i)],left_index=True,right_index=True)
            locals()['net_daily_gross_rtn_{}'.format(i)]=locals()['sub_{}'.format(i)].iloc[:,0]-locals()['sub_{}'.format(i)].iloc[:,1]
            locals()['net_daily_gross_rtn_{}'.format(i)][0] = 1 # 누적 wealth를 구하기 위해 첫날 수익률을 1이라고 가정.
            locals()['net_wealth_{}'.format(i)]=locals()['net_daily_gross_rtn_{}'.format(i)].cumprod()
        
        
        
        net_wealth = pd.DataFrame()
        for i in range(1,6):
            net_wealth = pd.concat([net_wealth,locals()['net_wealth_{}'.format(i)]],axis=1)
        net_wealth.columns = range(1,6)
        
        return net_wealth
#    locals()['dd_port_{}'.format(i)] = drawdown(pd.DataFrame(locals()['net_wealth_{}'.format(i)].pct_change(1)))
##            return_final = np.product(return_data[return_data!=0].dropna(axis=1),axis=1)   # return_data[return_data!=0].dropna(axis=1) => 0인걸 nan으로 바꾸고 다시 nan을 버린다!
#    locals()['mdd_port_{}'.format(i)] = locals()['dd_port_{}'.format(i)].min()
#    
#    locals()['sharpe_{}'.format(i)] = np.sqrt(252)*(locals()['net_daily_gross_rtn_{}'.format(i)][1:]-1).mean() / (locals()['net_daily_gross_rtn_{}'.format(i)][1:]-1).std()







