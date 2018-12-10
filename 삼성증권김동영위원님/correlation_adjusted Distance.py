# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 08:53:15 2018

@author: 지형범

김동영 위원의 Advanced 매크로 다이내믹 모델 예제


"""



import pandas as pd
import numpy as np
import cx_Oracle
from scipy import stats
#이거 두개 반드시 선언!
cx0=cx_Oracle.makedsn("localhost",1521,"xe")
connection = cx_Oracle.connect("lshlsh135","2tkdgns2",cx0) #이게 실행이 안될때가 있는데
kospi_day = pd.read_sql("""select * from kospi_day_prc_20181130""",con=connection) # 코스피 일별 종가
kospi_day.set_index('TRD_DATE',inplace=True)
datetime=pd.DataFrame(pd.to_datetime(kospi_day.index)) # 월말 날짜를 뽑기위해 datetime으로 바꿔줌
kospi_day = pd.DataFrame(kospi_day, index = datetime['TRD_DATE'])


start_date = '2018-11-30'

start_date_iloc = kospi_day.index.get_loc(kospi_day.index[kospi_day.index==start_date][0])
end_date_iloc = start_date_iloc -120
base_data = kospi_day.iloc[end_date_iloc+1:start_date_iloc+1,0]

i=0
range_start_date = '2005-12-29'
result = dict()
for i in range(len(kospi_day[kospi_day.index>=range_start_date])-241):
        
    compare_data = kospi_day.iloc[end_date_iloc+1-120-i:end_date_iloc+1-i,0]
    #두 지수를 비교하기 위해서는 시작을 100으로 만들어준다(index)
    base_data_index= base_data / base_data[-1] *100
    compare_data_index = compare_data / compare_data[-1] *100
    
    diff = base_data_index.reset_index(drop=True) - compare_data_index.reset_index(drop=True)
    diff_sq = diff**2
    
    #var_data = pd.concat([base_data_index.reset_index(drop=True),compare_data_index.reset_index(drop=True)])
    #var_data = pd.concat([base_data.reset_index(drop=True),compare_data.reset_index(drop=True)])
    var_data = kospi_day[(kospi_day.index>=range_start_date)&(kospi_day.index<=base_data.index[-1])]
    ivar = (var_data.std() / var_data.mean() *100 ) **2
    
    indexed_distance = (diff_sq.sum() / ivar /120) ** (1/2)
    correlation = stats.pearsonr(base_data,compare_data)[0]
    
    indexed_corr_adj_distance = indexed_distance + (1 - correlation)

    result[indexed_corr_adj_distance[0]]=compare_data_index


# =============================================================================
# 거리가 1보다 작은 sample의 향후 1개월 수익률 저장해본다.
# =============================================================================
import pandas as pd
import numpy as np
import cx_Oracle
from scipy import stats
#이거 두개 반드시 선언!
cx0=cx_Oracle.makedsn("localhost",1521,"xe")
connection = cx_Oracle.connect("lshlsh135","2tkdgns2",cx0) #이게 실행이 안될때가 있는데
kospi_day = pd.read_sql("""select * from kospi_day_prc_20181130""",con=connection) # 코스피 일별 종가
kospi_day.set_index('TRD_DATE',inplace=True)
datetime=pd.DataFrame(pd.to_datetime(kospi_day.index)) # 월말 날짜를 뽑기위해 datetime으로 바꿔줌
kospi_day = pd.DataFrame(kospi_day, index = datetime['TRD_DATE'])


start_date = '2018-06-29'

start_date_iloc = kospi_day.index.get_loc(kospi_day.index[kospi_day.index==start_date][0])
end_date_iloc = start_date_iloc -120
base_data = kospi_day.iloc[end_date_iloc+1:start_date_iloc+1,0]

i=0
range_start_date = '2005-12-29'
result = dict()
next_data_return = dict()
for i in range(len(kospi_day[kospi_day.index>=range_start_date])-241):
        
    compare_data = kospi_day.iloc[end_date_iloc+1-120-i:end_date_iloc+1-i,0]
    #두 지수를 비교하기 위해서는 시작을 100으로 만들어준다(index)
    base_data_index= base_data / base_data[-1] *100
    compare_data_index = compare_data / compare_data[-1] *100
    
    diff = base_data_index.reset_index(drop=True) - compare_data_index.reset_index(drop=True)
    diff_sq = diff**2
    
    #var_data = pd.concat([base_data_index.reset_index(drop=True),compare_data_index.reset_index(drop=True)])
    #var_data = pd.concat([base_data.reset_index(drop=True),compare_data.reset_index(drop=True)])
    var_data = kospi_day[(kospi_day.index>=range_start_date)&(kospi_day.index<=base_data.index[-1])]
    ivar = (var_data.std() / var_data.mean() *100 ) **2
    
    indexed_distance = (diff_sq.sum() / ivar /120) ** (1/2)
    correlation = stats.pearsonr(base_data,compare_data)[0]
    
    indexed_corr_adj_distance = indexed_distance + (1 - correlation)

    result[indexed_corr_adj_distance[0]]=compare_data_index
    if indexed_corr_adj_distance[0] < 1 :
        next_data = kospi_day.iloc[end_date_iloc+1-i:end_date_iloc+1-i+60,0]
        next_data_return[i] = next_data[-1]/next_data[0] -1

#np.mean(next_data_return.value)
