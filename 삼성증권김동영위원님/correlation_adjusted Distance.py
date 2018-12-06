# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 08:53:15 2018

@author: 지형범
"""



import pandas as pd
import numpy as np
import cx_Oracle

#이거 두개 반드시 선언!
cx0=cx_Oracle.makedsn("localhost",1521,"xe")
connection = cx_Oracle.connect("lshlsh135","2tkdgns2",cx0) #이게 실행이 안될때가 있는데
kospi_day = pd.read_sql("""select * from kospi_day_prc_20181130""",con=connection) # 코스피 일별 종가
kospi_day.set_index('TRD_DATE',inplace=True)
datetime=pd.DataFrame(pd.to_datetime(kospi_day.index)) # 월말 날짜를 뽑기위해 datetime으로 바꿔줌
kospi_day = pd.DataFrame(kospi_day, index = datetime['TRD_DATE'])



base_data = kospi_day.iloc[len(kospi_day)-120-60:len(kospi_day)-60,0]

i=1
compare_data = kospi_day.iloc[len(kospi_day)-1556-120:len(kospi_day)-1556,0]
#두 지수를 비교하기 위해서는 시작을 100으로 만들어준다(index)
base_data_index= base_data / base_data[-1] *100
compare_data_index = compare_data / compare_data[-1] *100

diff = base_data_index.reset_index(drop=True) - compare_data_index.reset_index(drop=True)
diff_sq = diff**2

#var_data = pd.concat([base_data_index.reset_index(drop=True),compare_data_index.reset_index(drop=True)])
var_data = pd.concat([base_data.reset_index(drop=True),compare_data.reset_index(drop=True)])
ivar = (var_data.std() / var_data.mean() *100 ) **2

(diff_sq.sum() / ivar /120) ** (1/2)


