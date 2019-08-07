# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 10:25:30 2018

@author: 지형범
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Oct  4 15:17:34 2018

@author: LSH_Notebook
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Mar  2 09:28:25 2018





월별로 할때 주의할 점... 
11 12 1 2 : 3분기 실적
3 4 : 4분기 실적
5 6 7 : 1분기 실적
8 9 10 : 2분기 실적

여기서 문제는 2월달에 4분기 실적을 안쓰고 3분기 실적을 써야 하는것. market cap과 ni_fwd 까다롭









@author: SH-NoteBook
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 15:50:36 2017

@author: SH-NoteBook
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Aug 24 13:27:10 2017

raw_data['sales_cap']=raw_data['SALES']/raw_data['MARKET_CAP']
raw_data['gpro_cap']=raw_data['GROSS_PROFIT']/raw_data['MARKET_CAP']
raw_data['opro_cap']=raw_data['OPE_PROFIT']/raw_data['MARKET_CAP'] # 이놈 시총제한 있고 없고 차이 심한데 
얘네는 계절성때문에 제외

@author: SH-NoteBook
"""
import xlsxwriter
import openpyxl
import pandas as pd
import numpy as np
import cx_Oracle
import itertools
from calculate_bm_updown import calculate_bm_updown
#from one_factor_v4 import QVGSM_VALUE
from one_factor_v5 import QVGSM_VALUE
from Performance_Evaluation_v2 import Performance_Evaluation

#이거 두개 반드시 선언!
cx0=cx_Oracle.makedsn("localhost",1521,"xe")
connection = cx_Oracle.connect("lshlsh135","2tkdgns2",cx0) #이게 실행이 안될때가 있는데
#그때는 services에 들어가서 oracle listner를 실행시켜줘야함
cpi_data = pd.read_sql("""select * from cpi_20181130""",con=connection)
cpi_data.iloc[107,0] = '2009-12-30'
cpi_data.iloc[119,0] = '2010-12-30'
cpi_data.iloc[131,0] = '2011-12-29'
cpi_data.iloc[143,0] = '2012-12-28'
cpi_data.iloc[155,0] = '2013-12-30'
cpi_data.iloc[156,0] = '2014-01-29'
cpi_data.iloc[167,0] = '2014-12-30'
cpi_data.iloc[179,0] = '2015-12-30'
cpi_data.iloc[191,0] = '2016-12-29'
cpi_data.iloc[203,0] = '2017-12-28'



kospi_day = pd.read_sql("""select * from kospi_day_prc_20181130""",con=connection) # 코스피 일별 종가
kospi_day.set_index('TRD_DATE',inplace=True)
kospi200_day = pd.read_sql("""select * from kospi200_day_prc_20181130""",con=connection) # 코스피 일별 종가
kospi200_day.set_index('TRD_DATE',inplace=True)
#a=calculate_bm_updown(kospi200_day,net_wealth)
#a.make_kospi200_plot()
#kospi_quarter = pd.read_sql("""select * from kospi_quarter_return_20180928""",con=connection)
#kospi_month = pd.read_sql("""select * from kospi_month_return_20180928""",con=connection)
#DATA를 가져온다!!
kospi = pd.read_sql("""select lead(trd_date,1) over(partition by gicode order by trd_date) trd_date,trd_date as trd_date_old, gicode, co_nm,iskosdaq,pretax_ni_ttm, wics_mid,wics_big,cap_size,EQUITY,CFO_TTM,cash_div_com,SALES_TTM,lead(ADJ_NI_12M_FWD,1) over(partition by gicode order by trd_date) ADJ_NI_12M_FWD,lead(ADJ_NI_12M_FWD,2) over(partition by gicode order by trd_date) ADJ_NI_12M_FWD_2lead,lead(market_cap,1) over(partition by gicode order by trd_date) market_cap,lead(market_cap,2) over(partition by gicode order by trd_date) market_cap_2lead,adj_prc, asset,asset - equity as liab,ni,liq_equity,liq_debt,(NI-lag(ni,3) over(partition by gicode order by trd_date))/ (ABS(ni) + abs(lag(ni,3) over(partition by gicode order by trd_date))) as chin,lag(ni,12) over(partition by gicode order by trd_date) as ni_1y, gross_profit_ttm ,
                    lead(NI_12M_FWD,1) over(partition by gicode order by trd_date) NI_12M_FWD,lead(NI_12M_FWD,2) over(partition by gicode order by trd_date) NI_12M_FWD_2lead,                    
                    adj_prc/lag(adj_prc,12) over(partition by gicode order by trd_date) rtn_12m,
                    iskospi200, cash_div_com_y,lead(market_cap_com,1) over(partition by gicode order by trd_date) market_cap_com,lead(market_cap_com,2) over(partition by gicode order by trd_date) market_cap_com_2lead,
                    lead(float_cap,1) over(partition by gicode order by trd_date) float_cap,lead(float_cap,2) over(partition by gicode order by trd_date) float_cap_2lead, com_trd_volume_20day_avg
                    from kospi_20181130""",con=connection)
kosdaq = pd.read_sql("""select lead(trd_date,1) over(partition by gicode order by trd_date) trd_date,trd_date as trd_date_old, gicode, co_nm,iskosdaq,pretax_ni_ttm, wics_mid,wics_big,cap_size,EQUITY,CFO_TTM,cash_div_com,SALES_TTM,lead(ADJ_NI_12M_FWD,1) over(partition by gicode order by trd_date) ADJ_NI_12M_FWD,lead(ADJ_NI_12M_FWD,2) over(partition by gicode order by trd_date) ADJ_NI_12M_FWD_2lead,lead(market_cap,1) over(partition by gicode order by trd_date) market_cap,lead(market_cap,2) over(partition by gicode order by trd_date) market_cap_2lead,adj_prc, asset,asset - equity as liab,ni,liq_equity,liq_debt,(NI-lag(ni,3) over(partition by gicode order by trd_date))/ (ABS(ni) + abs(lag(ni,3) over(partition by gicode order by trd_date))) as chin,lag(ni,12) over(partition by gicode order by trd_date) as ni_1y, gross_profit_ttm,
                    lead(NI_12M_FWD,1) over(partition by gicode order by trd_date) NI_12M_FWD,lead(NI_12M_FWD,2) over(partition by gicode order by trd_date) NI_12M_FWD_2lead,                    
                     adj_prc/lag(adj_prc,12) over(partition by gicode order by trd_date) rtn_12m,
                     iskospi200,  cash_div_com_y,lead(market_cap_com,1) over(partition by gicode order by trd_date) market_cap_com,lead(market_cap_com,2) over(partition by gicode order by trd_date) market_cap_com_2lead,
                     lead(float_cap,1) over(partition by gicode order by trd_date) float_cap,lead(float_cap,2) over(partition by gicode order by trd_date) float_cap_2lead, com_trd_volume_20day_avg
                     from kosdaq_20181130""",con=connection)
kospi_temp = pd.read_sql("""select trd_date, gicode, co_nm, jiju from kospi_20181130_temp""",con=connection)
kosdaq_temp = pd.read_sql("""select trd_date, gicode, co_nm, jiju from kosdaq_20181130_temp""",con=connection)
kospi = pd.merge(kospi,kospi_temp,on=['TRD_DATE','GICODE'])
kosdaq = pd.merge(kosdaq,kosdaq_temp,on=['TRD_DATE','GICODE'])

rebalancing_date = pd.read_sql("""select * from month_date_20181130""",con=connection)
#month_date = pd.read_sql("""select * from month_date_20180928""",con=connection)
#wics_mid = pd.read_sql("""select * from wics_mid_20180928""",con=connection)


#kospi_daily_return = pd.read_sql("""select * from kospi_daily_stock """,con=connection)
#kosdaq_daily_return = pd.read_sql("""select * from kosdaq_daily_stock """,con=connection)
daily_return = pd.read_sql("""select trd_date, gicode, co_nm, adj_prc_d, lag(adj_prc_d, 252) over(partition by gicode order by trd_date) adj_prc_d_252 from kospi_daily_stock_20181130 """,con=connection).drop_duplicates()

daily_return = daily_return[daily_return['ADJ_PRC_D'].notnull()] # 메모리 사용량을 줄이기 위해서 실행
#daily_date=pd.DataFrame(daily_return.groupby('TRD_DATE').count().reset_index()['TRD_DATE'])
#wealth = pd.DataFrame(np.zeros(shape = (daily_date.shape[0], daily_date.shape[1])),index = daily_date['TRD_DATE'], columns = ['RTN_D_CUM'])
#turnover_day = pd.DataFrame(np.zeros(shape = (daily_date.shape[0], daily_date.shape[1])),index = daily_date['TRD_DATE'])
raw_data = pd.concat([kospi,kosdaq],axis=0,ignore_index=True).drop_duplicates()   #왜인지 모르겠는데 db에 중복된 정보가 들어가있네 ..? 

col_length = len(rebalancing_date)-1 #rebalancing_date의 길이는 66이다. range로 이렇게 하면 0부터 65까지 66개의 i 가 만들어진다. -1을 해준건 실제 수익률은 -1개가 생성되기 때문.

wics_mid = pd.read_sql("""select distinct wics_mid from kospi_20181130""",con=connection)[1:]


for i in range(1,6):
    locals()['wics_mid_df_{}'.format(i)]=pd.DataFrame(data = np.zeros((wics_mid.shape[0],0)),index = wics_mid['WICS_MID'])
    
wics_big = pd.read_sql("""select distinct wics_big from kosdaq_20181130""",con=connection)[1:]


for i in range(1,6):
    locals()['wics_big_df_{}'.format(i)]=pd.DataFrame(data = np.zeros((wics_big.shape[0],0)),index = wics_big['WICS_BIG'])
      

return_data = pd.DataFrame(np.zeros((1,col_length)))
data_name = pd.DataFrame(np.zeros((200,col_length)))
return_month_data = pd.DataFrame(np.zeros((1,3*col_length)))
quarter_data = pd.DataFrame(np.zeros((200,3*col_length)))
return_final = pd.DataFrame(np.zeros((1,1)))
return_month_data = pd.DataFrame(np.zeros((1,3*col_length)))

#raw_data = raw_data[raw_data['WICS_BIG']!='금융']
#raw_data = raw_data[raw_data['JIJU']!='지주']
#raw_data = raw_data[(raw_data['EQUITY'].notnull())&(raw_data['EQUITY']>0)] #equity가 null인건 망한기업 혹은 기업이 아님
#raw_data['size_FIF_wisefn'] = raw_data['JISU_STOCK']*raw_data['FIF_RATIO']*raw_data['ADJ_PRC'] # 예전에는 코드에서 만들었으나 이제는 한번에 미리 만들자

#raw_data['CO_NM_y'] = raw_data['CO_NM'] # temp 데이타가 추가되게 되면 이게 필요한데 지금은 없어서 그냥 강제로 만들었다.


first_column = len(raw_data.columns)  # 1/pbr 의 loc
#raw_data['gp/a'] = raw_data['GROSS_PROFIT'] / raw_data['ASSET']    # gross profit을 ttm으로..
#raw_data['EBIT_YOY'] = raw_data['OPER_PROFIT_YOY']
# value
#raw_data['TLTA'] = raw_data['LIAB']/raw_data['ADJASSET']
#raw_data['WCTA'] = (raw_data['LIQ_EQUITY']-raw_data['LIQ_DEBT'])/raw_data['ADJASSET']
#raw_data['CLCA'] = raw_data['LIQ_DEBT']/raw_data['LIQ_EQUITY']
#raw_data['OENEG'] = (raw_data['LIAB']>raw_data['ASSET'])*1
#raw_data['NITA'] = raw_data['NI']/raw_data['ASSET']
#raw_data['FUTL'] = raw_data['PRETAX_NI_TTM']/raw_data['LIAB']
#raw_data['GPOA'] = raw_data['GROSS_PROFIT_TTM']/raw_data['ASSET']
#raw_data['ROE'] = raw_data['NI']/raw_data['EQUITY']
#raw_data['ROA'] = raw_data['NI']/raw_data['ASSET']
#raw_data['CFOA'] = raw_data['CFO_TTM']/raw_data['ASSET']
#raw_data['GMAR'] = raw_data['GROSS_PROFIT_TTM']/raw_data['SALES_TTM']
#raw_data['ACC'] = (raw_data['NI']-raw_data['CFO_TTM'])/raw_data['ASSET']
#raw_data['div_yield']=raw_data['CASH_DIV_COM']/raw_data['MARKET_CAP']
#raw_data['1/per']= raw_data['ADJ_NI_12M_FWD']/raw_data['MARKET_CAP']
#raw_data['1/pbr'] = raw_data['EQUITY']/raw_data['MARKET_CAP']

#raw_data['INTWO'] = np.max(raw_data['NI'],raw_data['NI_1Y']) 마지막에 해야겠는걸
raw_data = raw_data.rename(columns={'CO_NM_x':'CO_NM'}) # column 이름 변경
raw_data = raw_data[raw_data['CO_NM']!='에스와이코퍼레이션']

# =============================================================================
# 
# =============================================================================

kospi200_rtn_d  =(kospi200_day.pct_change()+1).fillna(1).cumprod()
#for i in ['코스피','코스닥','코스피200']:  
for i in ['코스닥','코스피+코스닥']:    
    writer = pd.ExcelWriter(i+'.xlsx',engine='xlsxwriter')
    for j in ['GPOA','CFOA']:
        for k in ['',10**11]:
            j_=""
            locals()['result_{}'.format(i)] = dict()
            a = QVGSM_VALUE(raw_data,rebalancing_date,kospi_day,daily_return,wics_big,wics_mid,j,i)
            d = a.QVGSM(k,"")
            b = Performance_Evaluation(d,kospi_day,kospi200_day)
            
            locals()['result_{}'.format(i)]['net_wealth'] = d
            Monthly_PF_EV_results = b.Monthly_PF_EV()
            locals()['result_{}'.format(i)]['year_month_table'] = Monthly_PF_EV_results[0]
            locals()['result_{}'.format(i)]['net_wealth_return&excess_return'] = Monthly_PF_EV_results[1]
            
            month_list = [12,24,36,60,211]
            locals()['result_{}'.format(i)]['statistics_table'] = b.Make_Tables(month_list)
        #    locals()['result_{}'.format(i)]['net_wealth_kospi200'] = pd.merge(d,kospi200_rtn_d,left_index=True, right_index=True, how='inner')
    
            if j == '1/per':
                j_ = '1_per'
            elif j== '1/pbr':
                j_ = '1_pbr'
            else:
                j_ = j
                
            if k != '':
                if j_ != "":
                    j_ = j_ + str(k)
                else:
                    j_ = j + str(k)
                    
            
                
                
                    
                    
            (locals()['result_{}'.format(i)]['net_wealth_return&excess_return']-1).to_excel(writer,j_)
            #xlsx 함수에서 cell foramt 담당
            workbook = writer.book
            worksheet = writer.sheets[j_]
            a = workbook.add_format({'num_format':'0.00%','align':'center'})
            b = workbook.add_format({'num_format':'0.000','align':'center'})
            format1 = workbook.add_format({'bg_color': '#FFC7CE'})
            format2 = workbook.add_format({'bg_color': 'white'})           

            
            locals()['result_{}'.format(i)]['net_wealth'].to_excel(writer,j_,startrow = 1,startcol=14,index = True, header = True)
            #time_df = pd.DataFrame(np.zeros((1,1)))
            #time_df.iloc[0,0] = timestr
            locals()['result_{}'.format(i)]['statistics_table'].to_excel(writer,j_,startrow = 1,startcol=22,index = True, header = True)
            (locals()['result_{}'.format(i)]['year_month_table']-1).to_excel(writer,j_,startrow =1, startcol =29,index = True, header = True)
            worksheet.set_column('B:N',15,a)
            worksheet.set_column('AD:AQ',15,a)
            worksheet.set_column('N:AC',15,b)
            

                                           

            worksheet.conditional_format('AE3:AQ97', {'type': 'blanks',
                                             'format': format2})
            worksheet.conditional_format('AE3:AQ97', {'type': 'cell',
                                                 'criteria': 'between',
                                                 'minimum': 0,
                                                 'maximum': 10,
                                                 'format': format1})
            
            
#            chart = workbook.add_chart({'type': 'line'})
#
#            # Configure the series of the chart from the dataframe data.
#            chart.add_series({'values': "'="+j+k+"!$Q$2:$T$4227'"})
#            
#            # Insert the chart into the worksheet.
#            worksheet.insert_chart('D2', chart)
    
    writer.save()

# =============================================================================
# 
# =============================================================================
a = QVGSM_VALUE(raw_data,rebalancing_date,kospi_day,daily_return,wics_big,wics_mid,j,i)
d = a.QVGSM(10**11,10**12)
