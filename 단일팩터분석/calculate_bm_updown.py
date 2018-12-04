# -*- coding: utf-8 -*-
"""
Created on Mon Nov 26 10:17:18 2018

@author: 지형범
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


class calculate_bm_updown:
 
    def __init__(self,kospi200_day,net_wealth):
#        n_w = n_w # 일별 net_wealth
        self.net_wealth = net_wealth
        self.kospi200_day = kospi200_day # 일별 종가 (위에서도 net_wealth를 받아왔기 때문에 계산이 편하다.)
        self.calendar_range = ['2002-04-24','2003-03-17','2005-05-21','2007-11-07','2008-11-20','2009-03-03','2011-05-03','2011-08-01','2011-08-22','2016-11-21','2017-11-10']
        self.datetime_kospi200=pd.DataFrame(pd.to_datetime(self.kospi200_day.index))
        self.kospi200_day = pd.DataFrame(self.kospi200_day['PRC'], index = self.datetime_kospi200['TRD_DATE'])
        self.datetime_net_wealth=pd.DataFrame(pd.to_datetime(self.net_wealth.index))
        self.net_wealth = pd.DataFrame(self.net_wealth, index = self.datetime_net_wealth['TRD_DATE_y'])
        self.kospi200_day = kospi200_day[kospi200_day.index>='2001-11-01']
    def make_kospi200_plot(self):
        
        for i in range(len(self.calendar_range)-1):
            
            a = self.kospi200_day[(self.kospi200_day.index>=self.calendar_range[i])&(self.kospi200_day.index<self.calendar_range[i+1])]
            b = self.net_wealth[(self.net_wealth.index>=self.calendar_range[i])&(self.net_wealth.index<self.calendar_range[i+1])]
            b = b.pct_change()+1
            b.loc[b.index == self.calendar_range[i]] = 1
            b = b.cumprod()
            
            
            fig = plt.figure(figsize=(12,12))
            ax1 = fig.add_subplot(211)
            ax2 = fig.add_subplot(212)
            a.plot(ax=ax1)
            b.plot(ax=ax2)
            plt.tight_layout()
            plt.savefig(str(i)+'.jpg',bbox_inches = 'tight')
            
            
            
            
            
            
#            a.plot(figsize=(12,4)) 
#            b.plot(figsize=(12,4))
#            plt.grid(True)
#            plt.tight_layout()
#            plt.show()
            
        
        
#            plt.figure(figsize=(12,4))           
#            plt.subplot(211)
#            plt.plot(a)   
#  
#            plt.subplot(212)
#            plt.plot(b)
#            plt.tight_layout()
#            plt.savefig(str(i)+'.jpg',bbox_inches = 'tight')