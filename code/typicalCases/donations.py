# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 22:22:03 2022

@author: ASUS
"""
import pandas as pd

group_address = [
'0x67a447c4567207991b3a7c58460ef19fa886c1a1',
'0xa6b4a3eb3ad8be4f52776788b01877713f39e88b',
'0xd00e2c85021d016e52849c167232e4de7e2d5dec',
'0x3eec65102ac4ec0f503d14a3c010b2466ab6658e',
'0x58a177769a7d53b73445798a65026adb38a17180'
    
]

data_detail = pd.read_csv("donation_rule2_feat.csv")
data_detail = data_detail[data_detail['address'].isin(group_address)]
data_detail