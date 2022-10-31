# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 21:43:24 2022

@author: ASUS
"""
import pandas as pd

group_address = ['0x083eed559f7bf65671d3d16b8b32c478244807bb',
 '0x0dcfb22cc0492bc4947e74d231ee8d23c8e1e77e',
 '0x197f52d0eaef29da5d349ddcb57541b0e391ab64',
 '0x1c531c8a8a0bb99c9e657754e2248a86d2160567',
 '0x25a231abae91db7ab4cca99e939d038a3360d3c6',
 '0x2b8497108d045270d51823256479d92033dd3606',
 '0x2e879f39ea4da0514e1a0ad5c26956b715733d44',
 '0x37fa4641bc2c77d94da6e6a1bfe51d9ca651ef24',
 '0x483fa7a2f41ce1262e93ddc86bbb25e2d4c11f31',
 '0x506214297beaae7f67d3bbc113858ee53b05e08d',
 '0x51a27ab2caa82c06f6b249880becced8f213e2ad',
 '0x6b7457cd5ad057daa6b82681d6104418d93b01b7',
 '0x6c61b6b7786b76bf82ad8877c71b2af9d5f5eda0',
 '0x81806189f9d9c4dd9db3146468a5ed9bb550c7c5',
 '0x8338c45c2449e6453031f3669c84806199103e25',
 '0xae72f2698fd6fd77caf967fb7dbf4e84b4ca68da',
 '0xb31da18a3eb7700c8e36ca38698e9e876172a622',
 '0xbd78ee97bd3201fe5af564e45fe8d998daf79531',
 '0xefcb26da1fe9520d4c097dfbe11310d96a94bf94',
 '0xfe578c8d56be26ad7d6bb232469b2faa92c1d41e']

data_detail = pd.read_csv("batch_transfer_rule_data_final.csv")
data_detail = data_detail[data_detail['address'].isin(group_address)]
data_detail
