import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
def get_zk_drop(df):
    df_other = df[df['chain'] != 'eth_zksync']
    df_zk = df[df['chain'] == 'eth_zksync']
    df_zk = df_zk.drop_duplicates(['txn_hash','grant_id'])
    df = pd.concat([df_other,df_zk],ignore_index=True)
    return df
def mark_label(df):
    if df['is_anomaly'] == False:
        return df.index
    else:
        return np.nan
def mark_label(df):
    if df['is_anomaly'] == False:
        return df.index
    else:
        return np.nan
        
def create_group(data_save,h_gap):
    m_gap = h_gap*60
    print(m_gap,"分钟")
    le = LabelEncoder()
    data_save['mark'] = data_save.index
    data_save['mark'][(data_save['gap_time'] < m_gap)] = np.nan
    data_save['mark'] = data_save['mark'].fillna(method='bfill')
    data_save['group_id_hour_%s'%(h_gap)] = le.fit_transform(data_save['mark'].astype(str))
    del data_save['mark']
    return data_save


data = pd.read_csv("hackathon-contributions-dataset_v2.csv")
con2 = data['network'] == 'mainnet'
data = data[con2]

print('0 drop',len(data))
data = data.drop_duplicates()
print('1 drop',len(data))
data = get_zk_drop(data)

print('2 drop',len(data))
data.index = range(len(data))
con2 = data['network'] == 'mainnet'
group_cols = ['grant_id','chain','network','token','amount_in_usdt']
data_save = data[con2]
print('data_save',data_save.shape)

data_save_desc = data_save.groupby(['grant_id','chain','network','token','amount_in_usdt']).agg({'address':'nunique'})
data_save_desc = data_save_desc.rename(columns={'address':'address_cnt'})
data_save_desc = data_save_desc.sort_values(by=['address_cnt'],ascending=False)
data_save_desc['group_id'] = list(range(len(data_save_desc)))
data_save_desc = data_save_desc.reset_index()
data_save = data_save.merge(data_save_desc,how='left',on=group_cols)






data_save = data_save.sort_values(by=['group_id','timestamp'])
data_save['time'] = data_save['timestamp'].map(lambda x:x[:19])
data_save['time'] = pd.to_datetime(data_save['time'])
data_save = data_save.sort_values(by=['group_id','time'])
data_save['time1'] = data_save.groupby(['group_id']).time.transform(lambda x:x.shift(-1))
data_save['rank'] = data_save.groupby(['group_id']).time.transform(lambda x:x.rank(method='dense'))
data_save['gap_time'] = (data_save['time1'] - data_save['time']).dt.seconds
data_save['gap_time'] = data_save['gap_time'] / 60

hour_list = [0.5,
#              1,
#              1.5,2,2.5,3,3.5,4
            ]
for h in hour_list:
    data_save = create_group(data_save,h)
hour_group = ['group_id_hour_%s'%(h)for h in hour_list]
hour_group += ['group_id']
for tg_group_id in hour_group:
    print(tg_group_id)
    data_save_desc2 = data_save.groupby([tg_group_id]).agg({'address':'nunique'})
    data_save_desc2 = data_save_desc2.rename(columns={'address':'address_cnt_%s'%(tg_group_id)})
    data_save = data_save.merge(data_save_desc2,how='left',on=[tg_group_id])
    data_save = data_save.sort_values(by=[tg_group_id,'time'])
#     data_save['time1'] = data_save.groupby(['group_id']).time.transform(lambda x:x.shift(-1))
    data_save['time_%s'%(tg_group_id)] = data_save.groupby([tg_group_id]).time.transform(lambda x:x.shift(-1))
    data_save['rank'] = data_save.groupby([tg_group_id]).time.transform(lambda x:x.rank(method='dense'))
    data_save['gap_time_%s'%(tg_group_id)] = (data_save['time_%s'%(tg_group_id)] - data_save['time']).dt.seconds
    data_save['gap_time_%s'%(tg_group_id)] = data_save['gap_time_%s'%(tg_group_id)] / 60
    
def get_time_per(gap_list_or,v):
    gap_list = [gap for gap in gap_list_or if gap >= 0]
    if len(gap_list) > 0:
        return np.percentile(gap_list,v)
    else:
        return np.nan
    
    
    
def get_time_std(gap_list_or):
    gap_list = [gap for gap in gap_list_or if gap >= 0]
    if len(gap_list) > 0:
        return np.std(gap_list)
    else:
        return np.nan
    
def get_time_mean(gap_list_or):
    gap_list = [gap for gap in gap_list_or if gap >= 0]
    if len(gap_list) > 0:
        return np.mean(gap_list)
    else:
        return np.nan


for tg_group_id in hour_group:   
    print(tg_group_id)
    data_save = data_save.sort_values(by=[tg_group_id,'time'])
    data_save['time_01_p_%s'%(tg_group_id)] = data_save.groupby([tg_group_id])['gap_time_%s'%(tg_group_id)].transform(lambda x: get_time_per(x,1))
    data_save['time_05_p_%s'%(tg_group_id)] = data_save.groupby([tg_group_id])['gap_time_%s'%(tg_group_id)].transform(lambda x: get_time_per(x,5))
    data_save['time_10_p_%s'%(tg_group_id)] = data_save.groupby([tg_group_id])['gap_time_%s'%(tg_group_id)].transform(lambda x: get_time_per(x,10))
    data_save['time_25_p_%s'%(tg_group_id)] = data_save.groupby([tg_group_id])['gap_time_%s'%(tg_group_id)].transform(lambda x: get_time_per(x,25))
    data_save['time_50_p_%s'%(tg_group_id)] = data_save.groupby([tg_group_id])['gap_time_%s'%(tg_group_id)].transform(lambda x: get_time_per(x,50))
    data_save['time_75_p_%s'%(tg_group_id)] = data_save.groupby([tg_group_id])['gap_time_%s'%(tg_group_id)].transform(lambda x: get_time_per(x,75))
    data_save['time_std_%s'%(tg_group_id)] = data_save.groupby([tg_group_id])['gap_time_%s'%(tg_group_id)].transform(lambda x: get_time_std(x))
    data_save['time_mean_%s'%(tg_group_id)] = data_save.groupby([tg_group_id])['gap_time_%s'%(tg_group_id)].transform(lambda x: get_time_mean(x))
    data_save['time_cov_%s'%(tg_group_id)] = data_save['time_std_%s'%(tg_group_id)] / data_save['time_mean_%s'%(tg_group_id)]

    
data_save.to_csv("./rule_data/rule_data1_v_check1029.csv",index=False)