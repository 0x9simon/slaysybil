import pandas as pd
import numpy as np


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
        
def amount_rule(x):
    # <=1.1 2
    # <=3   1
    # >3    0
    if x <= 1.02:
        return 2
    elif x <= 3:
        return 1
    else:
        return 0
    
def cov_rule(x):
# <0.63 2
# <5.38 1
# >5.38 0
    if x < 4.24:
        return 2
    elif x < 6.77:
        return 1
    else:
        return 0
    
def group_cnt_rule(x):
# >=11 6
# >5 4
# >1 2
# =1 0
    if x >= 16:
        return 2
    elif x > 5:
        return 1
    else:
        return 0
    
    
    
def time_75_p_rule(x):
    if x < 0.016667:
        return 2
    elif x < 0.03:
        return 1
    else:
        return 0
    
    
def grant_len_rule(x):
    if x >= 10:
        return 2
    elif x >= 4:
        return 1
    else:
        return 0
    
   
input_data_path = "../../data/hackathon-contributions-dataset_v2.csv"
out_data_path = "../../output/"
data = pd.read_csv(input_data_path)
data = data.drop_duplicates()
data = data[data['network'] == 'mainnet']
group_cols = ['grant_id','chain','network','token','amount_in_usdt']
data_grant_list1 = data.groupby(['address']).agg({'grant_id':'unique'})
data_grant_list1['grant_id'] = data_grant_list1['grant_id'].astype(str)
data_grant_list1 = data_grant_list1.rename(columns={'grant_id':'unique_grants'})
data_grant_list1 = data_grant_list1.reset_index()
# data_grant_list1
grant_list_cnt = data_grant_list1.groupby(['unique_grants']).agg({'address':'nunique'})
grant_list_cnt = grant_list_cnt.sort_values(by=['address'],ascending=False)
grant_list_cnt = grant_list_cnt.rename(columns={'address':'grant_list_addr_cnt'})
grant_list_cnt = grant_list_cnt.reset_index()

group_cols = ['grant_id','chain','network','token','amount_in_usdt']
data_grant_list = data.groupby(['address']).agg({'grant_id':['unique','nunique']})
# data_grant_list.columns.name = None
data_grant_list = data_grant_list['grant_id']
data_grant_list = data_grant_list.add_suffix("_grants")
data_grant_list = data_grant_list.reset_index()
grant_list_dfn = pd.DataFrame()
grant_list_dfn['unique_grants'] = data_grant_list['unique_grants'].astype(str).unique().tolist()





grant_list_dfn['grant_label'] = list(range(len(grant_list_dfn)))

data_grant_list['unique_grants'] = data_grant_list['unique_grants'].astype(str)
grant_list_dfn['unique_grants'] = grant_list_dfn['unique_grants'].astype(str)

data_grant_list = data_grant_list.merge(grant_list_dfn,how='left',on=['unique_grants'])


data_grant_list = data_grant_list.merge(grant_list_cnt,how='left',on=['unique_grants'])


data_new= data.merge(data_grant_list,how='left',on=['address'])
# data_new = data_new[data_new['nunique_grants'] >= 3]
data_new['date'] = data_new['timestamp'].map(lambda x:x[:19])

data_new['date2'] = data_new['timestamp'].map(lambda x:x[:10])
data_new['date'] = pd.to_datetime(data_new['date'])



# data_new.head()
data_amount_desc = data_new.groupby(['grant_label','network','token']).agg({'amount_in_usdt':['max','min','mean','median']})['amount_in_usdt']
data_amount_desc = data_amount_desc.add_suffix("_amount").reset_index()
data_new = data_new.merge(data_amount_desc,how='left',on=['grant_label','network','token'])
group_feat = ['grant_label','chain','network','token','max_amount','min_amount','mean_amount','median_amount'
              ,'date2'
             ]
group_feat_df = data_new[group_feat].drop_duplicates()
group_feat_df['group_id'] = range(len(group_feat_df))
data_new = data_new.merge(group_feat_df,how='left',on=group_feat)
# data_new = data_new.sort_values(by=['group_id','date'])
data_new['group_add_cnt'] = data_new.groupby(['group_id']).address.transform(lambda x:len(set(x)))
data_new = data_new.sort_values(by=['group_id','date'])
data_new['address_shift'] = data_new.groupby(['group_id']).address.transform(lambda x:x.shift(-1))
data_new['date1'] = data_new.groupby(['group_id']).date.transform(lambda x:x.shift(-1))
data_new['next_is_same_address'] = (data_new['address'] == data_new['address_shift']).astype(int)
data_new['gap_time'] = (data_new['date1'] - data_new['date']).dt.seconds
data_new['gap_time'] = data_new['gap_time'] / 60

data_new['time_01_p'] = data_new.groupby(['group_id']).gap_time.transform(lambda x: get_time_per(x,1))
data_new['time_05_p'] = data_new.groupby(['group_id']).gap_time.transform(lambda x: get_time_per(x,5))
data_new['time_10_p'] = data_new.groupby(['group_id']).gap_time.transform(lambda x: get_time_per(x,10))
data_new['time_25_p'] = data_new.groupby(['group_id']).gap_time.transform(lambda x: get_time_per(x,25))
data_new['time_50_p'] = data_new.groupby(['group_id']).gap_time.transform(lambda x: get_time_per(x,50))
data_new['time_75_p'] = data_new.groupby(['group_id']).gap_time.transform(lambda x: get_time_per(x,75))
data_new['time_std'] = data_new.groupby(['group_id']).gap_time.transform(lambda x: get_time_std(x))
data_new['time_mean'] = data_new.groupby(['group_id']).gap_time.transform(lambda x: get_time_mean(x))
data_new['time_cov'] = data_new['time_std'] / data_new['time_mean']

data_new['grant_list_ratio'] = data_new['group_add_cnt'] / data_new['grant_list_addr_cnt']
data_new['amount_rule_score'] = data_new['amount_in_usdt'].map(lambda x:amount_rule(x))
    
data_new['cov_rule_score'] = data_new['time_cov'].map(lambda x:cov_rule(x))

data_new['group_add_score'] = data_new['group_add_cnt'].map(lambda x:group_cnt_rule(x))

data_new['time_75_score'] = data_new['time_75_p'].map(lambda x:time_75_p_rule(x))

data_new['grant_len_score'] = data_new['nunique_grants'].map(lambda x:grant_len_rule(x))

score_cols = ['amount_rule_score',
              'cov_rule_score',
              'group_add_score',
              'time_75_score'
              ,'grant_len_score'
             ]

address_describe = data_new.groupby(['address']).agg({
                                                      'amount_rule_score':'max',
                                                      'cov_rule_score':'max',
                                                      'group_add_score':'max',
                                                      'time_75_score':'max',
                                                     'grant_len_score':'max'
                                                     })
address_describe['all_score'] = address_describe[score_cols].sum(axis=1)

address_describe_new = address_describe.reset_index()
data_new_merge = data_new.merge(address_describe_new,how='left',on=['address'])
address_describe.reset_index().groupby(['all_score']).agg({'address':'nunique'}).to_csv("%s/score_cnt.csv"%(out_data_path),index=False)
address_describe.reset_index()[['address','all_score']].to_csv("%s/Donations_score_rule2.csv"%(out_data_path),index=False)
