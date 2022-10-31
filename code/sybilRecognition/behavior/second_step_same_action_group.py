import pandas as pd
import numpy as  np

def get_time_split(x):
    if len(str(x).split(",")) == 0:
        return str([])
    else:
        return str([dt[:10]for dt in str(x).split(",")])
    

def get_amount_split(x):
    if len(str(x).split(",")) == 0:
        return str([])
    else:
        return str([np.round(float(dt),6)for dt in str(x).split(",")])
    





def get_group(data_all_chain,tg_action_col,mark):
    eth_group = data_all_chain.groupby(tg_action_col).agg({'from_addr':'nunique'})
    eth_group = eth_group.sort_values(by=['from_addr'],ascending=False)
    eth_group = eth_group.rename(columns={'from_addr':'addr_cnt'})
    eth_group['group'] = range(len(eth_group))
    eth_group = eth_group.add_suffix("_%s"%(mark))
    eth_group = eth_group.reset_index()
    return eth_group

output_path = "../../output/"
tg_path = "../../data/data_all_chain_action.csv"
data_all_chain = pd.read_csv(tg_path)
tg_all_col = [ 'to_list_eth', 'fun_list_eth','to_list_zksync', 'fun_list_zksync','eth_time_new','zk_time_new'
              ,'amount_list_zksync','amount_list_eth'
             ]

data_all_chain['to_list_eth'] = data_all_chain['to_list_eth'].fillna("")
data_all_chain['fun_list_eth'] = data_all_chain['fun_list_eth'].fillna("")
data_all_chain['to_list_zksync'] = data_all_chain['to_list_zksync'].fillna("")
data_all_chain['fun_list_zksync'] = data_all_chain['fun_list_zksync'].fillna("")

data_all_chain['amount_list_zksync'] = data_all_chain['amount_list_zksync'].map(lambda x:get_amount_split(x))
data_all_chain['amount_list_eth'] = data_all_chain['amount_list_eth'].map(lambda x:get_amount_split(x))


# data_all_chain_new = data_all_chain.copy()
data_all_chain['eth_time_new'] = data_all_chain['timelist_eth'].map(lambda x:get_time_split(x))
data_all_chain['zk_time_new'] = data_all_chain['timelist_zksync'].map(lambda x:get_time_split(x))


all_group_action = get_group(data_all_chain,tg_all_col,'_allchain')



# data_all_chain_new = data_all_chain.merge(eth_group_action,how='left',on=tg_eth_col)
# data_all_chain_new = data_all_chain_new.merge(zk_group_action,how='left',on=tg_zk_col)
data_all_chain_new = data_all_chain.merge(all_group_action,how='left',on=tg_all_col)

data_all_chain_new['eth_action_cnt'] = data_all_chain_new['fun_list_eth'].map(lambda x:len(str(x).split(",")))
data_all_chain_new['zk_action_cnt'] = data_all_chain_new['fun_list_zksync'].map(lambda x:len(str(x).split(",")))
data_all_chain_new['action_cnt_all'] = data_all_chain_new[['eth_action_cnt','zk_action_cnt']].sum(axis=1)





data_all_chain_new = data_all_chain_new[data_all_chain_new['action_cnt_all'] >= 3]
cluster_df = data_all_chain_new[['from_addr','group__allchain']].drop_duplicates()


group_cnt = data_all_chain_new.groupby(['group__allchain']).agg({'from_addr':'nunique'})
group_cnt = group_cnt.rename(columns={'from_addr':'addr_cnt'}).reset_index()
group_cnt = group_cnt.sort_values(by=['addr_cnt','group__allchain'],ascending=False)

group_list = group_cnt[group_cnt['group__allchain'] >= 5]['group__allchain'].tolist()
group_df = pd.DataFrame()
group_df['group__allchain'] = group_list
group_list = group_cnt[group_cnt['addr_cnt'] >= 5]['group__allchain'].tolist()

cluster_df1 = pd.DataFrame()
cluster_df1['address'] = cluster_df[cluster_df['group__allchain'].isin(group_list)]['from_addr'].tolist()
cluster_df1['score'] = np.nan
cluster_df1['level'] = 'high'
cluster_df1.to_csv("%s/behavior_score.csv"%(output_path),index=False)



