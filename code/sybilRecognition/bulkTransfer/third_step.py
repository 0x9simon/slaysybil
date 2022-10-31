import pandas as pd
def get_col(col):
    if '.' in col:
        return col.split(".")[-1]
    else:
        return col
        
def get_is_bull(x):
    if '0x7d655c57f71464b6f83811c55d84009cd9f5221c' in str(x):
        return 1
    else:
        return 0
    
def get_is_zksync(x):
    if '0xabea9132b05a70803a4e85094fd0e1800777fbef' in str(x):
        return 1
    else:
        return 0
  
def get_bull_rule(df):
    if df['is_bulk'] == 1:
        return -2
    else:
        return 0
    
    
def get_zksync_rule(df):
    if df['is_zksync'] == 1:
        return -2
    else:
        return 0
    
def get_gap_day_rule(df):
    if df['gap_day'] < 0:
        return 0
    elif df['gap_day'] == 0:
        return 2
    elif df['gap_day'] < 28:
        return 1
    else:
        return 0
        
def get_contribute_ratio_amount(df):
    if df['contribute_ratio'] >= 0.972430:
        return 2
    elif df['contribute_ratio'] >= 0.576923:
        return 1
    else:
        return 0

cols_save = [
'address', 
 'max_hash_contri_rule',
 'max_amount_cnt_rule',
 'max_amount_max_rule',
 'max_contr_ratio_rule',
 'bulk_rule',
 'zksync_rule'
 'gap_day_rule',
 'contribute_ratio_amount_rule',
 'contribute_ratio_rule',
 'dai_amount',
 'eth_amount',
 'gap_day',
 'max_add_cnt',
 'max_amount_cnt',
 'max_contribute_dt',
 'max_max_amount',
 'max_prepare_dt',
 'max_ratio',
 'sum_contribute_amount',
 'tranfer_all_amount',
 'usdc_amount',
 'usdt_amount'
]

rule_cols = [ 'max_hash_contri_rule',
 'max_amount_cnt_rule',
 'max_amount_max_rule',
 'max_contr_ratio_rule',
 'bulk_rule',
 'zksync_rule',
 'gap_day_rule',
 'contribute_ratio_amount_rule']
 
 
tg_path = "../../data/"
out_path = "../../output"
names = {'add_cnt': 'NumOfContr',
 'ratio': 'ContrRatio',
 'contribute_ratio': 'ContrAmountRatio',
 'amount_cnt': 'NumOfDistinctAmount',
 'max_amount': 'MaxAmount',
 'gap_day': 'GapDay',
'hash_contri_rule':'Hash_contribution_rule',
'amount_cnt_rule':'Amount_cnt_rule',
'amount_max_rule':'Amount_max_rule',
'contr_ratio_rule':'Contr_ratio_rule'
}
data = pd.read_csv("%s/batch_transfer_rule_data_final.csv"%(tg_path))

batch_contri_fromlist = pd.read_csv("%s/batch_contri_fromlist.csv"%(tg_path))
# data = data.iloc[1:,:]
# cols = data.columns.tolist()


# cols = [get_col(col) for col in cols]
# print("step3 data col",cols)
# data.columns = cols
data['max_contribute_dt'] = pd.to_datetime(data['max_contribute_dt'])
data['max_prepare_dt'] = pd.to_datetime(data['max_prepare_dt'])
data['gap_day'] = (data['max_contribute_dt'] - data['max_prepare_dt']).dt.days

batch_contri_fromlist = batch_contri_fromlist.iloc[1:,:]
data['gap_day_rule'] = data.apply(get_gap_day_rule,axis=1)


data['contribute_ratio_amount_rule'] = data.apply(get_contribute_ratio_amount,axis=1)


batch_contri_fromlist['is_bulk'] = batch_contri_fromlist['add_list'].map(lambda x:get_is_bull(x))
batch_contri_fromlist['is_zksync'] = batch_contri_fromlist['add_list'].map(lambda x:get_is_zksync(x))


batch_contri_fromlist['bulk_rule'] = batch_contri_fromlist.apply(get_bull_rule,axis=1)
batch_contri_fromlist['zksync_rule'] = batch_contri_fromlist.apply(get_zksync_rule,axis=1)

tg_cols = ['address','bulk_rule','zksync_rule']
batch_contri_fromlist = batch_contri_fromlist.rename(columns={'token_txn_to':'address'})
data = data.merge(batch_contri_fromlist[tg_cols],how='left',on=['address'])


data = data.rename(columns={'contribute_ratio':'contribute_ratio_rule'})
data['all_rule_score'] = data[rule_cols].fillna(0).sum(axis=1)
data[rule_cols] = data[rule_cols].astype(float)
data['all_rule_score'] = data[rule_cols].fillna(0).sum(axis=1)

save_cols = ['NumOfContr', 'ContrRatio', 'ContrAmountRatio', 'NumOfDistinctAmount', 'MaxAmount', 'GapDay', 'Hash_contribution_rule', 'Amount_cnt_rule', 'Amount_max_rule', 'Contr_ratio_rule']
# data[['address']+rule_cols+['all_rule_score']].to_csv("batch_rule.csv",index=False)
address_desc = data.groupby(['all_rule_score']).agg({'address':'count'}).reset_index()
address_desc = address_desc.rename(columns={'address':'address_cnt'})

data[['address','all_rule_score']].to_csv("%s/Bulk_Transfers_Score.csv"%(out_path),index=False)
