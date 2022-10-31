import pandas as pd
import numpy as np
def get_max_amount(x_or):

    if len(x_or) > 0:
        while None in x_or:
            x_or.remove(None)
            
        return max(x_or)
    else:
        return np.nan
        
def get_hash_contri_rule(df):
    if df['add_cnt'] >= 4:
        return 2
    elif df['add_cnt'] > 2:
        return 1
    else:
        return 0
    
def get_amount_cnt_rule(df):

    if df['amount_cnt'] == 1:
        return 2
    elif df['amount_cnt'] < 3:
        return 1
    else:
        return 0
    

def get_amount_max_rule(df):

    if df['max_amount'] <= 0.001:
        return 2
    elif df['max_amount'] <0.099:
        return 1
    else:
        return 0
    

def get_contr_ratio_rule(df):

    if df['ratio'] > 0.4:
        return 2
    elif df['ratio'] > 0.29:
        return 1
    else:
        return 0
        
tg_path = "../../data/"
out_path = "../../output/"
data1 = pd.read_csv("%s/batch_transfer_part1.csv"%(tg_path))
data2 = pd.read_csv("%s/batch_transfer_part2.csv"%(tg_path))
data = pd.concat([data1,data2],ignore_index=True)
all_batch = pd.read_csv("%s/luabase.csv"%(tg_path))
all_batch = all_batch.rename(columns={'from_address':'token_txn_from','address_cnt':'batch_add_cnt','transaction_hash':'hash'})
all_batch = all_batch[['hash','batch_add_cnt']]
data_new = data.merge(all_batch,how='left',on=['hash'])
data_new = data_new[data_new['batch_add_cnt'].notnull()]
data_new = data_new.drop_duplicates(['hash'])
data_new['amount_list_new'] = data_new['amount_list'].map(lambda x:eval(x))
data_new['max_amount'] = data_new['amount_list_new'].map(lambda x:get_max_amount(x))
data_new['ratio'] = data_new['add_cnt'] / data_new['batch_add_cnt']
data_new = data_new.sort_values(by=['ratio','add_cnt'],ascending=False)
data_new['hash_contri_rule'] = data_new.apply(get_hash_contri_rule,axis=1)
data_new['amount_cnt_rule'] = data_new.apply(get_amount_cnt_rule,axis=1)
data_new['amount_max_rule'] = data_new.apply(get_amount_max_rule,axis=1)
data_new['contr_ratio_rule'] = data_new.apply(get_contr_ratio_rule,axis=1)

def change_name(col,names):
    if col in list(names.keys()):
        return names[col]
    else:
        return col
save_cols = ['hash','add_cnt','amount_cnt','max_amount','ratio',
 'hash_contri_rule',
 'amount_cnt_rule',
 'amount_max_rule',
 'contr_ratio_rule'
]
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
save_cols2 = [change_name(col,names) for col in save_cols]
data_new = data_new.rename(columns=names)
print('step2 data cols',data_new.columns.tolist())
data_new[save_cols2].to_csv("%s/batch_transfer_contract_info.csv"%(tg_path),index=False)
