import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

names = ['address','group_id','time_index','cnt']
eth_df = pd.read_csv("../../data/heat_map.csv",sep="|",names=names)
eth_df = eth_df[eth_df['group_id'] <= 30]
max_eth_index = eth_df['time_index'].max()
eth_addr_count = len(eth_df['address'].unique())
max_index = max_eth_index
hf = np.zeros((eth_addr_count, max_index + 1))
 
row_index = -1
last_address = None
address_row_index_map = {}
for index, row in eth_df.iterrows():
    address = row['address']
    time_index = int(row['time_index'])
    cnt = int(row['cnt'])
    if address != last_address:
        row_index = row_index + 1
        last_address = address
        address_row_index_map[address] = row_index

    hf[row_index][time_index] = 1



addr_list_new = []
addr_list = []
row_index = -1
last_address = None
for index, row in eth_df.iterrows():
    address = row['address']
    if address != last_address:
        addr_list.append('')
        addr_list_new.append(address)
        last_address = address
 
 
from datetime import datetime
ts_list = []
ts_list_new = []
for i in range(max_index + 1):
    ts = 1662480000 + 21600 * i
    ts_str = datetime.fromtimestamp(ts).strftime('%Y%m%d%H')
    ts_list.append(ts_str)
    ts_list_new.append("")

new_df = pd.DataFrame(hf, 
                      index=addr_list, columns=ts_list_new
                     )



color = plt.get_cmap('YlGnBu')
f, ax = plt.subplots(figsize = (10, 10))
heat_map = sns.heatmap(data=new_df, 
                       cmap=color, 
                       ax=ax)
ax.set_title('Action Heatmap')
ax.set_ylabel('Addresses', fontsize = 15)
ax.set_xlabel('time', fontsize = 15)
f.savefig('../../output/behavior_heat_map.png', dpi=300, bbox_inches='tight')