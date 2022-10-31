---------------------------------------------------------------------------------------
--typical case 1:grant 7207
---------------------------------------------------------------------------------------
select
	a.user_id as user_id
	,a.address as address
	,a.current_time as `time`
	,a.time_gap_group_pre as time_gap
	,a.group_chain as chain
	,a.group_token as token
	,a.group_amount_in_usdt as amount
	,b.group_id_hour_0_5 as group_id
	,b.address_cnt_group_0_5 as group_address_cnt
	,a.group_grant_id as grant_id
	,b.grant_cnt as grant_donate_cnt
	,a.txn_id as txn_id
	,a.txn_hash as txn_hash
	,a.group_network as network
	,b.amount_score as amount_score
	,b.pre_cnt_score as pre_cnt_score
	,b.pre_pct_score as pre_pct_score
	,b.05_cnt_score as 05_cnt_score
	,b.05_time_75_score as 05_time_75_score
	,b.05_pct_score as 05_pct_score
	,b.05_cov_score as 05_cov_score
	,b.trans_score as trans_score
	,b.address_max_score as address_max_score

--	,a.group_id_pre as group_id_pre
--	,a.address_cnt_group_pre as address_cnt_group_pre
--	,a.next_time_group_pre as next_time_group_pre
--	,b.pct_address_cnt_group_pre as pct_address_cnt_group_pre
--	,b.pct_address_cnt_group_0_5 as pct_address_cnt_group_0_5
from
(
	select
		txn_id
		,txn_hash
		,user_id
		,address
		,group_grant_id
		,group_chain
		,group_network
		,group_token
		,group_amount_in_usdt
		,current_time
		,group_id_pre
		,address_cnt_group_pre
		,address_cnt_group_0_5
		,next_time_group_pre
		,time_gap_group_pre
	from
		safe_tmp.leo_temp_rule_data_1
	where
		group_grant_id = '7207'
) a
left join
(
	select
		txn_hash
		,address
		,group_id_hour_0_5
		,pct_address_cnt_group_pre
		,address_cnt_group_0_5
		,pct_address_cnt_group_0_5
		,grant_cnt
		,amount_score
		,pre_cnt_score
		,pre_pct_score
		,05_cnt_score
		,05_time_75_score
		,05_pct_score
		,05_cov_score
		,trans_score
		,address_max_score
	from
		safe_tmp.leo_temp_rule_var_score_final
	where
		group_grant_id = '7207'
) b
on
	a.txn_hash = b.txn_hash and a.address = b.address
order by
	address_cnt_group_pre
	,group_grant_id
	,current_time
;