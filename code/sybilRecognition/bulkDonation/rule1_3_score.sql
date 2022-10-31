---------------------------------------------------------------------------------
--3.bulkDonate Score
---------------------------------------------------------------------------------

---------------------------------------------------------------------------------
--3.1.bulkDonate rule1 Score - trans
---------------------------------------------------------------------------------
drop table if EXISTS safe_tmp.leo_temp_rule_var_score;
CREATE TABLE safe_tmp.leo_temp_rule_var_score ON CLUSTER default ENGINE= MergeTree()  ORDER BY group_grant_id
as
select
	txn_id
	,txn_hash
	,user_id
	,address
	,group_grant_id
	,grant_cnt
	,group_chain
	,group_network
	,group_token
	,group_amount_in_usdt
	,current_time

	,group_id_pre
	,address_cnt_group_pre
	,pct_address_cnt_group_pre
	,next_time_group_pre
	,time_gap_group_pre
	,time_p_75_group_pre
	,address_cnt_group_pre_01
	,address_cnt_group_pre_05
	,address_cnt_group_pre_10
	,address_cnt_group_pre_20
	,address_cnt_group_pre_25
	,address_cnt_group_pre_30
	,address_cnt_group_pre_40
	,address_cnt_group_pre_50
	,address_cnt_group_pre_60
	,address_cnt_group_pre_70
	,address_cnt_group_pre_75
	,address_cnt_group_pre_80
	,address_cnt_group_pre_90
	,address_cnt_group_pre_95
	,address_cnt_group_pre_99

	,group_id_hour_0_5
	,address_cnt_group_0_5
	,pct_address_cnt_group_0_5
	,next_time_group_0_5
	,time_gap_group_0_5
	,time_p_75_group_0_5
	,time_cov_group_0_5
	,address_cnt_group_0_5_01
	,address_cnt_group_0_5_05
	,address_cnt_group_0_5_10
	,address_cnt_group_0_5_20
	,address_cnt_group_0_5_25
	,address_cnt_group_0_5_30
	,address_cnt_group_0_5_40
	,address_cnt_group_0_5_50
	,address_cnt_group_0_5_60
	,address_cnt_group_0_5_70
	,address_cnt_group_0_5_75
	,address_cnt_group_0_5_80
	,address_cnt_group_0_5_90
	,address_cnt_group_0_5_95
	,address_cnt_group_0_5_99
	,time_p_75_group_0_5_01
	,time_p_75_group_0_5_05
	,time_p_75_group_0_5_10
	,time_p_75_group_0_5_20
	,time_p_75_group_0_5_25
	,time_p_75_group_0_5_30
	,time_p_75_group_0_5_40
	,time_p_75_group_0_5_50
	,time_p_75_group_0_5_60
	,time_p_75_group_0_5_70
	,time_p_75_group_0_5_75
	,time_p_75_group_0_5_80
	,time_p_75_group_0_5_90
	,time_p_75_group_0_5_95
	,time_p_75_group_0_5_99
	,time_cov_group_0_5_01
	,time_cov_group_0_5_05
	,time_cov_group_0_5_10
	,time_cov_group_0_5_20
	,time_cov_group_0_5_25
	,time_cov_group_0_5_30
	,time_cov_group_0_5_40
	,time_cov_group_0_5_50
	,time_cov_group_0_5_60
	,time_cov_group_0_5_70
	,time_cov_group_0_5_75
	,time_cov_group_0_5_80
	,time_cov_group_0_5_90
	,time_cov_group_0_5_95
	,time_cov_group_0_5_99

	,group_amount_in_usdt_01	
	,group_amount_in_usdt_05	
	,group_amount_in_usdt_10	
	,group_amount_in_usdt_20	
	,group_amount_in_usdt_25	
	,group_amount_in_usdt_30	
	,group_amount_in_usdt_40	
	,group_amount_in_usdt_50	
	,group_amount_in_usdt_60	
	,group_amount_in_usdt_70	
	,group_amount_in_usdt_75	
	,group_amount_in_usdt_80	
	,group_amount_in_usdt_90	
	,group_amount_in_usdt_95	
	,group_amount_in_usdt_99

	,amount_score
	,pre_cnt_score
	,pre_pct_score
	,05_cnt_score
	,05_time_75_score
	,05_pct_score
	,05_cov_score
	,amount_score + pre_cnt_score + pre_pct_score + 05_cnt_score
	+ 05_time_75_score + 05_pct_score + 05_cov_score as score
from
(
	select
		txn_id
		,txn_hash
		,user_id
		,address
		,group_grant_id
		,grant_cnt
		,group_chain
		,group_network
		,group_token
		,group_amount_in_usdt
		,current_time
	
		,group_id_pre
		,address_cnt_group_pre
		,pct_address_cnt_group_pre
		,next_time_group_pre
		,time_gap_group_pre
		,time_p_75_group_pre
		,address_cnt_group_pre_01
		,address_cnt_group_pre_05
		,address_cnt_group_pre_10
		,address_cnt_group_pre_20
		,address_cnt_group_pre_25
		,address_cnt_group_pre_30
		,address_cnt_group_pre_40
		,address_cnt_group_pre_50
		,address_cnt_group_pre_60
		,address_cnt_group_pre_70
		,address_cnt_group_pre_75
		,address_cnt_group_pre_80
		,address_cnt_group_pre_90
		,address_cnt_group_pre_95
		,address_cnt_group_pre_99
	
	    ,group_id_hour_0_5
		,address_cnt_group_0_5
		,pct_address_cnt_group_0_5
		,next_time_group_0_5
		,time_gap_group_0_5
		,time_p_75_group_0_5
		,time_cov_group_0_5
		,address_cnt_group_0_5_01
		,address_cnt_group_0_5_05
		,address_cnt_group_0_5_10
		,address_cnt_group_0_5_20
		,address_cnt_group_0_5_25
		,address_cnt_group_0_5_30
		,address_cnt_group_0_5_40
		,address_cnt_group_0_5_50
		,address_cnt_group_0_5_60
		,address_cnt_group_0_5_70
		,address_cnt_group_0_5_75
		,address_cnt_group_0_5_80
		,address_cnt_group_0_5_90
		,address_cnt_group_0_5_95
		,address_cnt_group_0_5_99
		,time_p_75_group_0_5_01
		,time_p_75_group_0_5_05
		,time_p_75_group_0_5_10
		,time_p_75_group_0_5_20
		,time_p_75_group_0_5_25
		,time_p_75_group_0_5_30
		,time_p_75_group_0_5_40
		,time_p_75_group_0_5_50
		,time_p_75_group_0_5_60
		,time_p_75_group_0_5_70
		,time_p_75_group_0_5_75
		,time_p_75_group_0_5_80
		,time_p_75_group_0_5_90
		,time_p_75_group_0_5_95
		,time_p_75_group_0_5_99
		,time_cov_group_0_5_01
		,time_cov_group_0_5_05
		,time_cov_group_0_5_10
		,time_cov_group_0_5_20
		,time_cov_group_0_5_25
		,time_cov_group_0_5_30
		,time_cov_group_0_5_40
		,time_cov_group_0_5_50
		,time_cov_group_0_5_60
		,time_cov_group_0_5_70
		,time_cov_group_0_5_75
		,time_cov_group_0_5_80
		,time_cov_group_0_5_90
		,time_cov_group_0_5_95
		,time_cov_group_0_5_99
	
		,group_amount_in_usdt_01	
		,group_amount_in_usdt_05	
		,group_amount_in_usdt_10	
		,group_amount_in_usdt_20	
		,group_amount_in_usdt_25	
		,group_amount_in_usdt_30	
		,group_amount_in_usdt_40	
		,group_amount_in_usdt_50	
		,group_amount_in_usdt_60	
		,group_amount_in_usdt_70	
		,group_amount_in_usdt_75	
		,group_amount_in_usdt_80	
		,group_amount_in_usdt_90	
		,group_amount_in_usdt_95	
		,group_amount_in_usdt_99
	
		,case
			when group_amount_in_usdt < toDecimal128(1,4) then 5
			when group_amount_in_usdt > toDecimal128(1,4) and group_amount_in_usdt <= toDecimal128(1.1,4) then 4
			when group_amount_in_usdt = toDecimal128(1,4) then 3
			when group_amount_in_usdt > toDecimal128(1.1,4) and group_amount_in_usdt <= toDecimal128(1.3,4) then 2
			else 1
		end as amount_score
		,case
			when address_cnt_group_pre >= address_cnt_group_pre_90 then 5
			when address_cnt_group_pre >= address_cnt_group_pre_70 then 4
			when address_cnt_group_pre >= address_cnt_group_pre_50 then 3
			when address_cnt_group_pre >= address_cnt_group_pre_25 then 2
			else 1
		end as pre_cnt_score
		,case
			when pct_address_cnt_group_pre >= pct_address_cnt_group_pre_90 then 5
			when pct_address_cnt_group_pre >= pct_address_cnt_group_pre_70 then 4
			when pct_address_cnt_group_pre >= pct_address_cnt_group_pre_50 then 3
			when pct_address_cnt_group_pre >= pct_address_cnt_group_pre_25 then 2
			else 1
		end as pre_pct_score
		,case
			when address_cnt_group_0_5 >= address_cnt_group_0_5_90 then 5
			when address_cnt_group_0_5 >= address_cnt_group_0_5_70 then 4
			when address_cnt_group_0_5 >= address_cnt_group_0_5_50 then 3
			when address_cnt_group_0_5 >= address_cnt_group_0_5_25 then 2
			else 1
		end as 05_cnt_score
		,case
			when time_p_75_group_0_5 <= time_p_75_group_0_5_10 then 5
			when time_p_75_group_0_5 <= time_p_75_group_0_5_30 then 4
			when time_p_75_group_0_5 <= time_p_75_group_0_5_50 then 3
			when time_p_75_group_0_5 <= time_p_75_group_0_5_75 then 2
			else 1
		end as 05_time_75_score
		,case
			when pct_address_cnt_group_0_5 >= pct_address_cnt_group_0_5_90 then 5
			when pct_address_cnt_group_0_5 >= pct_address_cnt_group_0_5_70 then 4
			when pct_address_cnt_group_0_5 >= pct_address_cnt_group_0_5_50 then 3
			when pct_address_cnt_group_0_5 >= pct_address_cnt_group_0_5_25 then 2
			else 1
		end as 05_pct_score
		,case
			when time_cov_group_0_5 <= time_cov_group_0_5_10 then 5
			when time_cov_group_0_5 <= time_cov_group_0_5_30 then 4
			when time_cov_group_0_5 <= time_cov_group_0_5_50 then 3
			when time_cov_group_0_5 <= time_cov_group_0_5_75 then 2
			else 1
		end as 05_cov_score
	from
		safe_tmp.leo_temp_rule_var1
)
;
--103841

---------------------------------------------------------------------------------
--3.2.bulkDonate rule1 Score - address
---------------------------------------------------------------------------------
drop table if EXISTS safe_tmp.leo_temp_rule_var_score_address;
CREATE TABLE safe_tmp.leo_temp_rule_var_score_address ON CLUSTER default ENGINE= MergeTree()  ORDER BY address
as
select
	address
		,max(score) as address_max_score
	from
		safe_tmp.leo_temp_rule_var_score
	group by
		address
;
--24525

---------------------------------------------------------------------------------
--3.3.join trans score and address score of rule1
---------------------------------------------------------------------------------
drop table if EXISTS safe_tmp.leo_temp_rule_var_score_final;
CREATE TABLE safe_tmp.leo_temp_rule_var_score_final ON CLUSTER default ENGINE= MergeTree()  ORDER BY group_grant_id
as
select
	a.txn_id as txn_id
	,a.txn_hash as txn_hash
	,a.user_id as user_id
	,a.address as address
	,a.group_grant_id as group_grant_id
	,a.grant_cnt as grant_cnt
	,a.group_chain as group_chain
	,a.group_network as group_network
	,a.group_token as group_token
	,a.group_amount_in_usdt as group_amount_in_usdt
	,a.current_time as current_time

	,a.group_id_pre as group_id_pre
	,a.address_cnt_group_pre as address_cnt_group_pre
	,a.pct_address_cnt_group_pre as pct_address_cnt_group_pre
	,a.next_time_group_pre as next_time_group_pre
	,a.time_gap_group_pre as time_gap_group_pre
	,a.time_p_75_group_pre as time_p_75_group_pre
	,a.address_cnt_group_pre_01 as address_cnt_group_pre_01
	,a.address_cnt_group_pre_05 as address_cnt_group_pre_05
	,a.address_cnt_group_pre_10 as address_cnt_group_pre_10
	,a.address_cnt_group_pre_20 as address_cnt_group_pre_20
	,a.address_cnt_group_pre_25 as address_cnt_group_pre_25
	,a.address_cnt_group_pre_30 as address_cnt_group_pre_30
	,a.address_cnt_group_pre_40 as address_cnt_group_pre_40
	,a.address_cnt_group_pre_50 as address_cnt_group_pre_50
	,a.address_cnt_group_pre_60 as address_cnt_group_pre_60
	,a.address_cnt_group_pre_70 as address_cnt_group_pre_70
	,a.address_cnt_group_pre_75 as address_cnt_group_pre_75
	,a.address_cnt_group_pre_80 as address_cnt_group_pre_80
	,a.address_cnt_group_pre_90 as address_cnt_group_pre_90
	,a.address_cnt_group_pre_95 as address_cnt_group_pre_95
	,a.address_cnt_group_pre_99 as address_cnt_group_pre_99

	,a.group_id_hour_0_5 as group_id_hour_0_5
	,a.address_cnt_group_0_5 as address_cnt_group_0_5
	,a.pct_address_cnt_group_0_5 as pct_address_cnt_group_0_5
	,a.next_time_group_0_5 as next_time_group_0_5
	,a.time_gap_group_0_5 as time_gap_group_0_5
	,a.time_p_75_group_0_5 as time_p_75_group_0_5
	,a.time_cov_group_0_5 as time_cov_group_0_5
	,a.address_cnt_group_0_5_01 as address_cnt_group_0_5_01
	,a.address_cnt_group_0_5_05 as address_cnt_group_0_5_05
	,a.address_cnt_group_0_5_10 as address_cnt_group_0_5_10
	,a.address_cnt_group_0_5_20 as address_cnt_group_0_5_20
	,a.address_cnt_group_0_5_25 as address_cnt_group_0_5_25
	,a.address_cnt_group_0_5_30 as address_cnt_group_0_5_30
	,a.address_cnt_group_0_5_40 as address_cnt_group_0_5_40
	,a.address_cnt_group_0_5_50 as address_cnt_group_0_5_50
	,a.address_cnt_group_0_5_60 as address_cnt_group_0_5_60
	,a.address_cnt_group_0_5_70 as address_cnt_group_0_5_70
	,a.address_cnt_group_0_5_75 as address_cnt_group_0_5_75
	,a.address_cnt_group_0_5_80 as address_cnt_group_0_5_80
	,a.address_cnt_group_0_5_90 as address_cnt_group_0_5_90
	,a.address_cnt_group_0_5_95 as address_cnt_group_0_5_95
	,a.address_cnt_group_0_5_99 as address_cnt_group_0_5_99
	,a.time_p_75_group_0_5_01 as time_p_75_group_0_5_01
	,a.time_p_75_group_0_5_05 as time_p_75_group_0_5_05
	,a.time_p_75_group_0_5_10 as time_p_75_group_0_5_10
	,a.time_p_75_group_0_5_20 as time_p_75_group_0_5_20
	,a.time_p_75_group_0_5_25 as time_p_75_group_0_5_25
	,a.time_p_75_group_0_5_30 as time_p_75_group_0_5_30
	,a.time_p_75_group_0_5_40 as time_p_75_group_0_5_40
	,a.time_p_75_group_0_5_50 as time_p_75_group_0_5_50
	,a.time_p_75_group_0_5_60 as time_p_75_group_0_5_60
	,a.time_p_75_group_0_5_70 as time_p_75_group_0_5_70
	,a.time_p_75_group_0_5_75 as time_p_75_group_0_5_75
	,a.time_p_75_group_0_5_80 as time_p_75_group_0_5_80
	,a.time_p_75_group_0_5_90 as time_p_75_group_0_5_90
	,a.time_p_75_group_0_5_95 as time_p_75_group_0_5_95
	,a.time_p_75_group_0_5_99 as time_p_75_group_0_5_99
	,a.time_cov_group_0_5_01 as time_cov_group_0_5_01
	,a.time_cov_group_0_5_05 as time_cov_group_0_5_05
	,a.time_cov_group_0_5_10 as time_cov_group_0_5_10
	,a.time_cov_group_0_5_20 as time_cov_group_0_5_20
	,a.time_cov_group_0_5_25 as time_cov_group_0_5_25
	,a.time_cov_group_0_5_30 as time_cov_group_0_5_30
	,a.time_cov_group_0_5_40 as time_cov_group_0_5_40
	,a.time_cov_group_0_5_50 as time_cov_group_0_5_50
	,a.time_cov_group_0_5_60 as time_cov_group_0_5_60
	,a.time_cov_group_0_5_70 as time_cov_group_0_5_70
	,a.time_cov_group_0_5_75 as time_cov_group_0_5_75
	,a.time_cov_group_0_5_80 as time_cov_group_0_5_80
	,a.time_cov_group_0_5_90 as time_cov_group_0_5_90
	,a.time_cov_group_0_5_95 as time_cov_group_0_5_95
	,a.time_cov_group_0_5_99 as time_cov_group_0_5_99

	,a.group_amount_in_usdt_01 as group_amount_in_usdt_01
	,a.group_amount_in_usdt_05 as group_amount_in_usdt_05
	,a.group_amount_in_usdt_10 as group_amount_in_usdt_10
	,a.group_amount_in_usdt_20 as group_amount_in_usdt_20
	,a.group_amount_in_usdt_25 as group_amount_in_usdt_25
	,a.group_amount_in_usdt_30 as group_amount_in_usdt_30
	,a.group_amount_in_usdt_40 as group_amount_in_usdt_40
	,a.group_amount_in_usdt_50 as group_amount_in_usdt_50
	,a.group_amount_in_usdt_60 as group_amount_in_usdt_60
	,a.group_amount_in_usdt_70 as group_amount_in_usdt_70
	,a.group_amount_in_usdt_75 as group_amount_in_usdt_75
	,a.group_amount_in_usdt_80 as group_amount_in_usdt_80
	,a.group_amount_in_usdt_90 as group_amount_in_usdt_90
	,a.group_amount_in_usdt_95 as group_amount_in_usdt_95
	,a.group_amount_in_usdt_99 as group_amount_in_usdt_99

	,a.amount_score as amount_score
	,a.pre_cnt_score as pre_cnt_score
	,a.pre_pct_score as pre_pct_score
	,a.05_cnt_score as 05_cnt_score
	,a.05_time_75_score as 05_time_75_score
	,a.05_pct_score as 05_pct_score
	,a.05_cov_score as 05_cov_score
	,a.score as trans_score
	,b.address_max_score as address_max_score
from
(
	select
		*
	from
		safe_tmp.leo_temp_rule_var_score
) a
join
(
	select
		*
	from
		safe_tmp.leo_temp_rule_var_score_address
) b
on
	a.address = b.address
;
--103841

	