drop table if EXISTS safe_tmp.leo_temp_rule_var;
CREATE TABLE safe_tmp.leo_temp_rule_var ON CLUSTER default ENGINE= MergeTree()  ORDER BY group_grant_id
as
select
	a.txn_id as txn_id
	,a.txn_hash as txn_hash
	,a.user_id as user_id
	,a.address as address
	,a.group_grant_id as group_grant_id
	,b.grant_cnt as grant_cnt
	,a.group_chain as group_chain
	,a.group_network as group_network
	,a.group_token as group_token
	--var1
	,a.group_amount_in_usdt as group_amount_in_usdt
	,a.current_time as current_time

	,a.group_id_pre as group_id_pre
	--var2
	,a.address_cnt_group_pre as address_cnt_group_pre
	--var3
	,toDecimal128(a.address_cnt_group_pre,8)/toDecimal128(b.grant_cnt,8) as pct_address_cnt_group_pre
	,a.next_time_group_pre as next_time_group_pre
	,a.time_gap_group_pre as time_gap_group_pre
	,a.time_p_75_group_pre as time_p_75_group_pre

	,c.group_amount_in_usdt_01 as group_amount_in_usdt_01
	,c.group_amount_in_usdt_05 as group_amount_in_usdt_05
	,c.group_amount_in_usdt_10 as group_amount_in_usdt_10
	,c.group_amount_in_usdt_20 as group_amount_in_usdt_20
	,c.group_amount_in_usdt_25 as group_amount_in_usdt_25
	,c.group_amount_in_usdt_30 as group_amount_in_usdt_30
	,c.group_amount_in_usdt_40 as group_amount_in_usdt_40
	,c.group_amount_in_usdt_50 as group_amount_in_usdt_50
	,c.group_amount_in_usdt_60 as group_amount_in_usdt_60
	,c.group_amount_in_usdt_70 as group_amount_in_usdt_70
	,c.group_amount_in_usdt_75 as group_amount_in_usdt_75
	,c.group_amount_in_usdt_80 as group_amount_in_usdt_80
	,c.group_amount_in_usdt_90 as group_amount_in_usdt_90
	,c.group_amount_in_usdt_95 as group_amount_in_usdt_95
	,c.group_amount_in_usdt_99 as group_amount_in_usdt_99

	,c.address_cnt_group_pre_01 as address_cnt_group_pre_01
	,c.address_cnt_group_pre_05 as address_cnt_group_pre_05
	,c.address_cnt_group_pre_10 as address_cnt_group_pre_10
	,c.address_cnt_group_pre_20 as address_cnt_group_pre_20
	,c.address_cnt_group_pre_25 as address_cnt_group_pre_25
	,c.address_cnt_group_pre_30 as address_cnt_group_pre_30
	,c.address_cnt_group_pre_40 as address_cnt_group_pre_40
	,c.address_cnt_group_pre_50 as address_cnt_group_pre_50
	,c.address_cnt_group_pre_60 as address_cnt_group_pre_60
	,c.address_cnt_group_pre_70 as address_cnt_group_pre_70
	,c.address_cnt_group_pre_75 as address_cnt_group_pre_75
	,c.address_cnt_group_pre_80 as address_cnt_group_pre_80
	,c.address_cnt_group_pre_90 as address_cnt_group_pre_90
	,c.address_cnt_group_pre_95 as address_cnt_group_pre_95
	,c.address_cnt_group_pre_99 as address_cnt_group_pre_99

    ,a.group_id_hour_0_5 as group_id_hour_0_5
    --var4
	,a.address_cnt_group_0_5 as address_cnt_group_0_5
	--var5
	,toDecimal128(a.address_cnt_group_0_5,8)/toDecimal128(b.grant_cnt,8) as pct_address_cnt_group_0_5
	,a.next_time_group_0_5 as next_time_group_0_5
	,a.time_gap_group_0_5 as time_gap_group_0_5
	--var6
	,a.time_p_75_group_0_5 as time_p_75_group_0_5
	--var7
	,a.time_cov_group_0_5 as time_cov_group_0_5

	,c.address_cnt_group_0_5_01 as address_cnt_group_0_5_01 
	,c.address_cnt_group_0_5_05 as address_cnt_group_0_5_05 
	,c.address_cnt_group_0_5_10 as address_cnt_group_0_5_10 
	,c.address_cnt_group_0_5_20 as address_cnt_group_0_5_20 
	,c.address_cnt_group_0_5_25 as address_cnt_group_0_5_25 
	,c.address_cnt_group_0_5_30 as address_cnt_group_0_5_30
	,c.address_cnt_group_0_5_40 as address_cnt_group_0_5_40
	,c.address_cnt_group_0_5_50 as address_cnt_group_0_5_50
	,c.address_cnt_group_0_5_60 as address_cnt_group_0_5_60
	,c.address_cnt_group_0_5_70 as address_cnt_group_0_5_70
	,c.address_cnt_group_0_5_75 as address_cnt_group_0_5_75
	,c.address_cnt_group_0_5_80 as address_cnt_group_0_5_80
	,c.address_cnt_group_0_5_90 as address_cnt_group_0_5_90
	,c.address_cnt_group_0_5_95 as address_cnt_group_0_5_95
	,c.address_cnt_group_0_5_99 as address_cnt_group_0_5_99
	
	,c.time_p_75_group_0_5_01 as time_p_75_group_0_5_01
	,c.time_p_75_group_0_5_05 as time_p_75_group_0_5_05
	,c.time_p_75_group_0_5_10 as time_p_75_group_0_5_10
	,c.time_p_75_group_0_5_20 as time_p_75_group_0_5_20
	,c.time_p_75_group_0_5_25 as time_p_75_group_0_5_25
	,c.time_p_75_group_0_5_30 as time_p_75_group_0_5_30
	,c.time_p_75_group_0_5_40 as time_p_75_group_0_5_40
	,c.time_p_75_group_0_5_50 as time_p_75_group_0_5_50
	,c.time_p_75_group_0_5_60 as time_p_75_group_0_5_60
	,c.time_p_75_group_0_5_70 as time_p_75_group_0_5_70
	,c.time_p_75_group_0_5_75 as time_p_75_group_0_5_75
	,c.time_p_75_group_0_5_80 as time_p_75_group_0_5_80
	,c.time_p_75_group_0_5_90 as time_p_75_group_0_5_90
	,c.time_p_75_group_0_5_95 as time_p_75_group_0_5_95
	,c.time_p_75_group_0_5_99 as time_p_75_group_0_5_99

	,c.time_cov_group_0_5_01 as time_cov_group_0_5_01
	,c.time_cov_group_0_5_05 as time_cov_group_0_5_05
	,c.time_cov_group_0_5_10 as time_cov_group_0_5_10
	,c.time_cov_group_0_5_20 as time_cov_group_0_5_20
	,c.time_cov_group_0_5_25 as time_cov_group_0_5_25
	,c.time_cov_group_0_5_30 as time_cov_group_0_5_30
	,c.time_cov_group_0_5_40 as time_cov_group_0_5_40
	,c.time_cov_group_0_5_50 as time_cov_group_0_5_50
	,c.time_cov_group_0_5_60 as time_cov_group_0_5_60
	,c.time_cov_group_0_5_70 as time_cov_group_0_5_70
	,c.time_cov_group_0_5_75 as time_cov_group_0_5_75
	,c.time_cov_group_0_5_80 as time_cov_group_0_5_80
	,c.time_cov_group_0_5_90 as time_cov_group_0_5_90
	,c.time_cov_group_0_5_95 as time_cov_group_0_5_95
	,c.time_cov_group_0_5_99 as time_cov_group_0_5_99
from
(
	select
		1 as flag
		,txn_id
	    ,txn_hash
	    ,user_id
	    ,address
	    ,group_grant_id
	    ,group_chain
	    ,group_network
	    ,group_token
	    ,group_amount_in_usdt
	    ,current_time
	
	    ,group_id_pre  --预分组ID
	    ,address_cnt_group_pre  --预分组组内地址数
	    ,next_time_group_pre  --预分组组内当前捐赠下一笔的时间
	    ,time_gap_group_pre  --预分组内和下一笔的时间差
	    ,time_p_75_group_pre  --预分组时间差的75分位数
	
	    ,group_id_hour_0_5
	    ,address_cnt_group_0_5  --时间差半小时的分组组内地址数
	    ,next_time_group_0_5  --时间差半小时的分组组内当前捐赠下一笔的时间
	    ,time_gap_group_0_5  --时间差半小时的分组内和下一笔的时间差
	    ,time_p_75_group_0_5   --时间差半小时的分组时间差的75分位数
	    ,time_std_group_0_5  --时间差半小时的分组时间差的标准差
	    ,time_cov_group_0_5  --时间差半小时的分组时间差的变异系数
	from
	    safe_tmp.leo_temp_rule_data_1
	where
	    address_cnt_group_pre >= 5
	    and address_cnt_group_0_5 > 3
	    and group_amount_in_usdt <= toDecimal128(3,4)
) a
left join
(
	select
		grant_id
    	,toInt32(contributor_count) as grant_cnt
    from
    	gr15_hackathon.grants
) b
on
	a.group_grant_id = b.grant_id
left join
(
	select
		1 as flag
		,quantile(0.01)(group_amount_in_usdt) as group_amount_in_usdt_01
    	,quantile(0.05)(group_amount_in_usdt) as group_amount_in_usdt_05
    	,quantile(0.10)(group_amount_in_usdt) as group_amount_in_usdt_10
    	,quantile(0.20)(group_amount_in_usdt) as group_amount_in_usdt_20
    	,quantile(0.25)(group_amount_in_usdt) as group_amount_in_usdt_25
    	,quantile(0.30)(group_amount_in_usdt) as group_amount_in_usdt_30
    	,quantile(0.40)(group_amount_in_usdt) as group_amount_in_usdt_40
    	,quantile(0.50)(group_amount_in_usdt) as group_amount_in_usdt_50
    	,quantile(0.60)(group_amount_in_usdt) as group_amount_in_usdt_60
    	,quantile(0.70)(group_amount_in_usdt) as group_amount_in_usdt_70
    	,quantile(0.75)(group_amount_in_usdt) as group_amount_in_usdt_75
    	,quantile(0.80)(group_amount_in_usdt) as group_amount_in_usdt_80
    	,quantile(0.90)(group_amount_in_usdt) as group_amount_in_usdt_90
    	,quantile(0.95)(group_amount_in_usdt) as group_amount_in_usdt_95
    	,quantile(0.99)(group_amount_in_usdt) as group_amount_in_usdt_99

		,quantile(0.01)(address_cnt_group_pre) as address_cnt_group_pre_01
    	,quantile(0.05)(address_cnt_group_pre) as address_cnt_group_pre_05
    	,quantile(0.10)(address_cnt_group_pre) as address_cnt_group_pre_10
    	,quantile(0.20)(address_cnt_group_pre) as address_cnt_group_pre_20
    	,quantile(0.25)(address_cnt_group_pre) as address_cnt_group_pre_25
    	,quantile(0.30)(address_cnt_group_pre) as address_cnt_group_pre_30
    	,quantile(0.40)(address_cnt_group_pre) as address_cnt_group_pre_40
    	,quantile(0.50)(address_cnt_group_pre) as address_cnt_group_pre_50
    	,quantile(0.60)(address_cnt_group_pre) as address_cnt_group_pre_60
    	,quantile(0.70)(address_cnt_group_pre) as address_cnt_group_pre_70
    	,quantile(0.75)(address_cnt_group_pre) as address_cnt_group_pre_75
    	,quantile(0.80)(address_cnt_group_pre) as address_cnt_group_pre_80
    	,quantile(0.90)(address_cnt_group_pre) as address_cnt_group_pre_90
    	,quantile(0.95)(address_cnt_group_pre) as address_cnt_group_pre_95
    	,quantile(0.99)(address_cnt_group_pre) as address_cnt_group_pre_99

    	,quantile(0.01)(address_cnt_group_0_5) as address_cnt_group_0_5_01
    	,quantile(0.05)(address_cnt_group_0_5) as address_cnt_group_0_5_05
    	,quantile(0.10)(address_cnt_group_0_5) as address_cnt_group_0_5_10
    	,quantile(0.20)(address_cnt_group_0_5) as address_cnt_group_0_5_20
    	,quantile(0.25)(address_cnt_group_0_5) as address_cnt_group_0_5_25
    	,quantile(0.30)(address_cnt_group_0_5) as address_cnt_group_0_5_30
    	,quantile(0.40)(address_cnt_group_0_5) as address_cnt_group_0_5_40
    	,quantile(0.50)(address_cnt_group_0_5) as address_cnt_group_0_5_50
    	,quantile(0.60)(address_cnt_group_0_5) as address_cnt_group_0_5_60
    	,quantile(0.70)(address_cnt_group_0_5) as address_cnt_group_0_5_70
    	,quantile(0.75)(address_cnt_group_0_5) as address_cnt_group_0_5_75
    	,quantile(0.80)(address_cnt_group_0_5) as address_cnt_group_0_5_80
    	,quantile(0.90)(address_cnt_group_0_5) as address_cnt_group_0_5_90
    	,quantile(0.95)(address_cnt_group_0_5) as address_cnt_group_0_5_95
    	,quantile(0.99)(address_cnt_group_0_5) as address_cnt_group_0_5_99
	
    	,quantile(0.01)(time_p_75_group_0_5) as time_p_75_group_0_5_01
		,quantile(0.05)(time_p_75_group_0_5) as time_p_75_group_0_5_05
		,quantile(0.10)(time_p_75_group_0_5) as time_p_75_group_0_5_10
		,quantile(0.20)(time_p_75_group_0_5) as time_p_75_group_0_5_20
		,quantile(0.25)(time_p_75_group_0_5) as time_p_75_group_0_5_25
		,quantile(0.30)(time_p_75_group_0_5) as time_p_75_group_0_5_30
		,quantile(0.40)(time_p_75_group_0_5) as time_p_75_group_0_5_40
		,quantile(0.50)(time_p_75_group_0_5) as time_p_75_group_0_5_50
		,quantile(0.60)(time_p_75_group_0_5) as time_p_75_group_0_5_60
		,quantile(0.70)(time_p_75_group_0_5) as time_p_75_group_0_5_70
		,quantile(0.75)(time_p_75_group_0_5) as time_p_75_group_0_5_75
		,quantile(0.80)(time_p_75_group_0_5) as time_p_75_group_0_5_80
		,quantile(0.90)(time_p_75_group_0_5) as time_p_75_group_0_5_90
		,quantile(0.95)(time_p_75_group_0_5) as time_p_75_group_0_5_95
		,quantile(0.99)(time_p_75_group_0_5) as time_p_75_group_0_5_99

		,quantile(0.01)(time_cov_group_0_5) as time_cov_group_0_5_01
		,quantile(0.05)(time_cov_group_0_5) as time_cov_group_0_5_05
		,quantile(0.10)(time_cov_group_0_5) as time_cov_group_0_5_10
		,quantile(0.20)(time_cov_group_0_5) as time_cov_group_0_5_20
		,quantile(0.25)(time_cov_group_0_5) as time_cov_group_0_5_25
		,quantile(0.30)(time_cov_group_0_5) as time_cov_group_0_5_30
		,quantile(0.40)(time_cov_group_0_5) as time_cov_group_0_5_40
		,quantile(0.50)(time_cov_group_0_5) as time_cov_group_0_5_50
		,quantile(0.60)(time_cov_group_0_5) as time_cov_group_0_5_60
		,quantile(0.70)(time_cov_group_0_5) as time_cov_group_0_5_70
		,quantile(0.75)(time_cov_group_0_5) as time_cov_group_0_5_75
		,quantile(0.80)(time_cov_group_0_5) as time_cov_group_0_5_80
		,quantile(0.90)(time_cov_group_0_5) as time_cov_group_0_5_90
		,quantile(0.95)(time_cov_group_0_5) as time_cov_group_0_5_95
		,quantile(0.99)(time_cov_group_0_5) as time_cov_group_0_5_99
    from
	    safe_tmp.leo_temp_rule_data_1
	where
	    address_cnt_group_pre >= 5
	    and address_cnt_group_0_5 > 3
	    and group_amount_in_usdt <= toDecimal128(3,4)
	group by
		flag
) c
on a.flag = c.flag
;
--103841

drop table if EXISTS safe_tmp.leo_temp_rule_var1;
CREATE TABLE safe_tmp.leo_temp_rule_var1 ON CLUSTER default ENGINE= MergeTree()  ORDER BY group_grant_id
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
	--var1
	,a.group_amount_in_usdt as group_amount_in_usdt
	,a.current_time as current_time

	,a.group_id_pre as group_id_pre
	--var2
	,a.address_cnt_group_pre as address_cnt_group_pre
	--var3
	,a.pct_address_cnt_group_pre as pct_address_cnt_group_pre
	,a.next_time_group_pre as next_time_group_pre
	,a.time_gap_group_pre as time_gap_group_pre
	,a.time_p_75_group_pre as time_p_75_group_pre

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
    --var4
	,a.address_cnt_group_0_5 as address_cnt_group_0_5
	--var5
	,a.pct_address_cnt_group_0_5 as pct_address_cnt_group_0_5
	,a.next_time_group_0_5 as next_time_group_0_5
	,a.time_gap_group_0_5 as time_gap_group_0_5
	--var6
	,a.time_p_75_group_0_5 as time_p_75_group_0_5
	--var7
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

	,b.pct_address_cnt_group_pre_01 as pct_address_cnt_group_pre_01
	,b.pct_address_cnt_group_pre_05 as pct_address_cnt_group_pre_05
	,b.pct_address_cnt_group_pre_10 as pct_address_cnt_group_pre_10
	,b.pct_address_cnt_group_pre_20 as pct_address_cnt_group_pre_20
	,b.pct_address_cnt_group_pre_25 as pct_address_cnt_group_pre_25
	,b.pct_address_cnt_group_pre_30 as pct_address_cnt_group_pre_30
	,b.pct_address_cnt_group_pre_40 as pct_address_cnt_group_pre_40
	,b.pct_address_cnt_group_pre_50 as pct_address_cnt_group_pre_50
	,b.pct_address_cnt_group_pre_60 as pct_address_cnt_group_pre_60
	,b.pct_address_cnt_group_pre_70 as pct_address_cnt_group_pre_70
	,b.pct_address_cnt_group_pre_75 as pct_address_cnt_group_pre_75
	,b.pct_address_cnt_group_pre_80 as pct_address_cnt_group_pre_80
	,b.pct_address_cnt_group_pre_90 as pct_address_cnt_group_pre_90
	,b.pct_address_cnt_group_pre_95 as pct_address_cnt_group_pre_95
	,b.pct_address_cnt_group_pre_99 as pct_address_cnt_group_pre_99

	,b.pct_address_cnt_group_0_5_01 as pct_address_cnt_group_0_5_01
	,b.pct_address_cnt_group_0_5_05 as pct_address_cnt_group_0_5_05
	,b.pct_address_cnt_group_0_5_10 as pct_address_cnt_group_0_5_10
	,b.pct_address_cnt_group_0_5_20 as pct_address_cnt_group_0_5_20
	,b.pct_address_cnt_group_0_5_25 as pct_address_cnt_group_0_5_25
	,b.pct_address_cnt_group_0_5_30 as pct_address_cnt_group_0_5_30
	,b.pct_address_cnt_group_0_5_40 as pct_address_cnt_group_0_5_40
	,b.pct_address_cnt_group_0_5_50 as pct_address_cnt_group_0_5_50
	,b.pct_address_cnt_group_0_5_60 as pct_address_cnt_group_0_5_60
	,b.pct_address_cnt_group_0_5_70 as pct_address_cnt_group_0_5_70
	,b.pct_address_cnt_group_0_5_75 as pct_address_cnt_group_0_5_75
	,b.pct_address_cnt_group_0_5_80 as pct_address_cnt_group_0_5_80
	,b.pct_address_cnt_group_0_5_90 as pct_address_cnt_group_0_5_90
	,b.pct_address_cnt_group_0_5_95 as pct_address_cnt_group_0_5_95
	,b.pct_address_cnt_group_0_5_99 as pct_address_cnt_group_0_5_99
from
(
	select
		1 as flag
		,txn_id
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
	from
		safe_tmp.leo_temp_rule_var
) a
left join
(
	select
		1 as flag
		,quantile(0.01)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_01
    	,quantile(0.05)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_05
    	,quantile(0.10)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_10
    	,quantile(0.20)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_20
    	,quantile(0.25)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_25
    	,quantile(0.30)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_30
    	,quantile(0.40)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_40
    	,quantile(0.50)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_50
    	,quantile(0.60)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_60
    	,quantile(0.70)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_70
    	,quantile(0.75)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_75
    	,quantile(0.80)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_80
    	,quantile(0.90)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_90
    	,quantile(0.95)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_95
    	,quantile(0.99)(pct_address_cnt_group_pre) as pct_address_cnt_group_pre_99

    	,quantile(0.01)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_01
    	,quantile(0.05)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_05
    	,quantile(0.10)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_10
    	,quantile(0.20)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_20
    	,quantile(0.25)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_25
    	,quantile(0.30)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_30
    	,quantile(0.40)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_40
    	,quantile(0.50)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_50
    	,quantile(0.60)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_60
    	,quantile(0.70)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_70
    	,quantile(0.75)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_75
    	,quantile(0.80)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_80
    	,quantile(0.90)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_90
    	,quantile(0.95)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_95
    	,quantile(0.99)(pct_address_cnt_group_0_5) as pct_address_cnt_group_0_5_99

	from
		safe_tmp.leo_temp_rule_var
	group by
		flag
) b
on
	a.flag = b.flag
;
--103841