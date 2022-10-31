drop table if EXISTS safe_tmp.leo_temp_rule_data_1;
CREATE TABLE safe_tmp.leo_temp_rule_data_1 ON CLUSTER default ENGINE= MergeTree()  ORDER BY group_id_pre
as
select
    txn_id
    ,txn_hash
    ,user_id
    ,address
    ,grant_id as group_grant_id
    ,chain as group_chain
    ,network as group_network
    ,token as group_token
    ,toDecimal32(amount_in_usdt,4) as group_amount_in_usdt
    ,parseDateTimeBestEffortOrNull(time) as current_time

    ,group_id as group_id_pre
    ,toDecimal32OrNull(address_cnt_group_id,1) as address_cnt_group_pre
    ,parseDateTimeBestEffortOrNull(time_group_id) as next_time_group_pre
    ,toDecimal128OrNull(substring(gap_time_group_id,1,position(gap_time_group_id, '.')+4),4) as time_gap_group_pre
    ,toDecimal128OrNull(substring(time_01_p_group_id,1,position(time_01_p_group_id, '.')+4),4) as time_p_01_group_pre
    ,toDecimal128OrNull(substring(time_05_p_group_id,1,position(time_05_p_group_id, '.')+4),4) as time_p_05_group_pre
    ,toDecimal128OrNull(substring(time_10_p_group_id,1,position(time_10_p_group_id, '.')+4),4) as time_p_10_group_pre
    ,toDecimal128OrNull(substring(time_25_p_group_id,1,position(time_25_p_group_id, '.')+4),4) as time_p_25_group_pre
    ,toDecimal128OrNull(substring(time_50_p_group_id,1,position(time_50_p_group_id, '.')+4),4) as time_p_50_group_pre
    ,toDecimal128OrNull(substring(time_75_p_group_id,1,position(time_75_p_group_id, '.')+4),4) as time_p_75_group_pre
    ,toDecimal128OrNull(substring(time_std_group_id,1,position(time_std_group_id, '.')+4),4) as time_std_group_pre
    ,toDecimal128OrNull(substring(time_mean_group_id,1,position(time_mean_group_id, '.')+4),4) as time_mean_group_pre
    ,toDecimal128OrNull(substring(time_cov_group_id,1,position(time_cov_group_id, '.')+4),4) as time_cov_group_pre

    ,group_id_hour_0_5
    ,toDecimal32OrNull(address_cnt_group_id_hour_0_5,1) as address_cnt_group_0_5
    ,parseDateTimeBestEffortOrNull(time_group_id_hour_0_5) as next_time_group_0_5
    ,toDecimal128OrNull(substring(gap_time_group_id_hour_0_5,1,position(gap_time_group_id_hour_0_5, '.')+4),4) as time_gap_group_0_5
    ,toDecimal128OrNull(substring(time_01_p_group_id_hour_0_5,1,position(time_01_p_group_id_hour_0_5, '.')+4),4) as time_p_01_group_0_5
    ,toDecimal128OrNull(substring(time_05_p_group_id_hour_0_5,1,position(time_05_p_group_id_hour_0_5, '.')+4),4) as time_p_05_group_0_5
    ,toDecimal128OrNull(substring(time_10_p_group_id_hour_0_5,1,position(time_10_p_group_id_hour_0_5, '.')+4),4) as time_p_10_group_0_5
    ,toDecimal128OrNull(substring(time_25_p_group_id_hour_0_5,1,position(time_25_p_group_id_hour_0_5, '.')+4),4) as time_p_25_group_0_5
    ,toDecimal128OrNull(substring(time_50_p_group_id_hour_0_5,1,position(time_50_p_group_id_hour_0_5, '.')+4),4) as time_p_50_group_0_5
    ,toDecimal128OrNull(substring(time_75_p_group_id_hour_0_5,1,position(time_75_p_group_id_hour_0_5, '.')+4),4) as time_p_75_group_0_5
    ,toDecimal128OrNull(substring(time_std_group_id_hour_0_5,1,position(time_std_group_id_hour_0_5, '.')+4),4) as time_std_group_0_5
    ,toDecimal128OrNull(substring(time_mean_group_id_hour_0_5,1,position(time_mean_group_id_hour_0_5, '.')+4),4) as time_mean_group_0_5
    ,toDecimal128OrNull(substring(time_cov_group_id_hour_0_5,1,position(time_cov_group_id_hour_0_5, '.')+4),4) as time_cov_group_0_5
from
    gr15_hackathon.rule_data1
where
    
    txn_id is not null and txn_id != ''
    and address_cnt is not null and address_cnt != ''
    and txn_hash is not null and txn_hash != ''
    and user_id is not null and user_id != ''
    and address is not null and address != ''
    and grant_id is not null and grant_id != ''
    and chain is not null and chain != ''
    and network is not null and network != ''
    and token is not null and token != ''
    and amount_in_usdt is not null and amount_in_usdt != ''
    and time is not null and time != ''
order by
    group_id
    ,time
;
----399214
    