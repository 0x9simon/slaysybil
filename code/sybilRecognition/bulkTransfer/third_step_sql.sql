
 
 SELECT 
a.*
,b.eth_amount
,b.usdt_amount
,b.usdc_amount
,b.dai_amount  
,c.max_contribute_dt as max_contribute_dt
,b.max_prepare_dt as max_prepare_dt
-- ,dateDiff('day', toDateTime(b.max_contribute_dt), toDateTime(a.max_prepare_dt)) as gap_day  -- 
,c.sum_contribute_amount
,(b.eth_amount*1300+b.usdt_amount+b.usdc_amount+b.dai_amount) as tranfer_all_amount  -- 
,c.sum_contribute_amount/(b.eth_amount*1300+b.usdt_amount+b.usdc_amount+b.dai_amount) as ContrAmountRatio -- 
from 
(

	SELECT 
	token_txn_to as address
	,max( add_cnt)           as max_add_cnt
	,max( NumOfDistinctAmount)        as max_amount_cnt
	,max( MaxAmount)        as max_max_amount
	,max( ContrRatio)             as max_ratio
	,max( Hash_contribution_rule)  as max_hash_contri_rule
	,max( Amount_cnt_rule)   as max_amount_cnt_rule
	,max( Amount_max_rule)   as max_amount_max_rule
	,max( Contr_ratio_rule)  as max_contr_ratio_rule
	from(
	SELECT  
	a.*
	,b.token_txn_to
	from (
	SELECT * from batch_transfer_contract_info

	)a LEFT JOIN (
		SELECT token_txn_to, hash from ethereum_transfer

	)b on a.hash=b.hash
	) GROUP BY  token_txn_to

)a
left join(
	SELECT 
	`from` as address
	,sum(case when token='ETH' then  toFloat64OrNull(token_count) else 0 end ) as eth_amount
	,sum(case when token='USDT' then toFloat64OrNull(token_count) else 0 end ) as usdt_amount
	,sum(case when token='USDC' then toFloat64OrNull(token_count) else 0 end ) as usdc_amount
	,sum(case when token='DAI' then  toFloat64OrNull(token_count) else 0 end ) as dai_amount  
	,substr(max(dt),1,19) as max_prepare_dt
	FROM `ethereum_transfer` WHERE `to` = lower('0xabea9132b05a70803a4e85094fd0e1800777fbef')
	and `from` in (
	SELECT address from contributions_dataset GROUP BY address
	) 
	and dt>='2022-08-22 00:00:00'
	and dt<='2022-09-22 24:59:59'
	GROUP BY `from` 
)b on a.address=b.address
left join (
SELECT 
address 
,substr(max( timestamp),1,19) as max_contribute_dt
,sum(toFloat64OrNull(amount_in_usdt)) as sum_contribute_amount
FROM `contributions_dataset` where network='mainnet' group by address

)c on a.address=c.address