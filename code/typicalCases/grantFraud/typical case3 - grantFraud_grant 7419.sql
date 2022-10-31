---------------------------------------------------------------------------------------
--typical case grantFraud:grant 7419
---------------------------------------------------------------------------------------
select
	a.address as donate_address
	,'0xFd9F8A0f4bdEaC72F08AF1c708023cC31dD2E3BE' as grant_address
	,a.current_time as `time`
	,a.group_chain as chain
	,a.group_token as token
	,a.group_amount_in_usdt as amount
	,a.group_grant_id as grant_id
	,a.user_id as user_id
	,a.txn_id as txn_id
	,a.txn_hash as txn_hash

--	,a.group_id_pre as group_id_pre
--	,a.address_cnt_group_pre as address_cnt_group_pre
--	,b.group_id_hour_0_5 as group_id_hour_0_5
--	,b.address_cnt_group_0_5 as address_cnt_group_0_5
--	,a.group_network as group_network
--	,a.next_time_group_pre as next_time_group_pre
--	,a.time_gap_group_pre as time_gap_group_pre
--	,b.pct_address_cnt_group_pre as pct_address_cnt_group_pre
--	,b.pct_address_cnt_group_0_5 as pct_address_cnt_group_0_5
--	,b.grant_cnt as grant_cnt
--	,b.amount_score as amount_score
--	,b.pre_cnt_score as pre_cnt_score
--	,b.pre_pct_score as pre_pct_score
--	,b.05_cnt_score as 05_cnt_score
--	,b.05_time_75_score as 05_time_75_score
--	,b.05_pct_score as 05_pct_score
--	,b.05_cov_score as 05_cov_score
--	,b.trans_score as trans_score
--	,b.address_max_score as address_max_score
	,case
		when a.address in
		(
			'0xfc66a1f969bb77eb89a314725d657312d58f1589'
			,'0xcf1dc2cb1a5b5344330b01a188d0cdc62773fe5e'
			,'0x0b6426070a98451308ee54cd3b3c114f5d1a2d65'
			,'0x288819c32f2228203aa9065dfa53497cc2527e69'
			,'0x05ed44153d4cb72748595ef915118772cf189553'
			,'0x79f2d17c3f46a9fffbbd3a5536b39c0cf6fc7b46'
			,'0xc6e2e356a38d91b79a3dee08fcc2a97e6ca12e52'
			,'0xb17e02ca9aa7d8554a5fe5aaa6fc2acac4aa5258'
			,'0x05b9488772c41dcf30aafa61f87168c6525f6e56'
			,'0xdd0a91f517685b6cc03284d6ef2a1c6534c21668'
			,'0x83ac2bb284930f4a9acfffb7cfb0dc0c92b5ab97'
			,'0xb869898cd011593d3d52037b743131924375e0ae'
			,'0xaef1313c44fb0c3c2d92182891242a42fb6096d7'
			,'0x8431e3b7dd271a7e8ef98a8f7ad9ba5cb8ca6aef'
			,'0x8b01f513b7371e48f0c03a7ae3d7ff3762676905'
			,'0x5c977f6520a5e6d1ca9c26133d80f2ce47bf937c'
			,'0x443aece9bf49cf54f23a186010cb0ce64c886b6d'
			,'0x0cd9d360311d935ad998f6c21edaf282b17f347e'
			,'0x04525c54438be2410dde4de7f2c4773457688946'
			,'0xae86d0ad922a0abe16878913a71cdcd018a50b96'
			,'0x1b5dff786eaccf5a41bf922e64313a0f4a60dab9'
			,'0xd69ae051efd6b799b8d10240d90e5df3fccf7774'
			,'0xae9982cfb1bb53954e2bdce2ed08e848a9c15540'
			,'0xeeeff9a28b467bc90de2315e0e0f9891648bace4'
			,'0x4fec52eb5658a89c9c57fd5986a0d0235ae7fd48'
			,'0x1bbc65739ccb24c2a010afa7b227580de7e809e6'
			,'0x21564c8648907e31c987e17713159caf6db6b311'
			,'0x5b2c14daba1907c3b8ef0f37e5b6032e98b58680'
			,'0xdd690b2d1e4c79a31ad2858c757a371b1b58c37d'
		) then 1
		else 0
	end as flag
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
		group_grant_id = '7419'
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
		group_grant_id = '7419'
) b
on
	a.txn_hash = b.txn_hash and a.address = b.address
--where
--	b.grant_cnt < 50
--	a.group_token = 'MATIC'
--	and a.group_amount_in_usdt < toDecimal128(0.1,4)
--	and trans_score >= 25
--	and a.address_cnt_group_0_5 >= 100
order by
	address_cnt_group_pre
	,group_grant_id
	,current_time
;