
SELECT 
	`token_txn_from`
    ,`hash`
    ,arrayCompact(groupArray(`token_txn_to`)) as add_list
    ,arrayCompact(groupArray(`token_count`)) as amount_list
    ,count( DISTINCT(`token_txn_to`)) as add_cnt
    ,count( DISTINCT(`token_count`)) as amount_cnt
	,stddev(`token_count`)  as std_amount
    -- ,GROUP_CONCAT(`token_txn_to`) as add_list
    FROM `ethereum_transfer` WHERE `token_txn_to` in (
        SELECT address FROM `contributions_dataset` where (address != '')
        and chain in ('eth_zksync','eth_std','eth_polygon','celo_std')
        and network='mainnet' group by address 
) and asset = 'ETH' group by `token_txn_from`,`hash` ORDER BY count( DISTINCT(`token_txn_to`)) DESC 


-- create tabel batch_transfer_part1
-- create tabel batch_transfer_part2