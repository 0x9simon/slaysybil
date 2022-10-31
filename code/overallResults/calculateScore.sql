--------------------------------------------------------------------------
--1.合并4种识别方案结果明细数据
---------------------------------------------------------------------------
drop table if EXISTS safe_tmp.leo_result_address_all;
CREATE TABLE safe_tmp.leo_result_address_all ON CLUSTER default ENGINE= MergeTree()  ORDER BY address
as
select
     a.address as address
    ,a.bulkdonate_risklevel as bulkdonate_risklevel
    ,b.atg_risklevel as atg_risklevel
    ,c.be_risklevel as be_risklevel
    ,d.bulktrans_risklevel as bulktrans_risklevel
from
(
    select
        address
        ,bulkdonate_risklevel
    from
        safe_tmp.leo_result_bulk_donate
) a
left join
(
    select
        address
        ,result as atg_risklevel
    from
        safe_tmp.leo_result_atg
) b
on a.address = b.address
left join
(
    select
        address
        ,be_risklevel
    from
        safe_tmp.leo_result_be
) c
on a.address = c.address
left join
(
    select
        address
        ,bulktrans_risklevel
    from
        safe_tmp.leo_result_bulk_trans
) d
on lower(a.address) = lower(d.address)
;
--55303

--------------------------------------------------------------------------
--2.计算Address最终的女巫攻击风险
--方法：4种方法中有一种认为是高风险，即为高风险；否则，4种方法中有一种认为是中风险，即为中风险；其他，为低风险；
---------------------------------------------------------------------------
drop table if EXISTS safe_tmp.leo_result_address_final;
CREATE TABLE safe_tmp.leo_result_address_final ON CLUSTER default ENGINE= MergeTree()  ORDER BY address
as
select
    address
    ,case
        when bulkdonate_risklevel = 'high' then 'high'
        when bulkdonate_risklevel = 'medium' then 'medium'
        else 'low'
    end as bulkdonate_risklevel
    ,case
        when atg_risklevel = 'high' then 'high'
        when atg_risklevel = 'midium' then 'medium'
        else 'low'
    end as atg_risklevel
    ,case
        when be_risklevel = 'high' then 'high'
        when be_risklevel = 'medium' then 'medium'
        else 'low'
    end as be_risklevel
    ,case
        when bulktrans_risklevel = 'high' then 'high'
        when bulktrans_risklevel = 'medium' then 'medium'
        else 'low'
    end as bulktrans_risklevel
    ,case
        when bulkdonate_risklevel = 'high' or atg_risklevel = 'high' or be_risklevel = 'high' or bulktrans_risklevel = 'high' then 'high'
        when bulkdonate_risklevel = 'medium' or atg_risklevel = 'medium' or be_risklevel = 'medium' or bulktrans_risklevel = 'medium' then 'medium'
        else 'low'
    end as risk_level
from
    safe_tmp.leo_result_address_all
;

--select * from safe_tmp.leo_result_address_final limit 100;
--select count(*) from safe_tmp.leo_result_address_final;
--55303

--------------------------------------------------------------------------
--4.统计高中低风险等级数据情况
---------------------------------------------------------------------------
select risk_level, count(1) as address_cnt from safe_tmp.leo_result_address_final group by risk_level;
select bulkdonate_risklevel, count(1) as bulkdonate_risklevel_address_cnt from safe_tmp.leo_result_address_final group by bulkdonate_risklevel;
select atg_risklevel, count(1) as atg_risklevel_address_cnt from safe_tmp.leo_result_address_final group by atg_risklevel;
select be_risklevel, count(1) as be_risklevel_address_cnt from safe_tmp.leo_result_address_final group by be_risklevel;
select bulktrans_risklevel, count(1) as bulktrans_risklevel_address_cnt from safe_tmp.leo_result_address_final group by bulktrans_risklevel;



