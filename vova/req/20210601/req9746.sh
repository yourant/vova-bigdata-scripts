drop table if exists tmp.tmp_zyzheng_req_base1_06078;
create table tmp.tmp_zyzheng_req_base1_06078 as
select
log.pt,
dvg.goods_id
from
dwd.dwd_vova_log_goods_impression log
inner join dim.dim_vova_goods dvg on dvg.virtual_goods_id = log.virtual_goods_id
inner join dim.dim_vova_devices dd on dd.device_id = log.device_id and dd.datasource = log.datasource
where log.pt >= '2021-05-01'
and log.pt <= '2021-05-31'
and log.datasource = 'vova'
and log.platform = 'mob'
and log.virtual_goods_id is not null
and date(dd.activate_time) = log.pt
group by log.pt, dvg.goods_id
;


select
date(dd.activate_time),
count(distinct dd.device_id)
from
dim.dim_vova_devices dd
where date(dd.activate_time) >= '2021-05-01'
and date(dd.activate_time) <= '2021-05-31'
and dd.datasource = 'vova'
group by date(dd.activate_time)
order by date(dd.activate_time)
;

select
t1.pt,
count(distinct goods_id) AS cnt_goods
from
tmp.tmp_zyzheng_req_base1_06078 t1
group by t1.pt
order by t1.pt
;

select
temp.pt,
count(distinct goods_id) AS cnt_1
from
(
select
t1.pt,
t1.goods_id,
count(*) AS cnt
from
tmp.tmp_zyzheng_req_base1_06078 t1
inner join ods_vova_vts.ods_vova_goods_comment vgc on vgc.goods_id = t1.goods_id
group by t1.pt,
t1.goods_id
) temp
group by temp.pt
order by temp.pt
;

select
temp.pt,
count(distinct goods_id) AS cnt_10
from
(
select
t1.pt,
t1.goods_id,
count(*) AS cnt
from
tmp.tmp_zyzheng_req_base1_06078 t1
inner join ods_vova_vts.ods_vova_goods_comment vgc on vgc.goods_id = t1.goods_id
group by t1.pt,
t1.goods_id
) temp
where temp.cnt >= 10
group by temp.pt
order by temp.pt
;


where date(dd.activate_time) >= '2021-05-01'
and date(dd.activate_time) <= '2021-05-31'
and dd.datasource = 'vova'


insert overwrite table tmp.tmp_zyzheng_req_base1_0607
select /*+ REPARTITION(1) */
regexp_replace(lower(trim(cc.element_id)),'\n|\t|\r', ' ') key_words,
count(*) as pv,
count(distinct cc.device_id) as uv
from dwd.dwd_vova_log_common_click cc
inner join dim.dim_vova_devices dd on dd.device_id = cc.device_id and dd.datasource = cc.datasource
where cc.pt >= '2021-05-01'
and cc.pt <= '2021-05-31'
and cc.pt = date(dd.activate_time)
and cc.element_name = 'search_confirm'
and cc.datasource = 'vova'
group by regexp_replace(lower(trim(cc.element_id)),'\n|\t|\r', ' ')
;
select *  from tmp.tmp_zyzheng_req_base1_0607 order by pv desc limit 10;
select count(distinct key_words), count(*) as a  from tmp.tmp_zyzheng_req_base1_0607 where pv > 1;

spark-sql -e "
select
key_words,
pv,
uv
from
tmp.tmp_zyzheng_req_base1_0607
 where pv > 1
;
"  > vova_search_0607.csv

DROP TABLE IF EXISTS tmp.tmp_zyzheng_req_base1_0607;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_zyzheng_req_base1_0607
(
    key_words         STRING,
    pv                BIGINT,
    uv                BIGINT
) COMMENT 'tmp_zyzheng_req_base1'
    STORED AS PARQUETFILE
;