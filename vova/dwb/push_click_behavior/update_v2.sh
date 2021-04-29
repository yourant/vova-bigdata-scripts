#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="dwd_vova_push_click_behavior_chenkai_${cur_date}"

month=`date -d "${cur_date}" +%m`

# sh /mnt/vova-bd-scripts/rpt/push_click_behavior/app-push-logs-to-hive.sh

interval_flag=72
interval=72

sql="
CREATE TABLE IF NOT EXISTS tmp.tmp_push_logs_${table_suffix} as
SELECT /*+ REPARTITION(200) */
  vapl.user_id                                           AS buyer_id,
  nvl(dev.region_code, 'NA')                         as region_code,
  vapl.switch_on,
  vapl.push_result,
  vapl.platform,
  nvl(vaptc.app_platform, 'NA')                          as datasource,
  cast(vapl.task_config_id as string)                    AS config_id,
  cast(vaptc.target_type as string)                      as target_type,
  vapl.push_time,
  nvl(dev.main_channel, 'Unknown')                       as main_channel,
  nvl(regexp_extract(ug.user_tag, 'R_([0-9])', 0), 'NA') as r_tag,
  nvl(regexp_extract(ug.user_tag, 'F_([0-9])', 0), 'NA') as f_tag,
  nvl(regexp_extract(ug.user_tag, 'M_([0-9])', 0), 'NA') as m_tag,
  case
    when ug.user_tag like '%is_new%' then 'new'
    when ug.user_tag like '%old%' then 'old'
    else 'NA'
    end                                                as is_new,
  case
    when vaptc.expected_period = 0 then '单日单次'
    when vaptc.expected_period = 20 then '每日循环'
    else 'others'
    end job_rate
FROM
  ods_vova_ext.ods_vova_app_push_logs vapl
inner join
  ods_vova_vtp.ods_vova_app_push_task_config vaptc
on vaptc.id = vapl.task_config_id
inner join
  ods_vova_vtp.ods_vova_app_push_task vapt2
on vapt2.id = vapl.task_id
left join
  dim.dim_vova_devices dev
on vapl.user_id = dev.current_buyer_id
left join
(
  select
    vut.user_id                            as buyer_id,
    concat_ws(',', collect_set(vapt.code)) as user_tag
  from
    ods_vova_vtp.ods_vova_user_tags vut
  inner join
    ods_vova_vtp.ods_vova_app_push_tag vapt
  on vut.tag_id = vapt.id
  group by vut.user_id
) as ug
on ug.buyer_id = vapl.user_id
where vapl.pt = '${cur_date}';

CREATE TABLE IF NOT EXISTS tmp.push_stas_${table_suffix} as
select
  'vova'                                                   as datasource,
  nvl(tpl.platform, 'all')                                 as platform,
  nvl(tpl.region_code, 'all')                              as region_code,
  '1'                                                      as target_type,
   'all'                                   as r_tag,
  'all'                                    as f_tag,
   'all'                                    as m_tag,
   'all'                                  as is_new,
  nvl(tpl.config_id, 'all')                                as config_id,
  nvl(tpl.main_channel, 'all')                             as main_channel,
  nvl(tpl.job_rate, 'all')                             as job_rate,
  0                                                        as push_click_uv,
  0                                                        as impressions,
  0                                                        as impressions_uv,
  0                                                        as impressions_pd,
  0                                                        as impressions_pd_uv,
  0                                                        as impressions_ex_pd,
  0                                                        as impressions_ex_pd_uv,
  0                                                        as carts,
  0                                                        as carts_uv,
  0                                                        as orders,
  0                                                        as orders_uv,
  0                                                        as pays,
  0                                                        as pays_uv,
  0                                                        as gmv,
  sum(1)                                                   as try_num,
  sum(if(tpl.push_result = 1, 1, 0))                       as push_num,
  sum(if(tpl.push_result = 1 and tpl.switch_on = 1, 1, 0)) as success_num
from
  tmp.tmp_push_logs_${table_suffix} tpl
where tpl.datasource = 'vova'
group by cube (tpl.platform, tpl.config_id, tpl.main_channel, tpl.region_code,tpl.job_rate)
union all
select
  'airyclub'                                               as datasource,
  nvl(tpl.platform, 'all')                                 as platform,
  nvl(tpl.region_code, 'all')                              as region_code,
  '1'                                                      as target_type,
  'all'                                    as r_tag,
  'all'                                    as f_tag,
  'all'                                    as m_tag,
  'all'                                   as is_new,
  nvl(tpl.config_id, 'all')                                as config_id,
  nvl(tpl.main_channel, 'all')                             as main_channel,
  nvl(tpl.job_rate, 'all')                             as job_rate,
  0                                                        as push_click_uv,
  0                                                        as impressions,
  0                                                        as impressions_uv,
  0                                                        as impressions_pd,
  0                                                        as impressions_pd_uv,
  0                                                        as impressions_ex_pd,
  0                                                        as impressions_ex_pd_uv,
  0                                                        as carts,
  0                                                        as carts_uv,
  0                                                        as orders,
  0                                                        as orders_uv,
  0                                                        as pays,
  0                                                        as pays_uv,
  0                                                        as gmv,
  sum(1)                                                   as try_num,
  sum(if(tpl.push_result = 1, 1, 0))                       as push_num,
  sum(if(tpl.push_result = 1 and tpl.switch_on = 1, 1, 0)) as success_num
from
  tmp.tmp_push_logs_${table_suffix} tpl
where tpl.datasource = 'airyclub'
group by cube (tpl.platform, tpl.config_id, tpl.main_channel, tpl.region_code,tpl.job_rate)
union all
select
  'app-group'                                              as datasource,
  nvl(tpl.platform, 'all')                                 as platform,
  nvl(tpl.region_code, 'all')                              as region_code,
  '1'                                                      as target_type,
  'all'                                    as r_tag,
  'all'                                    as f_tag,
  'all'                                    as m_tag,
  'all'                                   as is_new,
  nvl(tpl.config_id, 'all')                                as config_id,
  nvl(tpl.main_channel, 'all')                             as main_channel,
  nvl(tpl.job_rate, 'all')                             as job_rate,
  0                                                        as push_click_uv,
  0                                                        as impressions,
  0                                                        as impressions_uv,
  0                                                        as impressions_pd,
  0                                                        as impressions_pd_uv,
  0                                                        as impressions_ex_pd,
  0                                                        as impressions_ex_pd_uv,
  0                                                        as carts,
  0                                                        as carts_uv,
  0                                                        as orders,
  0                                                        as orders_uv,
  0                                                        as pays,
  0                                                        as pays_uv,
  0                                                        as gmv,
  sum(1)                                                   as try_num,
  sum(if(tpl.push_result = 1, 1, 0))                       as push_num,
  sum(if(tpl.push_result = 1 and tpl.switch_on = 1, 1, 0)) as success_num
from
  tmp.tmp_push_logs_${table_suffix} tpl
where tpl.datasource in (select distinct data_domain from ods_vova_vtsf.ods_vova_acg_app)
  and tpl.datasource not in ('vova','airyclub')
group by cube (tpl.platform, tpl.config_id, tpl.main_channel, tpl.region_code,tpl.job_rate)
-- 无脑 union
union all
SELECT
  datasource,
  platform,
  region_code,
  target_type,
  r_tag,
  f_tag,
  m_tag,
  is_new,
  config_id,
  main_channel,
  job_rate,
  push_click_uv,
  impressions,
  impressions_uv,
  impressions_pd,
  impressions_pd_uv,
  impressions_ex_pd,
  impressions_ex_pd_uv,
  carts,
  carts_uv,
  orders,
  orders_uv,
  pays,
  pays_uv,
  gmv,
  try_num,
  push_num,
  success_num
from
(
  select
    nvl(tpl.datasource, 'all')                               as datasource,
    nvl(tpl.platform, 'all')                                 as platform,
    nvl(tpl.region_code, 'all')                              as region_code,
    '1'                                                      as target_type,
    'all'                                                    as r_tag,
    'all'                                                    as f_tag,
    'all'                                                    as m_tag,
    'all'                                                    as is_new,
    nvl(tpl.config_id, 'all')                                as config_id,
    nvl(tpl.main_channel, 'all')                             as main_channel,
    nvl(tpl.job_rate, 'all')                                 as job_rate,
    0                                                        as push_click_uv,
    0                                                        as impressions,
    0                                                        as impressions_uv,
    0                                                        as impressions_pd,
    0                                                        as impressions_pd_uv,
    0                                                        as impressions_ex_pd,
    0                                                        as impressions_ex_pd_uv,
    0                                                        as carts,
    0                                                        as carts_uv,
    0                                                        as orders,
    0                                                        as orders_uv,
    0                                                        as pays,
    0                                                        as pays_uv,
    0                                                        as gmv,
    sum(1)                                                   as try_num,
    sum(if(tpl.push_result = 1, 1, 0))                       as push_num,
    sum(if(tpl.push_result = 1 and tpl.switch_on = 1, 1, 0)) as success_num
  from
    tmp.tmp_push_logs_${table_suffix} tpl
  where tpl.datasource in ('nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
  group by cube (tpl.datasource, tpl.platform, tpl.config_id, tpl.main_channel, tpl.region_code,tpl.job_rate)
) t1 where datasource != 'all'
;

-- push_click
create table IF NOT EXISTS tmp.tmp_push_click_all_${table_suffix} as
select /*+ REPARTITION(3) */ distinct
  case when fpc.datasource = 'vova' then 'vova'
    when fpc.datasource = 'airyclub' then 'airyclub'
    else 'app-group'
    end datasource,
  dev.region_code,
  nvl(fpc.platform, 'NA')          AS platform,
  nvl(fpc.target_type, 'NA')       AS target_type,
  nvl(fpc.r_tag, 'NA')             AS r_tag,
  nvl(fpc.f_tag, 'NA')             AS f_tag,
  nvl(fpc.m_tag, 'NA')             AS m_tag,
  nvl(fpc.is_new, 'NA')            AS is_new,
  nvl(fpc.config_id, 'NA')         AS config_id,
  case
    when vaptc.expected_period = 0 then '单日单次'
    when vaptc.expected_period = 20 then '每日循环'
    else 'others' end               job_rate,
  fpc.click_time                   AS click_time,
  fpc.push_time,
  fpc.time_zone,
  fpc.device_id,
  nvl(dev.main_channel, 'Unknown') as main_channel,
  '${interval}h'                   as click_interval
from
  dwd.dwd_vova_fact_push_click fpc
left join dim.dim_vova_devices dev
on dev.device_id = fpc.device_id
join
(
  select
    device_id
  from
    dwd.dwd_vova_log_common_click a
  join
    ods_vova_vtp.ods_vova_app_push_task b
  on split(a.element_id, '@')[0] = b.id
  where a.pt = '${cur_date}'
    and a.element_name = 'PushUserDecide'
    and a.element_id like '%@%'
  group by device_id
) flcc
on fpc.device_id = flcc.device_id
left join
  ods_vova_vtp.ods_vova_app_push_task_config vaptc
on fpc.config_id = vaptc.id
where date(fpc.push_time) = '${cur_date}'
  and fpc.device_id is not null
  and fpc.datasource in (select distinct data_domain from ods_vova_vtsf.ods_vova_acg_app)

union all
select /*+ REPARTITION(1) */ distinct
  fpc.datasource datasource,
  dev.region_code,
  nvl(fpc.platform, 'NA')          AS platform,
  nvl(fpc.target_type, 'NA')       AS target_type,
  nvl(fpc.r_tag, 'NA')             AS r_tag,
  nvl(fpc.f_tag, 'NA')             AS f_tag,
  nvl(fpc.m_tag, 'NA')             AS m_tag,
  nvl(fpc.is_new, 'NA')            AS is_new,
  nvl(fpc.config_id, 'NA')         AS config_id,
  case
    when vaptc.expected_period = 0 then '单日单次'
    when vaptc.expected_period = 20 then '每日循环'
    else 'others' end               job_rate,
  fpc.click_time                   AS click_time,
  fpc.push_time,
  fpc.time_zone,
  fpc.device_id,
  nvl(dev.main_channel, 'Unknown') as main_channel,
  '${interval}h'                   as click_interval
from
  dwd.dwd_vova_fact_push_click fpc
left join
  dim.dim_vova_devices dev
on dev.device_id = fpc.device_id
join
(
  select
    device_id
  from
    dwd.dwd_vova_log_common_click a
  join
    ods_vova_vtp.ods_vova_app_push_task b
  on split(a.element_id, '@')[0] = b.id
  where a.pt = '${cur_date}'
    and a.element_name = 'PushUserDecide'
    and a.element_id like '%@%'
  group by device_id
) flcc
on fpc.device_id = flcc.device_id
left join
  ods_vova_vtp.ods_vova_app_push_task_config vaptc
on fpc.config_id = vaptc.id
where date(fpc.push_time) = '${cur_date}'
  and fpc.device_id is not null
  and fpc.datasource in ('nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
;

-- impressions
create table IF NOT EXISTS tmp.tmp_push_click_all_imp_${table_suffix} as
select /*+ REPARTITION(10) */
  fpc.device_id,
  gi.buyer_id,
  gi.pd_dvce
from
  tmp.tmp_push_click_all_${table_suffix} as fpc
inner join
(
  select
    gi.device_id,
    gi.buyer_id,
    gi.collector_tstamp,
    gi.device_id as pd_dvce
  from
    dwd.dwd_vova_log_goods_impression gi
  where gi.pt >= '${cur_date}'
    and gi.pt <= date_add('${cur_date}', 1)
    and gi.page_code = 'product_detail'
) gi
on fpc.device_id = gi.device_id
where gi.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
  and gi.collector_tstamp / 1000 - 12 * 3600 < unix_timestamp(fpc.click_time);

-- product_detail
create table IF NOT EXISTS tmp.tmp_push_click_all_pd_${table_suffix} as
select /*+ REPARTITION(20) */
  fpc.device_id,
  gi.buyer_id,
  gi.ex_pd_dvce
from
  tmp.tmp_push_click_all_${table_suffix} as fpc
inner join
(
  select
    gi.device_id,
    gi.buyer_id,
    gi.collector_tstamp,
    gi.device_id as ex_pd_dvce
 from
   dwd.dwd_vova_log_goods_impression gi
 where gi.pt >= '${cur_date}'
   and gi.pt <= date_add('${cur_date}', 1)
   and gi.page_code != 'product_detail'
) gi
on fpc.device_id = gi.device_id
where gi.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
  and gi.collector_tstamp / 1000 - 12 * 3600 < unix_timestamp(fpc.click_time);

-- clicks
create table IF NOT EXISTS tmp.tmp_push_click_all_clk_${table_suffix} as
select /*+ REPARTITION(20) */
  fpc.device_id
from
  tmp.tmp_push_click_all_${table_suffix} fpc
inner join
(
  select
    cc.device_id,
    cc.collector_tstamp
  from
    dwd.dwd_vova_log_common_click cc
  where cc.pt >= '${cur_date}'
    and cc.pt <= date_add('${cur_date}', 1)
    and cc.element_name = 'pdAddToCartClick'
) cc
on fpc.device_id = cc.device_id
where cc.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
  and cc.collector_tstamp / 1000 - 12 * 3600 < unix_timestamp(fpc.click_time);

-- order
create table IF NOT EXISTS tmp.tmp_push_click_all_order_${table_suffix} as
select /*+ REPARTITION(20) */
  fpc.device_id,
  og.order_id,
  og.buyer_id
from
  tmp.tmp_push_click_all_${table_suffix} fpc
inner join
  dim.dim_vova_order_goods og
on fpc.device_id = og.device_id
where og.order_time > fpc.click_time
  and unix_timestamp(og.order_time) - 12 * 3600 < unix_timestamp(fpc.click_time);

-- pay
create table IF NOT EXISTS tmp.tmp_push_click_all_pay_${table_suffix} as
select /*+ REPARTITION(20) */
  fpc.device_id,
  py.order_id,
  py.buyer_id,
  py.shipping_fee,
  py.goods_number,
  py.shop_price,
  if(dg.brand_id > 0,'Y','N') is_brand
from
  tmp.tmp_push_click_all_${table_suffix} fpc
inner join
  dwd.dwd_vova_fact_pay py
on fpc.device_id = py.device_id
inner join
  dim.dim_vova_goods dg
on py.goods_id = dg.goods_id
where py.pay_time > fpc.click_time
  and unix_timestamp(py.pay_time) - 12 * 3600 < unix_timestamp(fpc.click_time);

CREATE TABLE IF NOT EXISTS tmp.tmp_push_click_${table_suffix}
select /*+ REPARTITION(10) */
  fpc.datasource datasource,
  fpc.region_code,
  fpc.platform,
  fpc.target_type,
  fpc.r_tag,
  fpc.f_tag,
  fpc.m_tag,
  fpc.is_new,
  fpc.config_id,
  fpc.click_time,
  fpc.device_id,
  fpc.main_channel,
  fpc.job_rate,
  '${interval}h'            as click_interval
from
  tmp.tmp_push_click_all_${table_suffix} fpc
where unix_timestamp(fpc.click_time) - unix_timestamp(fpc.push_time) + cast(fpc.time_zone as bigint) * 3600 < cast('${interval}' as bigint) * 3600
;


CREATE TABLE IF NOT EXISTS tmp.push_click_behavior_v2_result_01_${table_suffix} as
select /*+ REPARTITION(10) */
  ps.datasource,
  ps.platform,
  ps.region_code,
  ps.target_type,
  ps.config_id,
  ps.main_channel,
  ps.job_rate,
  ps.push_click_uv,
  ps.impressions,
  ps.impressions_uv,
  ps.impressions_pd,
  ps.impressions_pd_uv,
  ps.impressions_ex_pd,
  ps.impressions_ex_pd_uv,
  ps.carts,
  ps.carts_uv,
  ps.orders,
  ps.orders_uv,
  ps.pays,
  ps.pays_uv,
  ps.gmv,
  cast(0 as int) brand_gmv,
  cast(0 as int) no_brand_gmv,
  ps.try_num,
  ps.push_num,
  ps.success_num
from
  tmp.push_stas_${table_suffix} as ps
where datasource = 'vova'
union all
select /*+ REPARTITION(10) */
  'vova'                        as datasource,
  nvl(fpc.platform, 'all')      as platform,
  nvl(fpc.region_code, 'all')   as region_code,
  '1'                           as target_type,
  nvl(fpc.config_id, 'all')     as config_id,
  nvl(fpc.main_channel, 'all')  as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  count(distinct fpc.device_id) as push_click_uv,
  0                             as impressions,
  0                             as impressions_uv,
  0                             as impressions_pd,
  0                             as impressions_pd_uv,
  0                             as impressions_ex_pd,
  0                             as impressions_ex_pd_uv,
  0                             as carts,
  0                             as carts_uv,
  0                             as orders,
  0                             as orders_uv,
  0                             as pays,
  0                             as pays_uv,
  0                             as gmv,
  cast(0 as int)                             as brand_gmv,
  cast(0 as int)                             as no_brand_gmv,
  0                             as try_num,
  0                             as push_num,
  0                             as success_num
from
  tmp.tmp_push_click_${table_suffix} fpc
where datasource = 'vova'
group by cube (fpc.platform, fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  'vova'                       as datasource,
  nvl(fpc.platform, 'all')     as platform,
  nvl(fpc.region_code, 'all')  as region_code,
  '1'                          as target_type,
  nvl(fpc.config_id, 'all')    as config_id,
  nvl(fpc.main_channel, 'all') as main_channel,
  nvl(fpc.job_rate, 'all')     as job_rate,
  0                            as push_click_uv,
  0                            as impressions,
  0  as impressions_uv,
  sum(1)                       as impressions_pd,
  count(distinct gi.pd_dvce)   as impressions_pd_uv,
  0                            as impressions_ex_pd,
  0                            as impressions_ex_pd_uv,
  0                            as carts,
  0                            as carts_uv,
  0                            as orders,
  0                            as orders_uv,
  0                            as pays,
  0                            as pays_uv,
  0                            as gmv,
  cast(0 as int)                            as brand_gmv,
  cast(0 as int)                            as no_brand_gmv,
  0                            as try_num,
  0                            as push_num,
  0                            as success_num
from
  tmp.tmp_push_click_${table_suffix} as fpc
inner join
  tmp.tmp_push_click_all_imp_${table_suffix} gi
on fpc.device_id = gi.device_id
where datasource = 'vova'
group by cube (fpc.platform,  fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  'vova'                        as datasource,
  nvl(fpc.platform, 'all')      as platform,
  nvl(fpc.region_code, 'all')   as region_code,
  '1'                           as target_type,
  nvl(fpc.config_id, 'all')     as config_id,
  nvl(fpc.main_channel, 'all')  as main_channel,
  nvl(fpc.job_rate, 'all')      as job_rate,
  0                             as push_click_uv,
  0                             as impressions,
  0   as impressions_uv,
  0                             as impressions_pd,
  0                             as impressions_pd_uv,
  sum(1)                        as impressions_ex_pd,
  count(distinct gi.ex_pd_dvce) as impressions_ex_pd_uv,
  0                             as carts,
  0                             as carts_uv,
  0                             as orders,
  0                             as orders_uv,
  0                             as pays,
  0                             as pays_uv,
  0                             as gmv,
  cast(0 as int)                             as brand_gmv,
  cast(0 as int)                             as no_brand_gmv,
  0                             as try_num,
  0                             as push_num,
  0                             as success_num
from
  tmp.tmp_push_click_${table_suffix} as fpc
inner join
  tmp.tmp_push_click_all_pd_${table_suffix} gi
on fpc.device_id = gi.device_id
where datasource = 'vova'
group by cube (fpc.platform, fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  'vova'                       as datasource,
  nvl(fpc.platform, 'all')     as platform,
  nvl(fpc.region_code, 'all')  as region_code,
  '1'                          as target_type,
  nvl(fpc.config_id, 'all')    as config_id,
  nvl(fpc.main_channel, 'all') as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  0                            as push_click_uv,
  sum(1)                       as impressions,
  count(distinct gi.device_id)  as impressions_uv,
  0                       as impressions_pd,
  0   as impressions_pd_uv,
  0                            as impressions_ex_pd,
  0                            as impressions_ex_pd_uv,
  0                            as carts,
  0                            as carts_uv,
  0                            as orders,
  0                            as orders_uv,
  0                            as pays,
  0                            as pays_uv,
  0                            as gmv,
  cast(0 as int)                            as brand_gmv,
  cast(0 as int)                            as no_brand_gmv,
  0                            as try_num,
  0                            as push_num,
  0                            as success_num
from
  tmp.tmp_push_click_${table_suffix} as fpc
inner join
(
  select
    device_id
  from
    tmp.tmp_push_click_all_pd_${table_suffix}
  union all
  select
    device_id
  from
    tmp.tmp_push_click_all_imp_${table_suffix}
) gi
on fpc.device_id = gi.device_id
where datasource = 'vova'
group by cube (fpc.platform,  fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)

union all
select /*+ REPARTITION(10) */
  'vova'                       as datasource,
  nvl(fpc.platform, 'all')     as platform,
  nvl(fpc.region_code, 'all')  as region_code,
  '1'                          as target_type,
  nvl(fpc.config_id, 'all')    as config_id,
  nvl(fpc.main_channel, 'all') as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  0                            as push_click_uv,
  0                            as impressions,
  0                            as impressions_uv,
  0                            as impressions_pd,
  0                            as impressions_pd_uv,
  0                            as impressions_ex_pd,
  0                            as impressions_ex_pd_uv,
  sum(1)                       as carts,
  count(distinct cc.device_id) as carts_uv,
  0                            as orders,
  0                            as orders_uv,
  0                            as pays,
  0                            as pays_uv,
  0                            as gmv,
  cast(0 as int)                            as brand_gmv,
  cast(0 as int)                            as no_brand_gmv,
  0                            as try_num,
  0                            as push_num,
  0                            as success_num
from
  tmp.tmp_push_click_${table_suffix} fpc
inner join
  tmp.tmp_push_click_all_clk_${table_suffix} cc
on fpc.device_id = cc.device_id
where datasource = 'vova'
group by cube (fpc.platform, fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  'vova'                       as datasource,
  nvl(fpc.platform, 'all')     as platform,
  nvl(fpc.region_code, 'all')  as region_code,
  '1'                          as target_type,
  nvl(fpc.config_id, 'all')    as config_id,
  nvl(fpc.main_channel, 'all') as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  0                            as push_click_uv,
  0                            as impressions,
  0                            as impressions_uv,
  0                            as impressions_pd,
  0                            as impressions_pd_uv,
  0                            as impressions_ex_pd,
  0                            as impressions_ex_pd_uv,
  0                            as carts,
  0                            as carts_uv,
  count(distinct og.order_id)  as orders,
  count(distinct og.device_id)  as orders_uv,
  0                            as pays,
  0                            as pays_uv,
  0                            as gmv,
  cast(0 as int)                            as brand_gmv,
  cast(0 as int)                            as no_brand_gmv,
  0                            as try_num,
  0                            as push_num,
  0                            as success_num
from
  tmp.tmp_push_click_${table_suffix} fpc
inner join
  tmp.tmp_push_click_all_order_${table_suffix} og
on fpc.device_id = og.device_id
where datasource = 'vova'
group by cube (fpc.platform,fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  'vova'                                                 as datasource,
  nvl(fpc.platform, 'all')                               as platform,
  nvl(fpc.region_code, 'all')                            as region_code,
  '1'                                                    as target_type,
  nvl(fpc.config_id, 'all')                              as config_id,
  nvl(fpc.main_channel, 'all')                           as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  0                                                      as push_click_uv,
  0                                                      as impressions,
  0                                                      as impressions_uv,
  0                                                      as impressions_pd,
  0                                                      as impressions_pd_uv,
  0                                                      as impressions_ex_pd,
  0                                                      as impressions_ex_pd_uv,
  0                                                      as carts,
  0                                                      as carts_uv,
  0                                                      as orders,
  0                                                      as orders_uv,
  count(distinct py.order_id)                            as pays,
  count(distinct py.device_id)                            as pays_uv,
  sum(py.shipping_fee + py.goods_number * py.shop_price) as gmv,
  cast(sum(if(py.is_brand = 'Y',py.shipping_fee + py.goods_number * py.shop_price,0)) as int) as brand_gmv,
  cast(sum(if(py.is_brand = 'N',py.shipping_fee + py.goods_number * py.shop_price,0)) as int) as  no_brand_gmv,
  0                                                      as try_num,
  0                                                      as push_num,
  0                                                      as success_num
from
  tmp.tmp_push_click_${table_suffix} fpc
inner join
  tmp.tmp_push_click_all_pay_${table_suffix} py
on fpc.device_id = py.device_id
where datasource = 'vova'
group by cube (fpc.platform, fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
;

CREATE TABLE IF NOT EXISTS tmp.push_click_behavior_v2_result_02_${table_suffix} as
select /*+ REPARTITION(10) */
  tmp.datasource,
  '${cur_date}'                 as push_date,
  tmp.platform,
  tmp.region_code,
  '${interval_flag}h'                as click_interval,
  tmp.target_type,
  'all' r_tag,
  'all' f_tag,
  'all' m_tag,
  'all' is_new,
  tmp.config_id,
  tmp.main_channel,
  sum(tmp.push_click_uv)        as push_click_uv,
  sum(tmp.impressions)          as impressions,
  sum(tmp.impressions_uv)       as impressions_uv,
  sum(tmp.impressions_pd)       as impressions_pd,
  sum(tmp.impressions_pd_uv)    as impressions_pd_uv,
  sum(tmp.impressions_ex_pd)    as impressions_ex_pd,
  sum(tmp.impressions_ex_pd_uv) as impressions_ex_pd_uv,
  sum(tmp.carts)                as carts,
  sum(tmp.carts_uv)             as carts_uv,
  sum(tmp.orders)               as orders,
  sum(tmp.orders_uv)            as orders_uv,
  sum(tmp.pays)                 as pays,
  sum(tmp.pays_uv)              as pays_uv,
  sum(tmp.gmv)                  as gmv,
  sum(tmp.brand_gmv)                  as brand_gmv,
  sum(tmp.no_brand_gmv)                  as no_brand_gmv,
  sum(tmp.try_num)              as try_num,
  sum(tmp.push_num)             as push_num,
  sum(tmp.success_num)          as success_num,
  tmp.job_rate
from
  tmp.push_click_behavior_v2_result_01_${table_suffix} tmp
where tmp.datasource not IN ('NA', 'all')
  and tmp.platform in ('android', 'ios', 'all')
  and tmp.target_type != 'all'
  and tmp.config_id not IN ('NA', 'all')
  and tmp.main_channel != 'Unknown'
group by tmp.datasource, tmp.platform, tmp.target_type,tmp.config_id, tmp.main_channel, tmp.region_code,tmp.job_rate
;

CREATE TABLE IF NOT EXISTS tmp.push_click_behavior_v2_result_03_${table_suffix} as
select /*+ REPARTITION(10) */
  ps.datasource,
  ps.platform,
  ps.region_code,
  ps.target_type,
  ps.config_id,
  ps.main_channel,
  ps.job_rate,
  ps.push_click_uv,
  ps.impressions,
  ps.impressions_uv,
  ps.impressions_pd,
  ps.impressions_pd_uv,
  ps.impressions_ex_pd,
  ps.impressions_ex_pd_uv,
  ps.carts,
  ps.carts_uv,
  ps.orders,
  ps.orders_uv,
  ps.pays,
  ps.pays_uv,
  ps.gmv,
  0 brand_gmv,
  0 no_brand_gmv,
  ps.try_num,
  ps.push_num,
  ps.success_num
from
  tmp.push_stas_${table_suffix} as ps
where datasource in ('airyclub', 'app-group', 'nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
union all
select /*+ REPARTITION(10) */
  nvl(fpc.datasource, 'all')    as datasource,
  nvl(fpc.platform, 'all')      as platform,
  nvl(fpc.region_code, 'all')   as region_code,
  '1'                           as target_type,
  nvl(fpc.config_id, 'all')     as config_id,
  nvl(fpc.main_channel, 'all')  as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  count(distinct fpc.device_id) as push_click_uv,
  0                             as impressions,
  0                             as impressions_uv,
  0                             as impressions_pd,
  0                             as impressions_pd_uv,
  0                             as impressions_ex_pd,
  0                             as impressions_ex_pd_uv,
  0                             as carts,
  0                             as carts_uv,
  0                             as orders,
  0                             as orders_uv,
  0                             as pays,
  0                             as pays_uv,
  0                             as gmv,
  0                             as brand_gmv,
  0                             as no_brand_gmv,
  0                             as try_num,
  0                             as push_num,
  0                             as success_num
from
  tmp.tmp_push_click_${table_suffix} fpc
where datasource in ('airyclub', 'app-group', 'nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
group by cube (fpc.datasource, fpc.platform,fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  nvl(fpc.datasource, 'all')   as datasource,
  nvl(fpc.platform, 'all')     as platform,
  nvl(fpc.region_code, 'all')  as region_code,
  '1'                          as target_type,
  nvl(fpc.config_id, 'all')    as config_id,
  nvl(fpc.main_channel, 'all') as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  0                            as push_click_uv,
  sum(1)                       as impressions,
  count(distinct gi.device_id)  as impressions_uv,
  sum(1)                       as impressions_pd,
  count(distinct gi.pd_dvce)   as impressions_pd_uv,
  0                            as impressions_ex_pd,
  0                            as impressions_ex_pd_uv,
  0                            as carts,
  0                            as carts_uv,
  0                            as orders,
  0                            as orders_uv,
  0                            as pays,
  0                            as pays_uv,
  0                            as gmv,
  0                             as brand_gmv,
  0                             as no_brand_gmv,
  0                            as try_num,
  0                            as push_num,
  0                            as success_num
from
  tmp.tmp_push_click_${table_suffix} as fpc
inner join
  tmp.tmp_push_click_all_imp_${table_suffix} gi
on fpc.device_id = gi.device_id
where datasource in ('airyclub', 'app-group', 'nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
group by cube (fpc.datasource, fpc.platform, fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  nvl(fpc.datasource, 'all')    as datasource,
  nvl(fpc.platform, 'all')      as platform,
  nvl(fpc.region_code, 'all')   as region_code,
  '1'                           as target_type,
  nvl(fpc.config_id, 'all')     as config_id,
  nvl(fpc.main_channel, 'all')  as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  0                             as push_click_uv,
  sum(1)                        as impressions,
  count(distinct gi.device_id)   as impressions_uv,
  0                             as impressions_pd,
  0                             as impressions_pd_uv,
  sum(1)                        as impressions_ex_pd,
  count(distinct gi.ex_pd_dvce) as impressions_ex_pd_uv,
  0                             as carts,
  0                             as carts_uv,
  0                             as orders,
  0                             as orders_uv,
  0                             as pays,
  0                             as pays_uv,
  0                             as gmv,
  0                             as brand_gmv,
  0                             as no_brand_gmv,
  0                             as try_num,
  0                             as push_num,
  0                             as success_num
from
  tmp.tmp_push_click_${table_suffix} as fpc
inner join
  tmp.tmp_push_click_all_pd_${table_suffix} gi
on fpc.device_id = gi.device_id
where datasource in ('airyclub', 'app-group', 'nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
group by cube (fpc.datasource, fpc.platform, fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  nvl(fpc.datasource, 'all')   as datasource,
  nvl(fpc.platform, 'all')     as platform,
  nvl(fpc.region_code, 'all')  as region_code,
  '1'                          as target_type,
  nvl(fpc.config_id, 'all')    as config_id,
  nvl(fpc.main_channel, 'all') as main_channel,
  nvl(fpc.job_rate, 'all')     as job_rate,
  0                            as push_click_uv,
  0                            as impressions,
  0                            as impressions_uv,
  0                            as impressions_pd,
  0                            as impressions_pd_uv,
  0                            as impressions_ex_pd,
  0                            as impressions_ex_pd_uv,
  sum(1)                       as carts,
  count(distinct cc.device_id) as carts_uv,
  0                            as orders,
  0                            as orders_uv,
  0                            as pays,
  0                            as pays_uv,
  0                            as gmv,
  0                             as brand_gmv,
  0                             as no_brand_gmv,
  0                            as try_num,
  0                            as push_num,
  0                            as success_num
from
  tmp.tmp_push_click_${table_suffix} fpc
inner join
  tmp.tmp_push_click_all_clk_${table_suffix} cc
on fpc.device_id = cc.device_id
where datasource in ('airyclub', 'app-group', 'nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
group by cube (fpc.datasource, fpc.platform,  fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  nvl(fpc.datasource, 'all')   as datasource,
  nvl(fpc.platform, 'all')     as platform,
  nvl(fpc.region_code, 'all')  as region_code,
  '1'                          as target_type,
  nvl(fpc.config_id, 'all')    as config_id,
  nvl(fpc.main_channel, 'all') as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  0                            as push_click_uv,
  0                            as impressions,
  0                            as impressions_uv,
  0                            as impressions_pd,
  0                            as impressions_pd_uv,
  0                            as impressions_ex_pd,
  0                            as impressions_ex_pd_uv,
  0                            as carts,
  0                            as carts_uv,
  count(distinct og.order_id)  as orders,
  count(distinct og.device_id)  as orders_uv,
  0                            as pays,
  0                            as pays_uv,
  0                            as gmv,
  0                             as brand_gmv,
  0                             as no_brand_gmv,
  0                            as try_num,
  0                            as push_num,
  0                            as success_num
from
  tmp.tmp_push_click_${table_suffix} fpc
inner join
  tmp.tmp_push_click_all_order_${table_suffix} og
on fpc.device_id = og.device_id
where datasource in ('airyclub', 'app-group', 'nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
group by cube (fpc.datasource, fpc.platform, fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(10) */
  nvl(fpc.datasource, 'all')                             as datasource,
  nvl(fpc.platform, 'all')                               as platform,
  nvl(fpc.region_code, 'all')                            as region_code,
  '1'                                                    as target_type,
  nvl(fpc.config_id, 'all')                              as config_id,
  nvl(fpc.main_channel, 'all')                           as main_channel,
  nvl(fpc.job_rate, 'all')  as job_rate,
  0                                                      as push_click_uv,
  0                                                      as impressions,
  0                                                      as impressions_uv,
  0                                                      as impressions_pd,
  0                                                      as impressions_pd_uv,
  0                                                      as impressions_ex_pd,
  0                                                      as impressions_ex_pd_uv,
  0                                                      as carts,
  0                                                      as carts_uv,
  0                                                      as orders,
  0                                                      as orders_uv,
  count(distinct py.order_id)                            as pays,
  count(distinct py.device_id)                            as pays_uv,
  sum(py.shipping_fee + py.goods_number * py.shop_price) as gmv,
  sum(if(py.is_brand = 'Y',py.shipping_fee + py.goods_number * py.shop_price,0)) as brand_gmv,
  sum(if(py.is_brand = 'N',py.shipping_fee + py.goods_number * py.shop_price,0)) as no_brand_gmv,
  0                                                      as try_num,
  0                                                      as push_num,
  0                                                      as success_num
from
  tmp.tmp_push_click_${table_suffix} fpc
inner join
  tmp.tmp_push_click_all_pay_${table_suffix} py
on fpc.device_id = py.device_id
where datasource in ('airyclub', 'app-group', 'nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
group by cube (fpc.datasource, fpc.platform, fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
;


CREATE TABLE IF NOT EXISTS tmp.push_click_behavior_v2_result_04_${table_suffix} as
select /*+ REPARTITION(10) */
  tmp.datasource,
  '${cur_date}'                 as push_date,
  tmp.platform,
  tmp.region_code,
  '${interval_flag}h'                as click_interval,
  tmp.target_type,
  'all' r_tag,
  'all' f_tag,
  'all' m_tag,
  'all' is_new,
  tmp.config_id,
  tmp.main_channel,
  sum(tmp.push_click_uv)        as push_click_uv,
  sum(tmp.impressions)          as impressions,
  sum(tmp.impressions_uv)       as impressions_uv,
  sum(tmp.impressions_pd)       as impressions_pd,
  sum(tmp.impressions_pd_uv)    as impressions_pd_uv,
  sum(tmp.impressions_ex_pd)    as impressions_ex_pd,
  sum(tmp.impressions_ex_pd_uv) as impressions_ex_pd_uv,
  sum(tmp.carts)                as carts,
  sum(tmp.carts_uv)             as carts_uv,
  sum(tmp.orders)               as orders,
  sum(tmp.orders_uv)            as orders_uv,
  sum(tmp.pays)                 as pays,
  sum(tmp.pays_uv)              as pays_uv,
  sum(tmp.gmv)                  as gmv,
  sum(tmp.brand_gmv)                  as brand_gmv,
  sum(tmp.no_brand_gmv)                  as no_brand_gmv,
  sum(tmp.try_num)              as try_num,
  sum(tmp.push_num)             as push_num,
  sum(tmp.success_num)          as success_num,
  tmp.job_rate
from
  tmp.push_click_behavior_v2_result_03_${table_suffix} tmp
where tmp.datasource IN ('airyclub', 'app-group', 'nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
  and tmp.platform in ('android', 'ios', 'all')
  and tmp.target_type != 'all'
  and tmp.config_id not IN ('NA', 'all')
  and tmp.main_channel != 'Unknown'
group by tmp.datasource, tmp.platform, tmp.target_type,tmp.config_id, tmp.main_channel, tmp.region_code,tmp.job_rate
;

insert overwrite table dwb.dwb_vova_push_click_behavior_v2 PARTITION (pt = '${cur_date}', intervals = '${interval_flag}h')
select /*+ REPARTITION(10) */
  datasource,
  push_date,
  platform,
  region_code,
  click_interval,
  target_type,
  r_tag,
  f_tag,
  m_tag,
  is_new,
  config_id,
  main_channel,
  push_click_uv,
  impressions,
  impressions_uv,
  impressions_pd,
  impressions_pd_uv,
  impressions_ex_pd,
  impressions_ex_pd_uv,
  carts,
  carts_uv,
  orders,
  orders_uv,
  pays,
  pays_uv,
  gmv,
  try_num,
  push_num,
  success_num,
  job_rate,
  brand_gmv,
  no_brand_gmv
from
  tmp.push_click_behavior_v2_result_02_${table_suffix}
union all
select /*+ REPARTITION(10) */
  datasource,
  push_date,
  platform,
  region_code,
  click_interval,
  target_type,
  r_tag,
  f_tag,
  m_tag,
  is_new,
  config_id,
  main_channel,
  push_click_uv,
  impressions,
  impressions_uv,
  impressions_pd,
  impressions_pd_uv,
  impressions_ex_pd,
  impressions_ex_pd_uv,
  carts,
  carts_uv,
  orders,
  orders_uv,
  pays,
  pays_uv,
  gmv,
  try_num,
  push_num,
  success_num,
  job_rate,
  brand_gmv,
  no_brand_gmv
from
  tmp.push_click_behavior_v2_result_04_${table_suffix}
;

DROP TABLE IF EXISTS tmp.tmp_push_logs_${table_suffix};
DROP TABLE IF EXISTS tmp.push_stas_${table_suffix};
DROP TABLE IF EXISTS tmp.tmp_push_click_${table_suffix};
DROP TABLE IF EXISTS tmp.tmp_push_click_all_${table_suffix};
DROP TABLE IF EXISTS tmp.tmp_push_click_all_imp_${table_suffix};
DROP TABLE IF EXISTS tmp.tmp_push_click_all_pd_${table_suffix};
DROP TABLE IF EXISTS tmp.tmp_push_click_all_clk_${table_suffix};
DROP TABLE IF EXISTS tmp.tmp_push_click_all_order_${table_suffix};
DROP TABLE IF EXISTS tmp.tmp_push_click_all_pay_${table_suffix};
DROP TABLE IF EXISTS tmp.push_click_behavior_v2_result_01_${table_suffix};
DROP TABLE IF EXISTS tmp.push_click_behavior_v2_result_02_${table_suffix};
DROP TABLE IF EXISTS tmp.push_click_behavior_v2_result_03_${table_suffix};
DROP TABLE IF EXISTS tmp.push_click_behavior_v2_result_04_${table_suffix};
"


spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism=380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`








