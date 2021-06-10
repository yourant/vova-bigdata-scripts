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

job_name="dwb_vova_push_click_behavior_v2_chenkai_${cur_date}"

sql="
CREATE TABLE IF NOT EXISTS tmp.tmp_push_logs_${table_suffix} as
SELECT /*+ REPARTITION(100) */
  vapl.user_id                          as buyer_id,
  nvl(dev.region_code, 'NA')            as region_code,
  vapl.switch_on                        as switch_on,
  vapl.push_result                      as push_result,
  vapl.platform                         as platform,
  nvl(vaptc.app_platform, 'NA')         as datasource,
  cast(vapl.task_config_id as string)   as config_id,
  vapl.push_time                        as push_time,
  nvl(dev.main_channel, 'Unknown')      as main_channel,
  case when vaptc.expected_period = 0 then 'one_day_once' -- 单日单次
    when vaptc.expected_period = 20 then 'daily_cycle' -- 每日循环
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
where vapl.pt = '${cur_date}'
;

create table IF NOT EXISTS tmp.tmp_push_click_${table_suffix} as
select /*+ REPARTITION(10) */ distinct
  fpc.datasource datasource,
  nvl(dev.region_code, 'NA') region_code,
  nvl(fpc.platform, 'NA')          AS platform,
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
  flcc.session_id
from
(
  select distinct
    case when datasource = 'vova' then 'vova'
      when datasource = 'airyclub' then 'airyclub'
      else 'app-group'
      end datasource,
    nvl(platform, 'NA')          AS platform,
    nvl(config_id, 'NA')         AS config_id,
    click_time                   AS click_time,
    push_time,
    time_zone,
    device_id
  from
    dwd.dwd_vova_fact_push_click
  where date(push_time) = '${cur_date}'
    and device_id is not null
    and datasource in (select distinct data_domain from ods_vova_vtsf.ods_vova_acg_app)
  -- 需要单独再算站群的
  union all
  select distinct
    datasource datasource,
    nvl(platform, 'NA')          AS platform,
    nvl(config_id, 'NA')         AS config_id,
    click_time                   AS click_time,
    push_time,
    time_zone,
    device_id
  from
    dwd.dwd_vova_fact_push_click
  where date(push_time) = '${cur_date}'
    and device_id is not null
    and datasource in ('nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')
) fpc -- 所有的 加 部分站群的
left join
  dim.dim_vova_devices dev
on dev.device_id = fpc.device_id and dev.datasource = fpc.datasource
join
(
  select
    datasource,
    device_id,
    session_id
  from
    dwd.dwd_vova_log_common_click a
  join
    ods_vova_vtp.ods_vova_app_push_task b
  on split(a.element_id, '@')[0] = b.id
  where a.pt = '${cur_date}'
    and a.element_name = 'PushUserDecide'
    and a.element_id like '%@%'
  group by datasource, device_id, session_id
) flcc -- 推送点击打点
on fpc.device_id = flcc.device_id and fpc.datasource = flcc.datasource
left join
  ods_vova_vtp.ods_vova_app_push_task_config vaptc
on fpc.config_id = vaptc.id
where date(fpc.push_time) = '${cur_date}'
  and fpc.device_id is not null
  and fpc.datasource in (select distinct data_domain from ods_vova_vtsf.ods_vova_acg_app)
  and unix_timestamp(fpc.click_time) - unix_timestamp(fpc.push_time) + cast(fpc.time_zone as bigint) * 3600 < 24 * 3600
;

-- 明细
create table IF NOT EXISTS tmp.tmp_push_result_log_${table_suffix} as
-- 尝试推送,上传成功量,推送成功 明细
select /*+ REPARTITION(70) */
  case when datasource in ('vova','airyclub') then datasource
    else 'app-group'
  end datasource,
  nvl(region_code, 'NA') region_code,
  nvl(platform, 'NA') platform,
  nvl(config_id, 'NA') config_id,
  nvl(main_channel, 'NA') main_channel,
  nvl(job_rate, 'NA') job_rate,

  1 try_num,
  if(push_result = 1, 1, 0) push_num,
  if(push_result = 1 and switch_on = 1, 1, 0) success_num,
  null push_click_device,
  null impressions_pv,
  null impressions_device,
  null impressions_pd_pv,
  null impressions_pd_device,
  null impressions_ex_pd_pv,
  null impressions_ex_pd_device,
  null carts               ,
  null carts_device        ,
  null orders              ,
  null orders_device       ,
  null pays                ,
  null pays_device         ,
  null gmv                 ,
  null brand_gmv           ,
  null no_brand_gmv        ,
  null session_gmv         ,
  null session_brand_gmv   ,
  null session_no_brand_gmv
from
  tmp.tmp_push_logs_${table_suffix}
union all
select /*+ REPARTITION(1) */
  nvl(datasource, 'NA')datasource,
  nvl(region_code, 'NA') region_code,
  nvl(platform, 'NA') platform,
  nvl(config_id, 'NA') config_id,
  nvl(main_channel, 'NA') main_channel,
  nvl(job_rate, 'NA') job_rate,

  1 try_num,
  if(push_result = 1, 1, 0) push_num,
  if(push_result = 1 and switch_on = 1, 1, 0) success_num,
  null push_click_device,
  null impressions_pv,
  null impressions_device,
  null impressions_pd_pv,
  null impressions_pd_device,
  null impressions_ex_pd_pv,
  null impressions_ex_pd_device,
  null carts               ,
  null carts_device        ,
  null orders              ,
  null orders_device       ,
  null pays                ,
  null pays_device         ,
  null gmv                 ,
  null brand_gmv           ,
  null no_brand_gmv        ,
  null session_gmv         ,
  null session_brand_gmv   ,
  null session_no_brand_gmv
from
  tmp.tmp_push_logs_${table_suffix}
where datasource in ('nurkk', 'kulmasa', 'lupumart', 'boonlife', 'paivana')

union all -- 曝光
select /*+ REPARTITION(10) */
  nvl(fpc.datasource, 'NA') datasource,,
  nvl(fpc.region_code, 'NA') region_code,
  nvl(fpc.platform, 'NA') platform,
  nvl(fpc.config_id, 'NA') config_id,
  nvl(fpc.main_channel, 'NA') main_channel,
  nvl(fpc.job_rate, 'NA') job_rate,

  null try_num,
  null push_num,
  null success_num,
  null push_click_device,

  if(gi.device_id is not null
    and gi.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
    and gi.collector_tstamp / 1000 - 24 * 3600 < unix_timestamp(fpc.click_time), 1, null) impressions_pv,
  if(gi.device_id is not null
    and gi.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
    and gi.collector_tstamp / 1000 - 24 * 3600 < unix_timestamp(fpc.click_time), gi.device_id, null) impressions_device,

  if(gi.page_code = 'product_detail' and gi.device_id is not null
    and gi.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
    and gi.collector_tstamp / 1000 - 24 * 3600 < unix_timestamp(fpc.click_time), 1, null) impressions_pd_pv,
  if(gi.page_code = 'product_detail' and gi.device_id is not null
    and gi.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
    and gi.collector_tstamp / 1000 - 24 * 3600 < unix_timestamp(fpc.click_time), gi.device_id, null) impressions_pd_device,

  if(gi.page_code != 'product_detail' and gi.device_id is not null
    and gi.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
    and gi.collector_tstamp / 1000 - 24 * 3600 < unix_timestamp(fpc.click_time), 1, null) impressions_ex_pd_pv,
  if(gi.page_code != 'product_detail' and gi.device_id is not null
    and gi.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
    and gi.collector_tstamp / 1000 - 24 * 3600 < unix_timestamp(fpc.click_time), gi.device_id, null) impressions_ex_pd_device,

  null carts               ,
  null carts_device        ,
  null orders              ,
  null orders_device       ,
  null pays                ,
  null pays_device         ,
  null gmv                 ,
  null brand_gmv           ,
  null no_brand_gmv        ,
  null session_gmv         ,
  null session_brand_gmv   ,
  null session_no_brand_gmv
from
  tmp.tmp_push_click_${table_suffix} fpc -- 推送点击
inner join
(
  select
    gi.datasource,
    gi.device_id,
    gi.buyer_id,
    gi.collector_tstamp,
    gi.device_id,
    gi.page_code
  from
    dwd.dwd_vova_log_goods_impression gi
  where gi.pt >= '${cur_date}'
    and gi.pt <= date_add('${cur_date}', 1)
) gi -- 曝光
on fpc.device_id = gi.device_id and fpc.datasource = gi.datasource

union all -- 加购
select /*+ REPARTITION(1) */
  nvl(fpc.datasource, 'NA') datasource,,
  nvl(fpc.region_code, 'NA') region_code,
  nvl(fpc.platform, 'NA') platform,
  nvl(fpc.config_id, 'NA') config_id,
  nvl(fpc.main_channel, 'NA') main_channel,
  nvl(fpc.job_rate, 'NA') job_rate,

  null try_num,
  null push_num,
  null success_num,
  null push_click_device       ,
  null impressions_pv          ,
  null impressions_device      ,
  null impressions_pd_pv       ,
  null impressions_pd_device   ,
  null impressions_ex_pd_pv    ,
  null impressions_ex_pd_device,
  if(cc.device_id is not null and cc.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
    and cc.collector_tstamp / 1000 - 24 * 3600 < unix_timestamp(fpc.click_time), 1, null) carts,
  if(cc.device_id is not null and cc.collector_tstamp / 1000 > unix_timestamp(fpc.click_time)
    and cc.collector_tstamp / 1000 - 24 * 3600 < unix_timestamp(fpc.click_time), cc.device_id, null) carts_device,
  null orders              ,
  null orders_device       ,
  null pays                ,
  null pays_device         ,
  null gmv                 ,
  null brand_gmv           ,
  null no_brand_gmv        ,
  null session_gmv         ,
  null session_brand_gmv   ,
  null session_no_brand_gmv
from
  tmp.tmp_push_click_${table_suffix} fpc -- 推送点击
inner join
(
  select
    cc.datasource,
    cc.device_id,
    cc.collector_tstamp
  from
    dwd.dwd_vova_log_common_click cc
  where cc.pt >= '${cur_date}'
    and cc.pt <= date_add('${cur_date}', 1)
    and cc.element_name = 'pdAddToCartClick'
) cc -- 加购
on fpc.device_id = cc.device_id and fpc.datasource = cc.datasource

union all -- 下单
select /*+ REPARTITION(1) */
  nvl(fpc.datasource, 'NA') datasource,,
  nvl(fpc.region_code, 'NA') region_code,
  nvl(fpc.platform, 'NA') platform,
  nvl(fpc.config_id, 'NA') config_id,
  nvl(fpc.main_channel, 'NA') main_channel,
  nvl(fpc.job_rate, 'NA') job_rate,

  null try_num,
  null push_num,
  null success_num,
  null push_click_device       ,
  null impressions_pv          ,
  null impressions_device      ,
  null impressions_pd_pv       ,
  null impressions_pd_device   ,
  null impressions_ex_pd_pv    ,
  null impressions_ex_pd_device,
  null carts                   ,
  null carts_device            ,
  if(og.order_time > fpc.click_time
    and unix_timestamp(og.order_time) - 24 * 3600 < unix_timestamp(fpc.click_time), og.order_id, null) orders,
  if(og.order_time > fpc.click_time
    and unix_timestamp(og.order_time) - 24 * 3600 < unix_timestamp(fpc.click_time), og.device_id, null) order_device,
  null pays                ,
  null pays_device         ,
  null gmv                 ,
  null brand_gmv           ,
  null no_brand_gmv        ,
  null session_gmv         ,
  null session_brand_gmv   ,
  null session_no_brand_gmv
from
  tmp.tmp_push_click_${table_suffix} fpc -- 推送点击
inner join
  dim.dim_vova_order_goods og  -- 下单数
on fpc.device_id = og.device_id and fpc.datasource = og.datasource
where og.order_time > fpc.click_time
  and unix_timestamp(og.order_time) - 24 * 3600 < unix_timestamp(fpc.click_time)

union all -- 支付
select /*+ REPARTITION(1) */
  nvl(fpc.datasource, 'NA') datasource,,
  nvl(fpc.region_code, 'NA') region_code,
  nvl(fpc.platform, 'NA') platform,
  nvl(fpc.config_id, 'NA') config_id,
  nvl(fpc.main_channel, 'NA') main_channel,
  nvl(fpc.job_rate, 'NA') job_rate,

  null try_num                 ,
  null push_num                ,
  null success_num             ,
  nvl(fpc.device_id, null) push_click_device       ,
  null impressions_pv          ,
  null impressions_device      ,
  null impressions_pd_pv       ,
  null impressions_pd_device   ,
  null impressions_ex_pd_pv    ,
  null impressions_ex_pd_device,
  null carts                   ,
  null carts_device            ,
  null orders                  ,
  null order_device            ,

  if(fp.pay_time > fpc.click_time
    and unix_timestamp(fp.pay_time) - 24 * 3600 < unix_timestamp(fpc.click_time), fp.order_id, null) pays,
  if(fp.pay_time > fpc.click_time
    and unix_timestamp(fp.pay_time) - 24 * 3600 < unix_timestamp(fpc.click_time), fp.device_id, null) pays_device,
  if(fp.pay_time > fpc.click_time
    and unix_timestamp(fp.pay_time) - 24 * 3600 < unix_timestamp(fpc.click_time), fp.gmv, null) gmv,
  if(fp.pay_time > fpc.click_time
    and unix_timestamp(fp.pay_time) - 24 * 3600 < unix_timestamp(fpc.click_time)
    and fp.brand_id > 0, fp.gmv, null) brand_gmv,
  if(fp.pay_time > fpc.click_time
    and unix_timestamp(fp.pay_time) - 24 * 3600 < unix_timestamp(fpc.click_time)
    and fp.brand_id = 0, fp.gmv, null) no_brand_gmv,
  if(fp.pre_session_id is not null and fpc.session_id = fp.pre_session_id, fp.gmv, null) session_gmv,
  if(fp.pre_session_id is not null and fpc.session_id = fp.pre_session_id and fp.brand_id > 0, fp.gmv, null) session_brand_gmv,
  if(fp.pre_session_id is not null and fpc.session_id = fp.pre_session_id and fp.brand_id = 0, fp.gmv, null) session_no_brand_gmv
from
  tmp.tmp_push_click_${table_suffix} fpc -- 推送点击
left join
(
  select
    py.datasource,
    py.device_id,
    py.order_id,
    py.buyer_id,
    py.shipping_fee,
    py.goods_number,
    py.shop_price,
    py.shipping_fee + py.goods_number * py.shop_price gmv,
    foc2.pre_session_id,
    dg.brand_id,
    py.order_id,
    py.order_goods_id,
    py.pay_time
  from
    dwd.dwd_vova_fact_pay py
  inner join
    dim.dim_vova_goods dg
  on py.goods_id = dg.goods_id
  left join
  (
    select distinct
      datasource,
      order_goods_id,
      pre_session_id
    from
      dwd.dwd_vova_fact_order_cause_v2
    where pt='${cur_date}'
  ) foc2
  on py.order_goods_id = foc2.order_goods_id and py.datasource = foc2.datasource
  where py.pay_time >= '${cur_date}'
) fp -- 支付订单 及 gmv
on fpc.device_id = fp.device_id and fpc.datasource = fp.datasource
where fp.pay_time > fpc.click_time
  and unix_timestamp(fp.pay_time) - 24 * 3600 < unix_timestamp(fpc.click_time)
;

insert overwrite table dwb.dwb_vova_push_click_behavior partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  nvl(fpc.datasource, 'all')   as datas,
  nvl(fpc.platform, 'all')     as platform,
  nvl(fpc.region_code, 'all')  as region_code,
  nvl(fpc.config_id, 'all')    as config_id,
  nvl(fpc.main_channel, 'all') as main_channel,
  nvl(fpc.job_rate, 'all')     as job_rate,

  sum(try_num)     try_num,
  sum(push_num)    push_num,
  sum(success_num) success_num,
  count(distinct push_click_device)         push_click_uv      ,
  sum(impressions_pv)                       impressions_pv      ,
  count(distinct impressions_device)        impressions_uv      ,
  sum(impressions_pd_pv)                    impressions_pd_pv   ,
  count(distinct impressions_pd_device)     impressions_pd_uv   ,
  sum(impressions_ex_pd_pv)                 impressions_ex_pd_pv,
  count(distinct impressions_ex_pd_device)  impressions_ex_pd_uv,
  sum(carts)                    carts               ,
  count(distinct carts_device)  carts_uv            ,
  count(distinct orders)                   orders              ,
  count(distinct orders_device) orders_uv           ,
  count(distinct pays)                     pays                ,
  count(distinct pays_device)   pays_uv             ,
  sum(gmv)                      gmv                 ,
  sum(brand_gmv)                brand_gmv           ,
  sum(no_brand_gmv)             no_brand_gmv        ,
  sum(session_gmv)              session_gmv         ,
  sum(session_brand_gmv)        session_brand_gmv   ,
  sum(session_no_brand_gmv)     session_no_brand_gmv
from
  tmp.tmp_push_result_log_${table_suffix} fpc
where datasource in ('vova', 'airyclub', 'app-group')
group by cube (fpc.datasource, fpc.platform,fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
union all
select /*+ REPARTITION(1) */
  nvl(fpc.datasource, 'all')   as datas,
  nvl(fpc.platform, 'all')     as platform,
  nvl(fpc.region_code, 'all')  as region_code,
  nvl(fpc.config_id, 'all')    as config_id,
  nvl(fpc.main_channel, 'all') as main_channel,
  nvl(fpc.job_rate, 'all')     as job_rate,

  sum(try_num)     try_num,
  sum(push_num)    push_num,
  sum(success_num) success_num,
  count(distinct push_click_device)         push_click_uv       ,
  sum(impressions_pv)                       impressions_pv      ,
  count(distinct impressions_device)        impressions_uv      ,
  sum(impressions_pd_pv)                    impressions_pd_pv   ,
  count(distinct impressions_pd_device)     impressions_pd_uv   ,
  sum(impressions_ex_pd_pv)                 impressions_ex_pd_pv,
  count(distinct impressions_ex_pd_device)  impressions_ex_pd_uv,
  sum(carts)                    carts               ,
  count(distinct carts_device)  carts_uv            ,
  count(distinct orders)        orders              ,
  count(distinct orders_device) orders_uv           ,
  count(distinct pays)          pays                ,
  count(distinct pays_device)   pays_uv             ,
  sum(gmv)                      gmv                 ,
  sum(brand_gmv)                brand_gmv           ,
  sum(no_brand_gmv)             no_brand_gmv        ,
  sum(session_gmv)              session_gmv         ,
  sum(session_brand_gmv)        session_brand_gmv   ,
  sum(session_no_brand_gmv)     session_no_brand_gmv
from
  tmp.tmp_push_result_log_${table_suffix} fpc
where datasource not in ('vova', 'airyclub', 'app-group')
group by cube (fpc.datasource, fpc.platform,fpc.config_id, fpc.main_channel, fpc.region_code,fpc.job_rate)
having datas != 'all'
;

drop table if EXISTS tmp.tmp_push_logs_${table_suffix};
drop table if EXISTS tmp.tmp_push_click_${table_suffix};
drop table if EXISTS tmp.tmp_push_result_log_${table_suffix};
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

