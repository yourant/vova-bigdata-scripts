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

job_name="dwb_vova_markdown_goods_daily_req5319_chenkai_${cur_date}"

###逻辑sql
sql="
insert OVERWRITE TABLE dwb.dwb_vova_markdown_goods_daily PARTITION (pt='${cur_date}')
select
  /*+ REPARTITION(1) */
  'vova' datasource,
  tmp1.region_code region_code,
  tmp1.platform platform,
  nvl(tmp1.impression_goods_cnt, 0) impression_goods_cnt, -- 会场曝光商品总数量
  nvl(tmp2.pay_goods_cnt, 0) pay_goods_cnt, -- 售卖成功商品数量
  nvl(impression_low_price_original_goods_cnt, 0) impression_low_price_original_goods_cnt, -- 曝光商品数量中合格id商品数量
  nvl(impression_ac_top_goods_cnt, 0) impression_ac_top_goods_cnt, -- 曝光商品数量中AEtop款商品数量
  nvl(pay_low_price_original_goods_cnt, 0) pay_low_price_original_goods_cnt, -- 售卖成功商品数量中合格id商品数量
  nvl(pay_ac_top_goods_cnt, 0) pay_ac_top_goods_cnt, -- 售卖成功商品数量中AEtop款商品数量
  nvl(gmv, 0) gmv, -- 会场GMV
  nvl(low_price_original_gmv, 0) low_price_original_gmv, -- 合格id商品GMV
  nvl(ae_top_gmv, 0) ae_top_gmv -- AEtop款商品GMV
from
(
  select
    nvl(geo_country,'all') region_code,
    nvl(os_type,'all') platform,
    count(distinct(flgi.virtual_goods_id)) impression_goods_cnt, -- 会场曝光商品总数量
    count(distinct(if(vavgr.activity_select_name = 'low_price_original',flgi.virtual_goods_id, null))) impression_low_price_original_goods_cnt, --   曝光商品数量中合格id商品数量
    count(distinct(if(vavgr.activity_select_name = 'ae_top_1000',flgi.virtual_goods_id, null))) impression_ac_top_goods_cnt -- 曝光商品数量中AEtop款商品数量
  from
    dwd.dwd_vova_log_goods_impression flgi
  LEFT JOIN
    dim.dim_vova_goods dd
  on flgi.datasource = dd.datasource and flgi.virtual_goods_id = dd.virtual_goods_id
  LEFT JOIN
    (select *
      from ods_vova_vts.ods_vova_activity_valid_goods_record
      where activity_select_name in ('ae_top_1000', 'low_price_original')
        and '${cur_date}' >= to_date(start_time) and '${cur_date}' <= to_date(end_time)
    ) vavgr
  on dd.goods_id = vavgr.goods_id
  where pt = '${cur_date}' and page_code in ('markdown_homepage','markdown_under','markdown_selection')
    and flgi.datasource = 'vova' and platform='mob' and geo_country is not null
  group by cube(geo_country, os_type)
) tmp1
left join
(
  select
    nvl(dog.region_code, 'all') region_code,
    nvl(foc2.platform, 'all') platform,
    count(distinct(foc2.goods_id)) pay_goods_cnt, -- 售卖成功商品数量
    count(distinct(if(vavgr.activity_select_name = 'low_price_original',foc2.goods_id, null))) pay_low_price_original_goods_cnt, -- 售卖成功商品数量中合格id商品数量
    count(distinct(if(vavgr.activity_select_name = 'ae_top_1000',foc2.goods_id, null))) pay_ac_top_goods_cnt, -- 售卖成功商品数量中AEtop款商品数量
    sum(shipping_fee+shop_price*goods_number) gmv, -- 会场GMV
    sum(if(vavgr.activity_select_name = 'low_price_original', shipping_fee+shop_price*goods_number, 0)) low_price_original_gmv, -- 合格id商品GMV
    sum(if(vavgr.activity_select_name = 'ae_top_1000', shipping_fee+shop_price*goods_number, 0)) ae_top_gmv -- AEtop款商品GMV
  from
    dwd.dwd_vova_fact_order_cause_v2 foc2
  left join
    dim.dim_vova_order_goods dog
  on foc2.datasource = dog.datasource and foc2.order_goods_id = dog.order_goods_id
  LEFT JOIN
  (
    select *
    from ods_vova_vts.ods_vova_activity_valid_goods_record
    where activity_select_name in ('ae_top_1000', 'low_price_original')
      and '${cur_date}' >= to_date(start_time) and '${cur_date}' <= to_date(end_time)
  ) vavgr
  on dog.goods_id = vavgr.goods_id
  where foc2.pt='${cur_date}' and pre_page_code in ('markdown_homepage','markdown_under','markdown_selection')
    and foc2.datasource = 'vova' and foc2.platform in ('android','ios') and dog.pay_status >= 1 and foc2.device_id is not null
  group by cube(dog.region_code, foc2.platform)
) tmp2
on tmp1.region_code = tmp2.region_code and tmp1.platform = tmp2.platform
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`


job2_name="dwb_vova_markdown_order_daily_tmp_req5319_chenkai_${cur_date}"
###逻辑sql
sql2="
create table if not EXISTS tmp.tmp_markdown_order_goods_${table_suffix} as
select
/*+ REPARTITION(1) */
foc2.pt pt,
dog.region_code region_code,
foc2.platform platform,
foc2.device_id device_id,
dog.order_id order_id,
dog.order_goods_id order_goods_id,
if(dog.pay_status >= 1, dog.shipping_fee+dog.shop_price*dog.goods_number, 0) markdown_order_goods_gmv,
if(dog.pay_status >= 1, foc2.device_id, null) markdown_pay_device_id,
if(dd.first_pay_time < foc2.pt, foc2.device_id, null) not_first_pay_device_id,
foc2_his.device_id not_markdown_first_pay_device_id
from
dwd.dwd_vova_fact_order_cause_v2 foc2
left join
dim.dim_vova_order_goods dog
on foc2.datasource = dog.datasource and foc2.order_goods_id = dog.order_goods_id
left join
dim.dim_vova_devices dd
on foc2.device_id = dd.device_id and foc2.datasource = dd.datasource
left join
(
  select
    distinct
    tmp1.pt,
    foc2.datasource,
    foc2.device_id
  from
  (
    select distinct pt
    from dwd.dwd_vova_fact_order_cause_v2
    where pt <='${cur_date}' and pt >= date_sub('${cur_date}', 1)
  ) tmp1
  left join
    dwd.dwd_vova_fact_order_cause_v2 foc2
  on tmp1.pt > foc2.pt
  left join
    dim.dim_vova_order_goods dog
  on foc2.datasource = dog.datasource and foc2.order_goods_id = dog.order_goods_id
  where foc2.datasource = 'vova' and foc2.platform in ('android','ios')
    and foc2.pre_page_code in ('markdown_homepage','markdown_under','markdown_selection')
    and foc2.device_id is not null and dog.pay_status >= 1
) foc2_his
on foc2_his.datasource = foc2.datasource and foc2_his.device_id = foc2.device_id and foc2_his.pt = foc2.pt
where foc2.datasource = 'vova' and foc2.platform in ('android','ios')
  and foc2.pt <='${cur_date}' and foc2.pt >= date_sub('${cur_date}', 1)
  and foc2.pre_page_code in ('markdown_homepage','markdown_under','markdown_selection')
  and foc2.device_id is not null
;

"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job2_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql2"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job2_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`


job2_name="dwb_vova_markdown_order_daily_req5319_chenkai_${cur_date}"
###逻辑sql
sql2="
set Spark.hadoop.hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwb.dwb_vova_markdown_order_daily PARTITION (pt)
select
/*+ REPARTITION(1) */
'vova',
t1.region_code,
t1.platform,
t1.markdown_impression_uv, -- 低价会场曝光UV
t1.morrow_markdown_impression_uv, -- 低价会场次日曝光uv
t2.dau,  -- 主流程DAU
t3.gmv,  -- 主流程GMV
t4.markdown_order_gmv,  -- 会场订单GMV
t5.markdown_order_goods_gmv, -- 会场商品GMV
t5.markdown_order_uv, -- 会场下单uv
t5.markdown_pay_uv, -- 会场支付成功uv
t5.not_markdown_first_pay_uv, -- 当日非首次支付uv
t1.pt
from
(
  select
  nvl(tmp1.pt, 'all') pt,
  nvl(tmp1.geo_country, 'all') region_code,
  nvl(tmp1.os_type, 'all') platform,
  count(distinct(tmp1.device_id)) markdown_impression_uv, -- 低价专区首页UV
  count(distinct(if(tmp2.device_id is not null, tmp1.device_id, null))) morrow_markdown_impression_uv -- 低价专区首页次日UV
  from
  (
    select
    distinct pt, geo_country, os_type, device_id
    from dwd.dwd_vova_log_screen_view flsv
    where flsv.datasource ='vova' and flsv.os_type in ('android','ios')
    and flsv.pt <= '${cur_date}' and flsv.pt >= date_sub('${cur_date}', 1)
    and flsv.device_id is not null and geo_country is not null
    and flsv.page_code = 'markdown_homepage'
  ) tmp1
  left join
  (
    select
    distinct pt, geo_country, os_type, device_id
    from dwd.dwd_vova_log_screen_view flsv
    where flsv.datasource ='vova' and flsv.os_type in ('android','ios')
    and flsv.pt <= date_sub('${cur_date}', -1) and flsv.pt >= '${cur_date}'
    and flsv.device_id is not null and geo_country is not null
    and flsv.page_code = 'markdown_homepage'
  ) tmp2
  on datediff(tmp2.pt, tmp1.pt) = 1
    and tmp2.os_type = tmp1.os_type and tmp2.device_id = tmp1.device_id
    and tmp2.geo_country = tmp1.geo_country
  group by cube(tmp1.pt, tmp1.os_type, tmp1.geo_country)
  HAVING pt !='all'
) t1
left join
(
  select
  nvl(flsv.pt, 'all') pt,
  nvl(flsv.geo_country, 'all') region_code,
  nvl(flsv.os_type, 'all') platform,
  count(distinct(flsv.device_id)) dau --DAU
  from dwd.dwd_vova_log_screen_view flsv
  where flsv.datasource ='vova' and flsv.os_type in ('android','ios')
    and flsv.pt <= '${cur_date}' and flsv.pt >= date_sub('${cur_date}', 1)
    and flsv.device_id is not null and geo_country is not null
  group by cube(flsv.pt, flsv.geo_country, flsv.os_type)
  HAVING pt !='all'
) t2
on t1.pt = t2.pt and t1.region_code = t2.region_code and t1.platform = t2.platform
left join
(
  select
  nvl(to_date(dog.pay_time), 'all') pt,
  nvl(dog.region_code, 'all') region_code,
  nvl(dog.platform, 'all') platform,
  sum(dog.shipping_fee+dog.shop_price*dog.goods_number) gmv -- GMV
  from dim.dim_vova_order_goods dog
  where dog.datasource = 'vova' and dog.platform in ('android','ios')
    and to_date(dog.pay_time) <='${cur_date}' and to_date(dog.pay_time) >= date_sub('${cur_date}', 1)
    and device_id is not null and pay_status >= 1
  group by cube(to_date(dog.pay_time), dog.region_code, dog.platform)
  HAVING pt != 'all'
) t3
on t1.pt = t3.pt and t1.region_code = t3.region_code and t1.platform = t3.platform
left join
(
  select
  nvl(tmp1.pt, 'all') pt,
  nvl(dog.region_code, 'all') region_code,
  nvl(dog.platform, 'all') platform,
  sum(dog.shipping_fee+dog.shop_price*dog.goods_number) markdown_order_gmv -- 会场订单GMV
  from
  (
    select
    distinct pt, order_id
    from
    tmp.tmp_markdown_order_goods_${table_suffix}
    where markdown_pay_device_id is not null
  ) tmp1
  left join
  dim.dim_vova_order_goods dog
  on tmp1.order_id = dog.order_id
  group by cube(tmp1.pt, dog.region_code, dog.platform)
  HAVING pt != 'all'
) t4
on t1.pt = t4.pt and t1.region_code = t4.region_code and t1.platform = t4.platform
left join
(
  select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all') platform,
  sum(markdown_order_goods_gmv) markdown_order_goods_gmv, -- 会场商品GMV
  count(distinct(device_id)) markdown_order_uv, -- 低价会场订单gmv
  count(distinct(markdown_pay_device_id)) markdown_pay_uv, -- 会场支付成功uv
  count(distinct(not_first_pay_device_id)) not_first_pay_uv, -- 当日非首次支付uv
  count(distinct(not_markdown_first_pay_device_id)) not_markdown_first_pay_uv -- 当日非低价会场首次支付uv
  from
  tmp.tmp_markdown_order_goods_${table_suffix}
  group by cube(pt, region_code, platform)
  HAVING pt != 'all'
) t5
on t1.pt = t5.pt and t1.region_code = t5.region_code and t1.platform = t5.platform
;

drop table if EXISTS tmp.tmp_markdown_order_goods_${table_suffix};
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job2_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql2"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job2_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`


job3_name="rpt_markdown_goods_sales_req5319_chenkai_${cur_date}"
###逻辑sql
sql3="
insert OVERWRITE TABLE dwb.dwb_vova_markdown_goods_sales PARTITION (pt='${cur_date}')
select /*+ REPARTITION(1) */
  t1.datasource,
  'all' region_code,
  'all' platform,
  t1.virtual_goods_id,
  dg.goods_sn, -- GSN
  dm.mct_name, -- 店铺名称
  if(vavgr.activity_select_name is not null, vavgr.activity_select_name, vavgr_replace.activity_select_name) activity_select_name, -- 商品来源:(low_price_original)合格id/(ae_top_1000)AEtop款
  dg.first_cat_name, -- 一级品类名称
  dg.second_cat_name, -- 二级品类名称
  if(vavgr.activity_select_name ='low_price_original', vavgr.start_time, null) activity_start_time, -- 入选时间:合格id的活动时间
  dg.shipping_fee+dg.shop_price selling_price,-- 售价
  markdown_impression_cnt, -- 会场曝光量
  markdown_goods_number, -- 会场内销量
  markdown_gmv, -- 会场GMV
  no_markdown_impression_cnt, -- 非会场曝光量
  no_markdown_goods_number, -- 非会场销量
  no_markdown_gmv, -- 非会场gmv
  no_markdown_avg_price, -- 未参与降价id的均价
  no_markdown_max_goods_number -- 未参与降价id的最高销量
from
(
  select
    'vova' datasource,
    dog.virtual_goods_id virtual_goods_id, -- 虚拟商品id
    sum(if(pre_page_code in ('markdown_homepage','markdown_under','markdown_selection'), dog.goods_number, 0)) markdown_goods_number, -- 会场内销量
    sum(if(pre_page_code in ('markdown_homepage','markdown_under','markdown_selection'), dog.shipping_fee+dog.shop_price*dog.goods_number, 0)) markdown_gmv, -- 会场GMV
    sum(if(pre_page_code not in ('markdown_homepage','markdown_under','markdown_selection'), dog.goods_number, 0)) no_markdown_goods_number, -- 非会场销量
    sum(if(pre_page_code not in ('markdown_homepage','markdown_under','markdown_selection'), dog.shipping_fee+dog.shop_price*dog.goods_number, 0)) no_markdown_gmv -- 非会场gmv
  from
  (
    select
      distinct dog.virtual_goods_id virtual_goods_id -- 低价会场有销量商品
    from
      dwd.dwd_vova_fact_order_cause_v2 foc2
    left join
      dim.dim_vova_order_goods dog
    on foc2.datasource = dog.datasource and foc2.order_goods_id = dog.order_goods_id
    where foc2.pt='${cur_date}' and pre_page_code in ('markdown_homepage','markdown_under','markdown_selection')
      and foc2.datasource = 'vova' and foc2.platform in ('android','ios') and dog.pay_status >= 1
  ) tmp1
  left join
    dim.dim_vova_order_goods dog
  on tmp1.virtual_goods_id = dog.virtual_goods_id
  left join
    dwd.dwd_vova_fact_order_cause_v2 foc2
  on foc2.datasource = dog.datasource and foc2.order_goods_id = dog.order_goods_id
  where dog.datasource = 'vova' and dog.pay_status >= 1 and foc2.pt = '${cur_date}' and dog.platform in ('android','ios')
  group by dog.virtual_goods_id
) t1
left join
(
  select
    virtual_goods_id virtual_goods_id, -- 虚拟商品id
    count(virtual_goods_id) markdown_impression_cnt, -- 曝光次数
    sum(if(page_code in ('markdown_homepage','markdown_under','markdown_selection'), 1, 0)) no_markdown_impression_cnt -- 非会场曝光量
  from dwd.dwd_vova_log_goods_impression flgi
  where pt = '${cur_date}'
    and flgi.datasource = 'vova' and platform='mob' and geo_country is not null
  group by virtual_goods_id
) t2
on t1.virtual_goods_id = t2.virtual_goods_id
left join
(
  select
    markdown_virtual_goods_id virtual_goods_id,
    avg(no_markdown_avg_price) no_markdown_avg_price, -- 未参与降价id的均价 同款GSN下非会场商品的均价：售价+运费
    max(goods_number) no_markdown_max_goods_number -- 未参与降价id的最高销量  同款GSN下非会场商品中当日最高销量
  from
  (
    select
    tmp1.virtual_goods_id markdown_virtual_goods_id, -- 参与降价的商品
    dog.virtual_goods_id no_markdown_virtual_goods_id, -- 同款GSN下非会场商品
    avg(dg2.shop_price + dg2.shipping_fee) no_markdown_avg_price, -- 同款GSN下非会场商品售价
    sum(dog.goods_number) goods_number -- 同款GSN下非会场商品销量
    from
    (
      select
        distinct dog.virtual_goods_id virtual_goods_id, dog.goods_sn goods_sn -- 低价会场有销量商品
      from
        dwd.dwd_vova_fact_order_cause_v2 foc2
      left join
        dim.dim_vova_order_goods dog
      on foc2.datasource = dog.datasource and foc2.goods_id = dog.goods_id
      where foc2.pt='${cur_date}' and pre_page_code in ('markdown_homepage','markdown_under','markdown_selection')
        and foc2.datasource = 'vova' and foc2.platform in ('android','ios') and dog.pay_status >= 1
    ) tmp1
    left join
      (select * from dim.dim_vova_goods where datasource = 'vova') dg2 -- 获取低价会场有销量商品对应的GSN下非会场商品
    on tmp1.goods_sn = dg2.goods_sn
    left join
    (
      select *
      from
        dim.dim_vova_order_goods
      where datasource = 'vova' and pay_status >= 1
        and to_date(pay_time) = '${cur_date}' and platform in ('android','ios')
    ) dog -- 获取低价会场有销量商品对应的GSN下非会场商品当日订单
    on dg2.virtual_goods_id = dog.virtual_goods_id
    where tmp1.virtual_goods_id != dg2.virtual_goods_id  -- 过滤掉低价会场商品
    group by tmp1.virtual_goods_id, dog.virtual_goods_id
  ) res1
  group by markdown_virtual_goods_id
) t3
on t1.virtual_goods_id = t3.virtual_goods_id
left join
dim.dim_vova_goods dg
on t1.virtual_goods_id = dg.virtual_goods_id and t1.datasource = dg.datasource
left join
(select *
  from
    ods_vova_vts.ods_vova_activity_valid_goods_record
  where activity_select_name in ('ae_top_1000', 'low_price_original') and '${cur_date}' >= to_date(start_time) and '${cur_date}' <= to_date(end_time)
) vavgr
on dg.goods_id = vavgr.goods_id
left join
(
  select * from dim.dim_vova_merchant where datasource = 'vova'
) dm
on dg.mct_id = dm.mct_id
left join
(
  select
    distinct goods_id goods_id, activity_select_name activity_select_name
  from
    ods_vova_vts.ods_vova_activity_valid_goods_record
  where activity_select_name in ('low_price_replace') and '${cur_date}' >= to_date(start_time) and '${cur_date}' <= to_date(end_time)
) vavgr_replace
on dg.goods_id = vavgr_replace.goods_id
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job3_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql3"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job3_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
