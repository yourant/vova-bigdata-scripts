#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
cur_date2=`date -d "+1 day ${cur_date}" +%Y-%m-%d`
echo "$cur_date"
echo "$cur_date2"

##command: sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis_2.sh --db_code=vbd --table_name=test_goods_behave --etl_type=ALL  --mapers=3 --period_type=day --partition_num=3

sql="
INSERT OVERWRITE TABLE mlb.mlb_vova_hot_goods_prediction_pre PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(5) */
goods_id,
impressions,
test_start_date,
test_end_date,
is_test_scuess,
test_type,
test_goods_result_status
from
(
select
goods_id,
impressions,
test_start_date,
test_end_date,
is_test_scuess,
test_type,
test_goods_result_status,
ROW_NUMBER() OVER(PARTITION BY goods_id ORDER BY is_test_scuess desc, test_type desc) AS rank
from
(
SELECT
goods_id,
impressions,
test_start_date,
test_end_date,
is_test_scuess,
1 as test_type,
'-1' as test_goods_result_status
FROM
(
select
goods_id,
impressions,
create_time as test_start_date,
last_update_time as test_end_date,
if(test_result = 1,1,0) AS is_test_scuess,
ROW_NUMBER() OVER(PARTITION BY goods_id ORDER BY test_result, create_time desc) AS rank
from
ods_vova_vbd.ods_vova_test_goods_behave_inc
where test_status = 1
and test_result IN (1, 2)
) t1
where rank=1

-- UNION ALL
-- select
-- goods_id,
-- impressions,
-- add_test_time,
-- status_change_time,
-- if(test_goods_result_status = 6,1,0) AS is_test_scuess,
-- 2 as test_type,
-- test_goods_result_status
-- from
-- mlb.mlb_vova_vova_new_goods_examination_summary_history_export t1
-- where pt= '${cur_date}'
-- and test_goods_status > 1
-- and test_goods_result_status in (5, 6, 7)
) t1
) fin
where rank = 1
;

INSERT OVERWRITE TABLE mlb.mlb_vova_hot_goods_prediction_base PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(5) */
dg.cat_id,
hg.goods_id,
dg.first_on_time AS first_on_sale_date,
dg.last_on_time AS on_sale_date,
hg.test_start_date,
hg.test_end_date,
hg.is_test_scuess,
if(goods_gmv.goods_id is null, 0, if(goods_gmv.gmv / cg.gmv >= 0.01, 1, 0)) AS is_hot
from
mlb.mlb_vova_hot_goods_prediction_pre hg
inner join dim.dim_vova_goods dg on dg.goods_id = hg.goods_id
inner join (
select
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv,
dg.second_cat_id
from
dwd.dwd_vova_fact_pay fp
inner join dim.dim_vova_goods dg on dg.goods_id = fp.goods_id
where date(fp.pay_time) > date_sub('${cur_date}', 7)
and date(fp.pay_time) <= '${cur_date}'
and dg.second_cat_id is not null
group by dg.second_cat_id
) cg on cg.second_cat_id = dg.second_cat_id
left join
(
select
fp.goods_id,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time) > date_sub('${cur_date}', 7)
and date(fp.pay_time) <= '${cur_date}'
group by fp.goods_id
) goods_gmv on goods_gmv.goods_id = hg.goods_id

;
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=mlb_vova_hot_goods_prediction_base" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

