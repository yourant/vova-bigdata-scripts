#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dwd/dwd_vova_fact_luk_act
sql="
insert overwrite table dwd.dwd_vova_fact_luk_act
select /*+ REPARTITION(1) */
       'vova'                       as datasource,
       la.activity_id               as act_id,
       la.activity_config_id        as act_cfg_id,
       la.start_time                as start_time,
       la.end_time                  as end_time,
       la.round                     as round,
       la.current_number            as crt_cnt,
       la.activity_status           as act_sts,
       lap.prize_id                 as prz_id,
       lap.goods_id                 as gs_id,
       lap.market_price             as mkt_prc,
       lac.maximum_number           as max_cnt,
       lac.activity_interval        as act_itv,
       lac.activity_cost_type       as act_cst_type,
       lac.activity_cost_value      as act_cst_val,
       lac.activity_group_config_id as act_grp_cfg_id,
       lac.is_rapid                 as is_rapid,
       lagc.need_number             as need_cnt
from ods_vova_vts.ods_vova_luckystar_activity la
         inner join ods_vova_vts.ods_vova_luckystar_activity_config lac on la.activity_config_id = lac.activity_config_id
         inner join ods_vova_vts.ods_vova_luckystar_activity_prize lap on lap.prize_id = lac.prize_id
         inner join ods_vova_vts.ods_vova_luckystar_activity_group_config lagc on lagc.activity_group_config_id = lac.activity_group_config_id;

insert overwrite table dwd.dwd_vova_fact_luk_grp_act
select /*+ REPARTITION(2) */
       'vova'                  as datasource,
       lg.activity_id          as act_id,
       lg.group_id             as grp_id,
       lg.group_status         as grp_sts,
       lgm.group_member_id     as grp_mbr_id,
       lgm.user_id             as byr_id,
       lgm.type                as type,
       lgm.group_role          as grp_role,
       lgm.group_member_status as grp_mbr_sts,
       lgm.order_id            as ord_id,
       lgm.luckystar_order_id  as vtl_ord_id,
       lgm.goods_number        as gs_cnt,
       lgm.reward_coupon_code  as rwd_cpn_code,
       lwc.prize_id            as prz_id,
       lap.goods_id            as gs_id,
       lap.market_price        as mkt_prz,
       lwc.winning_status      as win_sts,
       lwr.lottery_source      as lty_src
from ods_vova_vts.ods_vova_luckystar_group lg
         left join ods_vova_vts.ods_vova_luckystar_group_member lgm on lg.group_id = lgm.group_id
         left join ods_vova_vts.ods_vova_luckystar_winning_config lwc on lwc.group_member_id = lgm.group_member_id
         left join ods_vova_vts.ods_vova_luckystar_activity_prize lap on lap.prize_id = lwc.prize_id
         left join ods_vova_vts.ods_vova_luckystar_winning_record lwr on lwr.group_member_id = lgm.group_member_id;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
#spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=40" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwd_vova_luckystar" --conf "spark.sql.autoBroadcastJoinThreshold=10485760" --conf "spark.sql.output.merge=true"   -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi