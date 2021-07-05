#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
insert overwrite table dwb.dwb_vova_goods_current_limiting PARTITION (pt='${cur_date}')
select tmp.goods_id,
       if(dg.group_id = -1,1,0) as is_alone,
       if(dg.group_id = -1,null,dg.group_id),
       dg.shop_price,
       if(dg.group_id!=-1 and dg.shop_price = price_min, 1,0) as is_cheapest
from (
         select goods_id,
                0 as nlrf_rate_9w,
                mct_ref_rate
         from (
                  select t1.goods_id,
                         sum(mct_refund_15w) / count(distinct t1.order_goods_id) as mct_ref_rate
                  from (
                           select fp.goods_id,
                                  fr.order_goods_id,
                                  if(fr.refund_type_id in (5, 6, 11, 14) and ogs.sku_pay_status > 1 and
                                     ((fr.rr_audit_status = 'audit_passed' and
                                       datediff(fr.rr_audit_time, fp.confirm_time) < 105) or
                                      (fr.rr_audit_status != 'audit_passed' and ogs.sku_pay_status = 4 and datediff(fr.exec_refund_time,fp.confirm_time)<105)),
                                     1, 0) as mct_refund_15w
                           from dwd.dwd_vova_fact_pay fp
                                    left join dwd.dwd_vova_fact_refund fr
                                              on fp.order_goods_id = fr.order_goods_id
                                    left join ods_vova_vts.ods_vova_order_goods_status ogs
                                              on fp.order_goods_id = ogs.order_goods_id
                           where to_date(fp.pay_time) >= date_sub('${cur_date}', dayofweek('${cur_date}') - 2)
                             and fp.datasource = 'vova'
                       ) t1
                  group by t1.goods_id
              ) t
         union all
         select goods_id,
                nlrf_rate_9w,
                0 as mct_ref_rate
         from (
                  select t1.goods_id,
                         sum(t1.nlrf_order_cnt_9w) / count(t1.order_goods_id) as nlrf_rate_9w
                  from (
                           select og.goods_id,
                                  og.order_goods_id,
                                  case
                                      when datediff(fr.audit_time, og.confirm_time) < 63 and
                                           fr.refund_reason_type_id not in (8, 9) and fr.refund_type_id = 2 and
                                           fr.rr_audit_status = 'audit_passed' and og.sku_pay_status > 1 then 1
                                      else 0 end nlrf_order_cnt_9w
                           from dim.dim_vova_order_goods og
                                    left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id = og.order_goods_id
                                    left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id = fl.order_goods_id
                           where datediff('${cur_date}', to_date(og.confirm_time)) between 62 and 92
                       ) t1
                  group by t1.goods_id
              ) t
     ) tmp
         inner join dim.dim_vova_goods dg
                    on dg.goods_id = tmp.goods_id
left join (select group_id,min(shop_price) as price_min
           from dim.dim_vova_goods
           group by group_id) m
on dg.group_id = m.group_id
where tmp.goods_id in (
    select goods_id
    from dwd.dwd_vova_fact_pay
    where datediff('${cur_date}', to_date(pay_time)) > 28
      and datediff('${cur_date}', to_date(pay_time)) <= 56
    group by goods_id
    having sum(goods_number) >= 10
)
group by tmp.goods_id,
         dg.group_id,
         dg.shop_price,
         price_min
having sum(nlrf_rate_9w) + sum(mct_ref_rate) > 0.3
;
"

spark-sql \
--conf "spark.app.name=dwb_vova_goods_current_limiting" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=10" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi

spark-submit \
--deploy-mode client \
--name 'vova_goods_current_limiting' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "select * from dwb.dwb_vova_goods_current_limiting where pt='${cur_date}'"  \
-head "商品ID,是否为孤品,商品组id,商品价格,是否为组内最低价"  \
-receiver "huyinke@vova.com.hk,linshiyin@vova.com.hk,huachen@vova.com.hk" \
-title "商品限流数据 ${cur_date}" \
--type attachment \
--fileName "商品限流数据 ${cur_date}"

if [ $? -ne 0 ];then
  exit 1
fi