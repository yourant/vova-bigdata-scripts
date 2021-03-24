#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期当天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
select a.cur_date,
       a.mct_id,
       a.mct_name,
       a.not_app_rate,
       round(b.cnt / a.order_goods_cnt, 4) expre_rate,
       a.new_custumer_rate,
       a.expre_rate
from (
         select '${cur_date}'                                               cur_date,
                b.mct_id,
                b.mct_name,
                concat(round(count(distinct if(a.platform in ('pc', 'mob'), order_goods_id, null)) * 100 /
                             count(distinct order_goods_id), 4), '%')       not_app_rate,
                count(distinct order_goods_id)                              order_goods_cnt,
                concat(round(count(distinct if(to_date(d.first_pay_time) > date_sub('${cur_date}', 30), a.device_id,
                                               null)) *
                             100 / count(distinct a.device_id), 4), '%')    new_custumer_rate,
                concat(round(count(distinct if(to_date(d.first_pay_time) > date_sub('${cur_date}', 30), order_goods_id,
                                               null)) *
                             100 / count(distinct order_goods_id), 4), '%') expre_rate
         from dwd.dwd_vova_fact_pay a
                  join dim.dim_vova_merchant b on a.mct_id = b.mct_id
                  left join dim.dim_vova_devices d on a.device_id = d.device_id
         where a.datasource = 'vova'
           and to_date(a.pay_time) > date_sub('${cur_date}', 30)
         group by b.mct_id, b.mct_name
     ) a
         join (
    select b.mct_id, count(1) cnt
    from dwd.dwd_vova_log_goods_impression a
             join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
    where pt > date_sub('${cur_date}', 30)
    group by b.mct_id
) b on a.mct_id = b.mct_id
"

head="
日期,
店铺id,
店铺名称,
网站下单率,
曝光效率,
店铺新客率,
新用户订单占比
"

spark-submit \
--deploy-mode client \
--name 'vova_send_email_merchant_swipe_monitor' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "${sql}"  \
-head "${head}"  \
-receiver "juntao@vova.com.hk,alex.chen@vova.com.hk,queqiaoxian@vova.com.hk,qizi@vova.com.hk" \
-title "店铺刷单监控数据(${cur_date})" \
--type attachment \
--fileName "店铺刷单监控数据(${cur_date})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi
