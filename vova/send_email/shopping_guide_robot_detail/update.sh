#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期当天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
file_date=`date -d "-1 day" +%Y%m%d`
fi

sql="
select '${cur_date}' cur_date,
       a.region_code region_code,
       a.occur_time occur_time,
       a.list list,
       a.behavior behavior,
       a.buyer_id buyer_id,
       b.gender gender,
       b.user_age_group user_age_group,
       c.goods_id goods_id,
       c.shop_price shop_price,
       regexp_replace(d.brand_name, ',', '-') brand_name
from (
         select a.geo_country                                                                 region_code,
                from_unixtime(cast(collector_tstamp / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') occur_time,
                nvl(if(page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list', '会话界面',
                       'viewall列表'),
                    'all')                                                                    list,
                '点击'                                                                          behavior,
                a.buyer_id,a.virtual_goods_id
         from dwd.dwd_vova_log_goods_click a
         where pt = '${cur_date}'
           and ((page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list') or
                (page_code = 'recommend_product_list' and list_type = '/robert_guide_also_like'))
         union all
         select b.region_code,
                from_unixtime(cast(dvce_created_tstamp / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') occur_time,
                nvl(if(pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list', '会话界面',
                       'viewall列表'), 'all')                                               list,
                '加购'                                                                      behavior,
                a.buyer_id,a.virtual_goods_id
         from dwd.dwd_vova_fact_cart_cause_v2 a
                  left join dim.dim_vova_buyers b on a.buyer_id = b.buyer_id
         where pt = '${cur_date}'
           and ((pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list') or
                (pre_page_code = 'recommend_product_list' and pre_list_type = '/robert_guide_also_like'))
         union all
         select b.region_code,
                b.pay_time                  occur_time,
                nvl(if(pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list', '会话界面',
                       'viewall列表'), 'all') list,
                '支付'                        behavior,
                a.buyer_id,c.virtual_goods_id
         from dwd.dwd_vova_fact_order_cause_v2 a
                  join dwd.dwd_vova_fact_pay b on a.order_goods_id = b.order_goods_id
         left join dim.dim_vova_goods c on b.goods_id = c.goods_id
         where a.pt = '${cur_date}'
           and ((pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list') or
                (pre_page_code = 'recommend_product_list' and pre_list_type = '/robert_guide_also_like'))
     ) a
         left join dim.dim_vova_buyers b on a.buyer_id = b.buyer_id
         left join dim.dim_vova_goods c on a.virtual_goods_id = c.virtual_goods_id
         left join ods_vova_vts.ods_vova_brand d on c.brand_id = d.brand_id
"

head="
日期,
国家,
事件发生时间,
列表,
行为,
用户ID,
性别,
年龄区间,
商品ID,
价格,
品类名称
"

spark-submit \
--deploy-mode client \
--name 'vova_send_email_shopping_guide_robot_email_send' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "${sql}"  \
-head "${head}"  \
-receiver "juntao@vova.com.hk,mulan@vova.com.hk" \
-title "智能导购机器人明细(${cur_date})" \
--type attachment \
--fileName "智能导购机器人明细数据(${cur_date})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi
