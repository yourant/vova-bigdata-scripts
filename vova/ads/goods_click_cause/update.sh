#!/bin/bash
cur_date=$1
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
now_date=`date -d "-1 days ago ${cur_date}" +%Y-%m-%d`

spark-submit \
--deploy-mode client \
--master yarn  \
--driver-memory 8G \
--conf spark.dynamicAllocation.maxExecutors=120 \
--conf spark.default.parallelism=380 \
--conf spark.sql.shuffle.partitions=380 \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.join.enabled=true \
--conf spark.shuffle.sort.bypassMergeThreshold=10000 \
--conf spark.sql.inMemoryColumnarStorage.compressed=true \
--conf spark.sql.inMemoryColumnarStorage.partitionPruning=true \
--conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
--conf spark.network.timeout=300 \
--conf spark.app.name=rec_vova_user_clk_behave_link_d \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.eventLog.enabled=false \
--driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsClickCause s3://vomkt-emr-rec/jar/vova-goods-click-cause-v2.jar \
--pt ${cur_date} --now_date ${now_date}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

spark-sql \
--driver-memory 8G \
--executor-memory 16G --executor-cores 4 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=user_behavior_link_add" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.driver.maxResultSize=8G" \
-e"


insert overwrite table tmp.tmp_user_behavior_link_add_clk_1
select
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d a
left join (SELECT event_fingerprint,
        device_id,
        session_id,
        buyer_id,
        gender,
        b.languages_id                      language,
        c.country_id                        geo_country,
        os_type,
        device_model,
        cast(element_id AS BIGINT)          virtual_goods_id,
        page_code,
        list_type,
        collector_tstamp,
        d.goods_id,
        cast(dvce_created_tstamp as bigint) dvce_created_tstamp
 FROM dwd.dwd_vova_log_click_arc a
          left join (select languages_id, languages_code
                     from dim.dim_vova_languages
                     group by languages_id, languages_code) b
                    on a.language = b.languages_code
          left join (select country_id, country_code from dim.dim_vova_region group by country_id, country_code) c
                    on a.geo_country = c.country_code
          left join dim.dim_vova_goods d on cast(a.element_id AS BIGINT) = d.virtual_goods_id
 WHERE pt <= '$cur_date' and pt >= date_sub('$cur_date',1)
   AND event_type = 'goods'
   AND platform = 'mob') b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date'
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clk_2
select
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,

      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(c.collector_tstamp as string),14,'0'),cast(c.goods_id as string))
                 )
               )
             ),
        '\\\d+:','') AS goods_cart_list
from tmp.tmp_user_behavior_link_add_clk_1 a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',2)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToCartSuccess')
    ) c on a.buyer_id = c.buyer_id and a.device_id = c.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > c.collector_tstamp
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,a.goods_clk_list
;

insert overwrite table tmp.tmp_user_behavior_link_add_clk_3
select
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list,
      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(d.collector_tstamp as string),14,'0'),cast(d.goods_id as string))
                 )
               )
             ),
        '\\\d+:','') AS goods_wish_list
from tmp.tmp_user_behavior_link_add_clk_2 a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',2)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToWishlistClick')
    ) d on a.buyer_id = d.buyer_id and a.device_id = d.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > d.collector_tstamp
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list
;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ads.ads_vova_user_behave_link_d partition(pt='$cur_date',pagecode)
select  /*+ REPARTITION(4) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list,
    a.goods_wish_list,
      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(e.collector_tstamp as string),14,'0'),cast(e.search_words as string))
                 )
               )
             ),
        '\\\d+:','') AS search_words_list,page_code pagecode
from tmp.tmp_user_behavior_link_add_clk_3 a
left join (
    select device_id,buyer_id,collector_tstamp,
           lower(trim(regexp_replace(regexp_replace(element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) search_words
           from dwd.dwd_vova_log_common_click where pt = '$cur_date'
           and element_name = 'search_confirm'
    ) e on a.buyer_id = e.buyer_id and a.device_id = e.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > e.collector_tstamp
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list,
    a.goods_wish_list
;

"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


