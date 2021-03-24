#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新用户首单
sql="
drop table if exists ods.vova_goods_languages_merge;
create table if not exists ods.vova_goods_languages_merge as
select
/*+ REPARTITION(500) */
*
from
(
select id,goods_id,'ar' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_ar
union
select id,goods_id,'be' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_be
union
select id,goods_id,'cs' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_cs
union
select id,goods_id,'da' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_da
union
select id,goods_id,'de' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_de
union
select id,goods_id,'el' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_el
union
select id,goods_id,'en' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_en
union
select id,goods_id,'es' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_es
union
select id,goods_id,'et' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_et
union
select id,goods_id,'fi' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_fi
union
select id,goods_id,'fr' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_fr
union
select id,goods_id,'ga' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_ga
union
select id,goods_id,'he' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_he
union
select id,goods_id,'hr' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_hr
union
select id,goods_id,'ht' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_ht
union
select id,goods_id,'hu' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_hu
union
select id,goods_id,'id' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_id
union
select id,goods_id,'is' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_is
union
select id,goods_id,'it' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_it
union
select id,goods_id,'ja' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_ja
union
select id,goods_id,'ko' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_ko
union
select id,goods_id,'lt' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_lt
union
select id,goods_id,'ms' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_ms
union
select id,goods_id,'mt' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_mt
union
select id,goods_id,'nl' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_nl
union
select id,goods_id,'no' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_no
union
select id,goods_id,'pl' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_pl
union
select id,goods_id,'pt' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_pt
union
select id,goods_id,'ru' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_ru
union
select id,goods_id,'se' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_se
union
select id,goods_id,'sk' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_sk
union
select id,goods_id,'sl' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_sl
union
select id,goods_id,'th' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_th
union
select id,goods_id,'tr' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_tr
union
select id,goods_id,'tw' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_tw
union
select id,goods_id,'zh' language_code,source,goods_name,keywords,goods_desc,create_time,last_update_time from ods.vova_goods_languages_zh
) t;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql  --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.app.name=merge_vova_goods_languages" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
