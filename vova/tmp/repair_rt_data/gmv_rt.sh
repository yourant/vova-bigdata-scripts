#!/bin/bash
#指定日期和引擎
cur_date=$1
cur_hour=$2
sql="
insert overwrite table tmp.tmp_gmv_rt
select 'all' datasource,'all' country,to_date(pay_time) cur_date,date_format(pay_time,'HH') cur_hour,sum(shop_price * goods_number + shipping_fee) gmv,count(distinct order_id) as payed_num,count(distinct buyer_id) as payed_uv from dwd.fact_pay where to_date(pay_time)='$cur_date' and date_format(pay_time,'HH')='$cur_hour' group by to_date(pay_time),date_format(pay_time,'HH')
union all
select datasource,'all' country, to_date(pay_time) cur_date,date_format(pay_time,'HH') cur_hour,sum(shop_price * goods_number + shipping_fee) gmv ,count(distinct order_id) as payed_num,count(distinct buyer_id) as payed_uv from dwd.fact_pay where to_date(pay_time)='$cur_date' and date_format(pay_time,'HH')='$cur_hour' group by datasource, to_date(pay_time),date_format(pay_time,'HH')
union all
select 'all' datasource,region_code country, to_date(pay_time) cur_date,date_format(pay_time,'HH') cur_hour,sum(shop_price * goods_number + shipping_fee) gmv ,count(distinct order_id) as payed_num,count(distinct buyer_id) as payed_uv from dwd.fact_pay where to_date(pay_time)='$cur_date' and date_format(pay_time,'HH')='$cur_hour' group by region_code, to_date(pay_time),date_format(pay_time,'HH')
union all
select datasource,region_code country, to_date(pay_time) cur_date,date_format(pay_time,'HH') cur_hour,sum(shop_price * goods_number + shipping_fee) gmv ,count(distinct order_id) as payed_num,count(distinct buyer_id) as payed_uv from dwd.fact_pay where to_date(pay_time)='$cur_date' and date_format(pay_time,'HH')='$cur_hour' group by datasource,region_code, to_date(pay_time),date_format(pay_time,'HH')
"
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.sql.output.merge=true"  --conf "spark.app.name=repair_rt_gmv" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bimaster --password sYG2Ri3yIDu2NPki \
--m 1 \
--table rpt_gmv_h_rt \
--update-key "datasource,country,cur_date,cur_hour" \
--update-mode allowinsert \
--hcatalog-database tmp \
--hcatalog-table tmp_gmv_rt \
--fields-terminated-by '\001'