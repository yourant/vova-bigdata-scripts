#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mariadb:aurora://db-logistics-w.gitvv.com:3306/themis_logistics_report \
--username vvreport20210517 --password thuy*at1OhG1eiyoh8she \
--connection-manager org.apache.sqoop.manager.MySQLManager \
--table dwb_vova_market_web_dau \
--update-key event_date,datasource,region_code \
--update-mode allowinsert \
--m 1 \
--hcatalog-database dwb \
--hcatalog-table dwb_vova_market_web_dau \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi