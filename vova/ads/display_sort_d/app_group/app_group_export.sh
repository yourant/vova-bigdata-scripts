#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
drop table if exists themis.ads_app_group_display_sort_new;
drop table if exists themis.ads_app_group_display_sort_pre;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

sql="
CREATE TABLE IF NOT EXISTS themis.ads_app_group_display_sort_new
(
    id                   int(11)          NOT NULL AUTO_INCREMENT,
    goods_id             int(11) UNSIGNED NOT NULL COMMENT '商品id',
    region_id            int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'region_id',
    region_standard_type varchar(20)      NOT NULL DEFAULT 'excellent ,normal ',
    gcr_rank_desc        int(11)          NOT NULL DEFAULT 0,
    create_time          timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time     timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY idx_goods (goods_id, region_id, region_standard_type)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='ads_app_group_display_sort';
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_app_group_display_sort_new \
--m 1 \
--columns goods_id,region_id,region_standard_type,gcr_rank_desc \
--hcatalog-database ads \
--hcatalog-table ads_vova_app_group_display_sort \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_app_group_display_sort to themis.ads_app_group_display_sort_pre,
             themis.ads_app_group_display_sort_new to themis.ads_app_group_display_sort;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi