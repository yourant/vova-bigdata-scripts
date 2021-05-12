#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-0 day" +%Y-%m-%d`
fi

sql="
drop table if exists themis.ads_vova_six_mct_goods_flow_support_h_new;
drop table if exists themis.ads_vova_six_mct_goods_flow_support_h_pre;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

sql="
CREATE TABLE IF NOT EXISTS themis.ads_vova_six_mct_goods_flow_support_h_new
(
    id                   int(11)          NOT NULL AUTO_INCREMENT,
    goods_id             int(11) UNSIGNED NOT NULL COMMENT '商品id',
    page_code            varchar(30)      NOT NULL DEFAULT '' COMMENT 'product_detail, product_list',
    create_time          timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time     timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY goods_id (goods_id, page_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ads_vova_six_mct_goods_flow_support_h'
;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_vova_six_mct_goods_flow_support_h_new \
--m 1 \
--columns goods_id,page_code \
--hcatalog-database ads \
--hcatalog-table ads_vova_six_mct_goods_flow_support_h \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_vova_six_mct_goods_flow_support_h to themis.ads_vova_six_mct_goods_flow_support_h_pre,
             themis.ads_vova_six_mct_goods_flow_support_h_new to themis.ads_vova_six_mct_goods_flow_support_h;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi