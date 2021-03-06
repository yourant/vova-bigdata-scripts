#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
drop table if exists themis.ads_vova_sale_goods_3m_new;
drop table if exists themis.ads_vova_sale_goods_3m_pre;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

sql="
CREATE TABLE IF NOT EXISTS themis.ads_vova_sale_goods_3m_new
(
    id                   int(11)          NOT NULL AUTO_INCREMENT,
    goods_id             int(11) UNSIGNED NOT NULL COMMENT '商品id',
    sales_order          int(11) NOT NULL DEFAULT '0' COMMENT '销量',
    create_time          timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time     timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY goods_id (goods_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ads_vova_sale_goods_3m'
;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_vova_sale_goods_3m_new \
--m 1 \
--columns goods_id,sales_order \
--hcatalog-database ads \
--hcatalog-table ads_vova_sale_goods_3m \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_vova_sale_goods_3m to themis.ads_vova_sale_goods_3m_pre,
             themis.ads_vova_sale_goods_3m_new to themis.ads_vova_sale_goods_3m;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi