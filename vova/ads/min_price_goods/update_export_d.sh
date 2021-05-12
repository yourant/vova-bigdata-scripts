#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
echo "$pt"

sql="
drop table if exists themis.ads_min_price_goods_d_new;
drop table if exists themis.ads_min_price_goods_d_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.ads_min_price_goods_d_new (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  goods_id int(11) NOT NULL,
  min_price_goods_id int(11) NOT NULL,
  strategy varchar(16) NOT NULL,
  group_number varchar(32) NOT NULL,
  update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  min_show_price decimal(14,4) DEFAULT NULL COMMENT '最低价',
  avg_sku_price decimal(14,4) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY ux_goods_id (goods_id,strategy),
  KEY goods_id (goods_id),
  KEY group_number (group_number)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS themis.ads_min_price_goods_d (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  goods_id int(11) NOT NULL,
  min_price_goods_id int(11) NOT NULL,
  strategy varchar(16) NOT NULL,
  group_number varchar(32) NOT NULL,
  update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  min_show_price decimal(14,4) DEFAULT NULL COMMENT '最低价',
  avg_sku_price decimal(14,4) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY ux_goods_id (goods_id,strategy),
  KEY goods_id (goods_id),
  KEY group_number (group_number)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=10000 \
-Dmapreduce.map.memory.mb=8192 \
-Dmapreduce.reduce.memory.mb=8192 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_min_price_goods_d_new \
--update-key "goods_id,strategy" \
--m 1 \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_min_price_goods_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001' \
--columns "goods_id,min_price_goods_id,strategy,group_number,min_show_price,avg_sku_price"

if [ $? -ne 0 ];then
  echo "ads_min_price_goods_d sqoop error"
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_min_price_goods_d to themis.ads_min_price_goods_d_pre,themis.ads_min_price_goods_d_new to themis.ads_min_price_goods_d;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi