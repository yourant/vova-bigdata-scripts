#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

sql="
drop table if exists themis.ads_vova_buyer_channel_pre;
drop table if exists themis.ads_vova_buyer_channel_new;
create table themis.ads_vova_buyer_channel_new(
  id            int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  device_id     varchar(150)  NOT NULL COMMENT '设备id',
  channel       varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '渠道',
  update_time   datetime      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY device_id (device_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='vova用户渠道';

create table if not exists themis.ads_vova_buyer_channel (
  id            int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  device_id     varchar(150)  NOT NULL COMMENT '设备id',
  channel       varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '渠道',
  update_time   datetime      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY device_id (device_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='vova用户渠道';
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=10240 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table ads_vova_buyer_channel_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_buyer_channel \
--columns device_id,channel \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_vova_buyer_channel to themis.ads_vova_buyer_channel_pre,themis.ads_vova_buyer_channel_new to themis.ads_vova_buyer_channel;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
