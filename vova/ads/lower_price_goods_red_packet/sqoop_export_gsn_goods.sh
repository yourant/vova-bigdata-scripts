#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

hive -e "msck repair table ads.ads_vova_red_packet_gsn_goods;"
if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists themis.ads_vova_red_packet_gsn_goods_pre;
drop table if exists themis.ads_vova_red_packet_gsn_goods_new;
create table IF NOT EXISTS themis.ads_vova_red_packet_gsn_goods_new(
  id             int(11)      NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id       bigint       NOT NULL COMMENT '商品id',
  goods_sn       varchar(200) NOT NULL COMMENT '商品GSN',
  update_time    datetime     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='红包报名商品对应gsn下goods_id'
;
create table IF NOT EXISTS themis.ads_vova_red_packet_gsn_goods(
  id             int(11)      NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id       bigint       NOT NULL COMMENT '商品id',
  goods_sn       varchar(200) NOT NULL COMMENT '商品GSN',
  update_time    datetime     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='红包报名商品对应gsn下goods_id'
;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis?disableMariaDbDriver \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--m 1 \
--table ads_vova_red_packet_gsn_goods_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_red_packet_gsn_goods \
--columns goods_id,goods_sn \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.ads_vova_red_packet_gsn_goods to themis.ads_vova_red_packet_gsn_goods_pre,themis.ads_vova_red_packet_gsn_goods_new to themis.ads_vova_red_packet_gsn_goods;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi