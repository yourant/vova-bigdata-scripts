#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

sql="
drop table if exists themis.mlb_vova_goods_second_cat_pre;
drop table if exists themis.mlb_vova_goods_second_cat_new;
create table themis.mlb_vova_goods_second_cat_new(
  id                 int(11)     NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  virtual_goods_id   int         NOT NULL COMMENT '商品虚拟id',
  goods_id           int         NOT NULL COMMENT '商品id',
  second_cat_id      int         NOT NULL COMMENT '二级品类id',
  cat_id             int         NOT NULL COMMENT '品类id',
  group_id           int         NOT NULL COMMENT '商品组id',
  brand_id           int         NOT NULL COMMENT '品牌ID',
  update_time        datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id) USING BTREE,
KEY virtual_goods_id (virtual_goods_id) USING BTREE,
UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='在架商品及二级品类id'
;

create table if not exists themis.mlb_vova_goods_second_cat (
  id                 int(11)     NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  virtual_goods_id   int         NOT NULL COMMENT '商品虚拟id',
  goods_id           int         NOT NULL COMMENT '商品id',
  second_cat_id      int         NOT NULL COMMENT '二级品类id',
  cat_id             int         NOT NULL COMMENT '品类id',
  group_id           int         NOT NULL COMMENT '商品组id',
  brand_id           int         NOT NULL COMMENT '品牌ID',
  update_time        datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id) USING BTREE,
KEY virtual_goods_id (virtual_goods_id) USING BTREE,
UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='在架商品及二级品类id'
;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis?disableMariaDbDriver \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table mlb_vova_goods_second_cat_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_goods_second_cat \
--columns virtual_goods_id,goods_id,second_cat_id,cat_id,group_id,brand_id \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.mlb_vova_goods_second_cat to themis.mlb_vova_goods_second_cat_pre,themis.mlb_vova_goods_second_cat_new to themis.mlb_vova_goods_second_cat;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
