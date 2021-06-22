#!/bin/bash

pre_date=$1
if [ ! -n "$1" ];then
   pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

db_url="rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com"
db_name="themis"
db_username="dwwriter"
db_password="wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx"

sql="
drop table if exists $db_name.mlb_vova_buyer_goods_rating_offline_pre;
drop table if exists $db_name.mlb_vova_buyer_goods_rating_offline_new;

CREATE TABLE IF NOT EXISTS \`$db_name\`.\`mlb_vova_buyer_goods_rating_offline_new\` (
  \`id\` int NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  \`buyer_id\` int NOT NULL COMMENT '用户ID',
  \`goods_id\` int NOT NULL COMMENT '商品ID',
  \`first_cat_id\` int NOT NULL COMMENT '一级品类ID',
  \`second_cat_id\` int COMMENT '二级品类ID',
  \`cat_id\` int NOT NULL COMMENT '品类ID',
  \`his_rating\` decimal(10,4) NOT NULL COMMENT '用户偏好',
  \`last_update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (\`id\`),
  UNIQUE KEY \`multi_unique_key\` (\`buyer_id\`,\`goods_id\`) USING BTREE,
  KEY \`buyer_id\` (\`buyer_id\`) USING BTREE
) ENGINE=InnoDB  AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户品类偏好离线表'
;

CREATE TABLE IF NOT EXISTS \`$db_name\`.\`mlb_vova_buyer_goods_rating_offline\` (
  \`id\` int NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  \`buyer_id\` int NOT NULL COMMENT '用户ID',
  \`goods_id\` int NOT NULL COMMENT '商品ID',
  \`first_cat_id\` int NOT NULL COMMENT '一级品类ID',
  \`second_cat_id\` int COMMENT '二级品类ID',
  \`cat_id\` int NOT NULL COMMENT '品类ID',
  \`his_rating\` decimal(10,4) NOT NULL COMMENT '用户偏好',
  \`last_update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (\`id\`),
  UNIQUE KEY \`multi_unique_key\` (\`buyer_id\`,\`goods_id\`) USING BTREE,
  KEY \`buyer_id\` (\`buyer_id\`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户品类偏好离线表'
;
"
mysql -h ${db_url} -u ${db_username} -p${db_password} -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://${db_url}:3306/${db_name} \
--username ${db_username} --password ${db_password} \
--m 1 \
--table mlb_vova_buyer_goods_rating_offline_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_buyer_goods_rating_offline \
--columns buyer_id,goods_id,first_cat_id,second_cat_id,cat_id,his_rating \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h ${db_url} -u ${db_username}  -p${db_password} <<EOF
rename table ${db_name}.mlb_vova_buyer_goods_rating_offline to ${db_name}.mlb_vova_buyer_goods_rating_offline_pre,${db_name}.mlb_vova_buyer_goods_rating_offline_new to ${db_name}.mlb_vova_buyer_goods_rating_offline;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
