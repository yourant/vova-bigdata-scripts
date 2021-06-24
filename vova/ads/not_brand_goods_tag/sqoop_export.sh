#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_vova_not_brand_goods_tag_data 800 "pt='${pre_date}'"


sql="
drop table if exists themis.ads_vova_not_brand_goods_tag_data_pre;
drop table if exists themis.ads_vova_not_brand_goods_tag_data_new;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_vova_not_brand_goods_tag_data_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`tag_id\` int(4) NOT NULL COMMENT '标签id',
  \`weight\` int(4) NOT NULL COMMENT '权重',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`goods_id\` (\`goods_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='非brand商品标签表';
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_vova_not_brand_goods_tag_data\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`tag_id\` int(4) NOT NULL COMMENT '标签id',
  \`weight\` int(4) NOT NULL COMMENT '权重',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`goods_id\` (\`goods_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='非brand商品标签表';
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table ads_vova_not_brand_goods_tag_data_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_not_brand_goods_tag_data \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns goods_id,tag_id,weight \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_vova_not_brand_goods_tag_data to themis.ads_vova_not_brand_goods_tag_data_pre,themis.ads_vova_not_brand_goods_tag_data_new to themis.ads_vova_not_brand_goods_tag_data;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi