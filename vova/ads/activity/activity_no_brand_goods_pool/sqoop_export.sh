#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_vova_activity_no_brand_goods_pool 800 "pt='${pre_date}'"


sql="
drop table if exists themis.ads_vova_activity_no_brand_goods_pool_pre;
drop table if exists themis.ads_vova_activity_no_brand_goods_pool_new;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_vova_activity_no_brand_goods_pool_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`region_id\` int(11) NOT NULL COMMENT '国家id',
  \`first_cat_id\` int(11) NOT NULL COMMENT '一级分类id',
  \`second_cat_id\` int(11) NOT NULL COMMENT '二级品类id',
  \`biz_type\` varchar(50) NOT NULL COMMENT 'biz_type,规则id',
  \`rp_type\` varchar(10) NOT NULL COMMENT 'rp标记',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`region_id_key\` (\`region_id\`),
  KEY \`goods_id_key\` (\`goods_id\`),
  KEY \`first_cat_id_key\` (\`first_cat_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_vova_activity_no_brand_goods_pool\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`region_id\` int(11) NOT NULL COMMENT '国家id',
  \`first_cat_id\` int(11) NOT NULL COMMENT '一级分类id',
  \`second_cat_id\` int(11) NOT NULL COMMENT '二级品类id',
  \`biz_type\` varchar(50) NOT NULL COMMENT 'biz_type,规则id',
  \`rp_type\` varchar(10) NOT NULL COMMENT 'rp标记',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`region_id_key\` (\`region_id\`),
  KEY \`goods_id_key\` (\`goods_id\`),
  KEY \`first_cat_id_key\` (\`first_cat_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table ads_vova_activity_no_brand_goods_pool_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_activity_no_brand_goods_pool \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns goods_id,region_id,first_cat_id,second_cat_id,biz_type,rp_type,rank \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_vova_activity_no_brand_goods_pool to themis.ads_vova_activity_no_brand_goods_pool_pre,themis.ads_vova_activity_no_brand_goods_pool_new to themis.ads_vova_activity_no_brand_goods_pool;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi