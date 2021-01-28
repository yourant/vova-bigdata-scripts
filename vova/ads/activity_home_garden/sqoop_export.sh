#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.ads_activity_home_garden_pre;
drop table if exists themis.ads_activity_home_garden_new;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_activity_home_garden_new\` (
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
  KEY \`biz_type_key\` (\`biz_type\`),
  KEY \`rp_type_key\` (\`rp_type\`),
  KEY \`first_cat_id_key\` (\`first_cat_id\`),
  KEY \`second_cat_id_key\` (\`second_cat_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;

CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_activity_home_garden\` (
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
  KEY \`biz_type_key\` (\`biz_type\`),
  KEY \`rp_type_key\` (\`rp_type\`),
  KEY \`first_cat_id_key\` (\`first_cat_id\`),
  KEY \`second_cat_id_key\` (\`second_cat_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bimaster --password sYG2Ri3yIDu2NPki \
--m 1 \
--table ads_activity_home_garden_new \
--hcatalog-database ads \
--hcatalog-table ads_activity_home_garden \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns goods_id,region_id,first_cat_id,second_cat_id,biz_type,rp_type,rank \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki <<EOF
rename table themis.ads_activity_home_garden to themis.ads_activity_home_garden_pre,themis.ads_activity_home_garden_new to themis.ads_activity_home_garden;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
