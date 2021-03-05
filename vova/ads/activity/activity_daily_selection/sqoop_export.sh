#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi


sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_vova_activity_daily_selection 13000 "pt='${pre_date}'"

if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists themis.ads_activity_daily_selection_pre;
drop table if exists themis.ads_activity_daily_selection_new;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_activity_daily_selection_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`first_cat_id\` int(11) NOT NULL COMMENT '二级分类id',
  \`region_id\` int(11) NOT NULL COMMENT '国家id',
  \`event_type\` varchar(10) NOT NULL COMMENT '类型',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`region_id_key\` (\`region_id\`),
  KEY \`first_cat_id_key\` (\`first_cat_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_activity_daily_selection\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`first_cat_id\` int(11) NOT NULL COMMENT '二级分类id',
  \`region_id\` int(11) NOT NULL COMMENT '国家id',
  \`event_type\` varchar(10) NOT NULL COMMENT '类型',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`region_id_key\` (\`region_id\`) USING BTREE,
  KEY \`first_cat_id_key\` (\`first_cat_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--m 1 \
--table ads_activity_daily_selection_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_activity_daily_selection \
--columns event_type,region_id,first_cat_id,goods_id,rank \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.ads_activity_daily_selection to themis.ads_activity_daily_selection_pre,themis.ads_activity_daily_selection_new to themis.ads_activity_daily_selection;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi