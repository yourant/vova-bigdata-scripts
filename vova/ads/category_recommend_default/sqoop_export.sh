#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.ads_category_recommend_default_pre;
drop table if exists themis.ads_category_recommend_default_new;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_category_recommend_default_new\` (
  \`id\`        int(11) NOT NULL AUTO_INCREMENT,
  \`cat_id\`    int(11) NOT NULL COMMENT 'category id',
  \`type\`      int(1)  NOT NULL COMMENT  '1.一级类目，2.二级类目',
  \`region_id\` int(11) NOT NULL COMMENT '国家id',
  \`gender\`    int(1)  NOT NULL COMMENT '1.man，2.wemen，3.unknow',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`cat_id_key\` (\`cat_id\`,\`region_id\`,\`gender\`,\`type\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_category_recommend_default\` (
  \`id\`        int(11) NOT NULL AUTO_INCREMENT,
  \`cat_id\`    int(11) NOT NULL COMMENT 'category id',
  \`type\`      int(1)  NOT NULL COMMENT  '1.一级类目，2.二级类目',
  \`region_id\` int(11) NOT NULL COMMENT '国家id',
  \`gender\`    int(1)  NOT NULL COMMENT '1.man，2.wemen，3.unknow',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`cat_id_key\` (\`cat_id\`,\`region_id\`,\`gender\`,\`type\`)
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
--table ads_category_recommend_default_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_category_recommend_default \
--columns cat_id,region_id,gender,type,rank \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.ads_category_recommend_default to themis.ads_category_recommend_default_pre,themis.ads_category_recommend_default_new to themis.ads_category_recommend_default;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi