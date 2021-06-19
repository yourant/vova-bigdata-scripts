#!/bin/bash
#指定日期和引擎
if [ $? -ne 0 ];then
  exit 1
fi
tableName=$1
pt=$2
suffix=$3
if [ ! -n "$suffix" ]; then
  exit 1
fi
prefix=mlb_vova_recall_words_search

sql="
drop table if exists themis.${prefix}_${suffix}_pre;
drop table if exists themis.${prefix}_${suffix}_new;
CREATE TABLE IF NOT EXISTS \`themis\`.\`${prefix}_${suffix}_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
  \`words\` varchar(200) NOT NULL DEFAULT '0' COMMENT '商品向量序列化结果',
  \`is_update\` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否更新，0-不更新，1-更新',
  \`model_name\` varchar(255) NOT NULL DEFAULT '' COMMENT '模型名称',
  \`last_update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`ux_words\` (\`words\`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 row_format=dynamic;;
CREATE TABLE IF NOT EXISTS \`themis\`.\`${prefix}_${suffix}\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
  \`words\` varchar(200) NOT NULL DEFAULT '0' COMMENT '商品向量序列化结果',
  \`is_update\` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否更新，0-不更新，1-更新',
  \`model_name\` varchar(255) NOT NULL DEFAULT '' COMMENT '模型名称',
  \`last_update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`ux_words\` (\`words\`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 row_format=dynamic;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

spark-sql -e "msck repair table mlb.${tableName}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table ${prefix}_${suffix}_new \
--hcatalog-database mlb \
--hcatalog-table ${tableName} \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--columns words,is_update,model_name \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi
rename_sql="rename table themis.${prefix}_${suffix} to themis.${prefix}_${suffix}_pre,themis.${prefix}_${suffix}_new to themis.${prefix}_${suffix};"
echo $rename_sql
echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
$rename_sql
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi

echo "succ"