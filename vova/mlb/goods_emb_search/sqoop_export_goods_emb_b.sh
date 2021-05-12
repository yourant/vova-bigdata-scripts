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
prefix=mlb_vova_search_goods_emb_b

sql="
drop table if exists \`als_images\`.\`${prefix}_${suffix}_pre\`;
drop table if exists \`als_images\`.\`${prefix}_${suffix}_new\`;
CREATE TABLE IF NOT EXISTS \`als_images\`.\`${prefix}_${suffix}_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
  \`goods_id\` bigint(20) NOT NULL DEFAULT '0' COMMENT '商品ID',
  \`goods_vec\` varchar(1600) NOT NULL DEFAULT '' COMMENT '商品向量序列化结果',
  \`is_update\` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否更新，0-不更新，1-更新',
  \`model_name\` varchar(255) NOT NULL DEFAULT '' COMMENT '模型名称',
  \`last_update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY \`ux_goods_id\` (\`goods_id\`) USING BTREE,
  KEY \`ix_update\` (\`is_update\`,\`goods_id\`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
CREATE TABLE IF NOT EXISTS \`als_images\`.\`${prefix}_${suffix}\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
  \`goods_id\` bigint(20) NOT NULL DEFAULT '0' COMMENT '商品ID',
  \`goods_vec\` varchar(1600) NOT NULL DEFAULT '' COMMENT '商品向量序列化结果',
  \`is_update\` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否更新，0-不更新，1-更新',
  \`model_name\` varchar(255) NOT NULL DEFAULT '' COMMENT '模型名称',
  \`last_update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY \`ux_goods_id\` (\`goods_id\`) USING BTREE,
  KEY \`ix_update\` (\`is_update\`,\`goods_id\`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi


spark-sql -e "msck repair table mlb.${tableName}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/als_images \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--m 1 \
--table ${prefix}_${suffix}_new \
--hcatalog-database mlb \
--hcatalog-table ${tableName} \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--columns goods_id,goods_vec,is_update,model_name \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

rename_sql="rename table als_images.${prefix}_${suffix} to als_images.${prefix}_${suffix}_pre,als_images.${prefix}_${suffix}_new to als_images.${prefix}_${suffix};"
echo $rename_sql
echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
$rename_sql
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi

echo "succ"