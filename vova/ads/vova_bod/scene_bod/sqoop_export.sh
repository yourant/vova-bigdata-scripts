#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_vova_scene_bod_goods_rank_data 800 "pt='${pre_date}'"

sql="
drop table if exists rec_recall.ads_vova_scene_bod_goods_rank_data_pre;
drop table if exists rec_recall.ads_vova_scene_bod_goods_rank_data_new;
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`ads_vova_scene_bod_goods_rank_data_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`bod_id\` int(11) NOT NULL COMMENT '榜单id',
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`rank\` int(11) NOT NULL COMMENT '商品评分排名',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`),
  KEY \`bod_id\` (\`bod_id\`),
  KEY \`goods_id\` (\`goods_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='场景式榜单数据';
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`ads_vova_scene_bod_goods_rank_data\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`bod_id\` int(11) NOT NULL COMMENT '榜单id',
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`rank\` int(11) NOT NULL COMMENT '商品评分排名',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`),
  KEY \`bod_id\` (\`bod_id\`),
  KEY \`goods_id\` (\`goods_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='场景式榜单数据';
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table ads_vova_scene_bod_goods_rank_data_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_scene_bod_goods_rank_data \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns bod_id,goods_id,rank \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.ads_vova_scene_bod_goods_rank_data to rec_recall.ads_vova_scene_bod_goods_rank_data_pre,rec_recall.ads_vova_scene_bod_goods_rank_data_new to rec_recall.ads_vova_scene_bod_goods_rank_data;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi