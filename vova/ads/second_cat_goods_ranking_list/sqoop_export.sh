#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.ads_second_cat_goods_ranking_list_pre;
drop table if exists themis.ads_second_cat_goods_ranking_list_new;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_second_cat_goods_ranking_list_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\`      int(11)  NOT NULL COMMENT '商品id',
  \`region_id\`     int(11) NOT NULL COMMENT '国家id',
  \`second_cat_id\` int(11)  NOT NULL COMMENT '二级分类id',
  \`is_brand\`      tinyint  NOT NULL COMMENT '是否brand,etc:1.brand,0.非brand',
  \`list_type\`     tinyint  NOT NULL COMMENT '榜单类型，etc:1.热销榜，2.好评榜，3.人气榜',
  \`list_val\`      int      NOT NULL COMMENT '榜单值，热销榜代表销量，好评表代表好评数，人气榜代表人气数',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`region_id_s_cat_id_type_key\` (\`second_cat_id\`,\`region_id\`,\`list_type\`,\`is_brand\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_second_cat_goods_ranking_list\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\`      int(11)  NOT NULL COMMENT '商品id',
  \`region_id\`     int(11) NOT NULL COMMENT '国家id',
  \`second_cat_id\` int(11)  NOT NULL COMMENT '二级分类id',
  \`is_brand\`      tinyint  NOT NULL COMMENT '是否brand,etc:1.brand,0.非brand',
  \`list_type\`     tinyint  NOT NULL COMMENT '榜单类型，etc:1.热销榜，2.好评榜，3.人气榜',
  \`list_val\`      int      NOT NULL COMMENT '榜单值，热销榜代表销量，好评表代表好评数，人气榜代表人气数',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`region_id_s_cat_id_type_key\` (\`second_cat_id\`,\`region_id\`,\`list_type\`,\`is_brand\`)
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
--table ads_second_cat_goods_ranking_list_new \
--hcatalog-database ads \
--hcatalog-table ads_second_cat_goods_ranking_list \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns goods_id,region_id,second_cat_id,is_brand,list_type,list_val,rank \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki <<EOF
rename table themis.ads_second_cat_goods_ranking_list to themis.ads_second_cat_goods_ranking_list_pre,themis.ads_second_cat_goods_ranking_list_new to themis.ads_second_cat_goods_ranking_list;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
