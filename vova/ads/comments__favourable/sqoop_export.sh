#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
echo "pre_date: ${pre_date}"

hive -e "msck repair table ads.ads_vova_comment_favourable;"
if [ $? -ne 0 ];then
  exit 1
fi
echo "msck repair table ok"

cnt=$(spark-sql -e "select count(*) from ads.ads_vova_comment_favourable where pt ='${pre_date}';" | tail -1)
echo ${cnt}
if [ ${cnt} -le 0 ];then
  echo "Error: count(*)=${cnt} -le 0"
  exit 1
fi

sql="
drop table if exists themis.comments_favourable_pre;
drop table if exists themis.comments_favourable_new;
CREATE TABLE IF NOT EXISTS \`themis\`.\`comments_favourable_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` bigint(20) NOT NULL COMMENT '商品id',
  \`buyer_id\` bigint(20) NOT NULL COMMENT 'buyer id',
  \`comment_id\` bigint(20) NOT NULL COMMENT 'comment id',
  \`mct_id\` int(11) NOT NULL COMMENT 'merchant id',
  \`first_cat_id\` int(11) NOT NULL COMMENT '商品一级类目id，0表示全部',
  \`rank\` int(11) NOT NULL COMMENT '排名',
  \`order_type\` int(1) NOT NULL COMMENT '0：销量排序 1:CTR排序',
  \`create_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE INDEX \`uk_cmt_fav\`(\`goods_id\`,\`buyer_id\`, \`comment_id\`, \`mct_id\`,\`first_cat_id\`,\`order_type\`) USING BTREE COMMENT '一个排序方法下同一个商家的同一个品类的同一个产品的评价唯一'
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '好评数据' ROW_FORMAT = Dynamic;

CREATE TABLE IF NOT EXISTS \`themis\`.\`comments_favourable\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` bigint(20) NOT NULL COMMENT '商品id',
  \`buyer_id\` bigint(20) NOT NULL COMMENT 'buyer id',
  \`comment_id\` bigint(20) NOT NULL COMMENT 'comment id',
  \`mct_id\` int(11) NOT NULL COMMENT 'merchant id',
  \`first_cat_id\` int(11) NOT NULL COMMENT '商品一级类目id，0表示全部',
  \`rank\` int(11) NOT NULL COMMENT '排名',
  \`order_type\` int(1) NOT NULL COMMENT '0：销量排序 1:CTR排序',
  \`create_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE INDEX \`uk_cmt_fav\`(\`goods_id\`,\`buyer_id\`, \`comment_id\`, \`mct_id\`,\`first_cat_id\`,\`order_type\`) USING BTREE COMMENT '一个排序方法下同一个商家的同一个品类的同一个产品的评价唯一'
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '好评数据' ROW_FORMAT = Dynamic;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table comments_favourable_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_comment_favourable \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns goods_id,buyer_id,comment_id,mct_id,first_cat_id,rank,order_type \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.comments_favourable to themis.comments_favourable_pre,themis.comments_favourable_new to themis.comments_favourable;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
