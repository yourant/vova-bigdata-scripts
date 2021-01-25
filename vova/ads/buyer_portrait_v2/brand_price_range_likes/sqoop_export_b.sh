#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.ads_buyer_portrait_brand_price_range_likes_top10_b_pre;
drop table if exists themis.ads_buyer_portrait_brand_price_range_likes_top10_b_now;
create table if not exists themis.ads_buyer_portrait_brand_price_range_likes_top10_b_now(
\`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`buyer_id\`                    bigint COMMENT '用户id',
\`brand_id\`                    int COMMENT '品牌id',
\`price_range\`                 int COMMENT '价格区间id',
\`rk\`                          int COMMENT '排名',
\`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
INDEX  \`buyer_id\` (\`buyer_id\`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户画像品牌加价格区间gmv top10统计表';
create table if not exists themis.ads_buyer_portrait_brand_price_range_likes_top10_b(
\`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`buyer_id\`                    bigint COMMENT '用户id',
\`brand_id\`                    int COMMENT '品牌id',
\`price_range\`                 int COMMENT '价格区间id',
\`rk\`                          int COMMENT '排名',
\`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
INDEX  \`buyer_id\` (\`buyer_id\`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品画像品牌加价格区间gmv top10统计表';
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
-Dsqoop.export.records.per.statement=3000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bimaster --password sYG2Ri3yIDu2NPki \
--m 2 \
--table ads_buyer_portrait_brand_price_range_likes_top10_b_now \
--hcatalog-database ads \
--hcatalog-table ads_buyer_portrait_brand_price_range_likes_top10_b \
--columns buyer_id,brand_id,price_range,rk \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki <<EOF
rename table themis.ads_buyer_portrait_brand_price_range_likes_top10_b to themis.ads_buyer_portrait_brand_price_range_likes_top10_b_pre,themis.ads_buyer_portrait_brand_price_range_likes_top10_b_now to themis.ads_buyer_portrait_brand_price_range_likes_top10_b;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
