#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.ads_goods_portrait_brand_price_range_likes_top20_pre;
drop table if exists themis.ads_goods_portrait_brand_price_range_likes_top20_now;
create table themis.ads_goods_portrait_brand_price_range_likes_top20_now(
\`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`goods_id\`                    bigint COMMENT '商品id',
\`brand_id\`                    int COMMENT '品牌id',
\`price_range\`                 int COMMENT '价格区间id',
\`rk\`                          int COMMENT '排名',
\`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
INDEX \`tag_id\` (\`brand_id\`,\`price_range\`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品画像品牌加价格区间gmv top20统计表';
create table if not exists themis.ads_goods_portrait_brand_price_range_likes_top20(
\`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`goods_id\`                    bigint COMMENT '商品id',
\`brand_id\`                    int COMMENT '品牌id',
\`price_range\`                 int COMMENT '价格区间id',
\`rk\`                          int COMMENT '排名',
\`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
INDEX \`tag_id\` (\`brand_id\`,\`price_range\`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品画像品牌加价格区间gmv top20统计表';
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table ads_goods_portrait_brand_price_range_likes_top20_now \
--hcatalog-database ads \
--hcatalog-table ads_vova_goods_portrait_brand_price_range_likes_top20 \
--columns goods_id,brand_id,price_range,rk \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_goods_portrait_brand_price_range_likes_top20 to themis.ads_goods_portrait_brand_price_range_likes_top20_pre,themis.ads_goods_portrait_brand_price_range_likes_top20_now to themis.ads_goods_portrait_brand_price_range_likes_top20;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
