#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists search.fn_ads_goods_portrait_pre;
drop table if exists search.fn_ads_goods_portrait_new;
create table if not exists search.fn_ads_goods_portrait_new(
\`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`goods_id\`         int              COMMENT '商品id',
\`datasource\`       varchar(10)      COMMENT '数据源',
\`cat_id\`           int              COMMENT '品类id',
\`first_cat_id\`     int              COMMENT '一级品类id',
\`price\`            decimal(13,2)    COMMENT '价格（包括运费）',
\`expre_cnt_1w\`     int              COMMENT '1周曝光量',
\`clk_cnt_1w\`       int              COMMENT '一周点击量',
\`add_cart_cnt_1w\`  int              COMMENT '一周加车量',
\`collect_cnt_1w\`   int              COMMENT '一周加车量',
\`sales_vol_1w\`     int              COMMENT '一周销量',
\`ord_cnt_1w\`       int              COMMENT '一周订单量',
\`gmv_1w\`           decimal(13,2)            COMMENT '一周gmv',
\`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
INDEX \`goods_id\` (\`goods_id\`) USING BTREE,
INDEX \`cat_id\` (\`cat_id\`) USING BTREE,
INDEX \`first_cat_id\` (\`first_cat_id\`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='fn商品画像表';
create table if not exists search.fn_ads_goods_portrait(
\`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`goods_id\`         int              COMMENT '商品id',
\`datasource\`       varchar(10)      COMMENT '数据源',
\`cat_id\`           int              COMMENT '品类id',
\`first_cat_id\`     int              COMMENT '一级品类id',
\`price\`            decimal(13,2)    COMMENT '价格（包括运费）',
\`expre_cnt_1w\`     int              COMMENT '1周曝光量',
\`clk_cnt_1w\`       int              COMMENT '一周点击量',
\`add_cart_cnt_1w\`  int              COMMENT '一周加车量',
\`collect_cnt_1w\`   int              COMMENT '一周加车量',
\`sales_vol_1w\`     int              COMMENT '一周销量',
\`ord_cnt_1w\`       int              COMMENT '一周订单量',
\`gmv_1w\`           decimal(13,2)    COMMENT '一周gmv',
\`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
INDEX \`goods_id\` (\`goods_id\`) USING BTREE,
INDEX \`cat_id\` (\`cat_id\`) USING BTREE,
INDEX \`first_cat_id\` (\`first_cat_id\`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='fn商品画像表';
"
mysql -hfn-rec.cznqgcwo1pjt.us-east-1.rds.amazonaws.com -udatagroup20200925 -p8SxDdHGDZyrsesCgfdEZvJmr -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
-Dsqoop.export.records.per.statement=3000 \
--connect jdbc:mysql://fn-rec.cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/search \
--username datagroup20200925 --password 8SxDdHGDZyrsesCgfdEZvJmr \
--m 2 \
--table fn_ads_goods_portrait_new \
--hcatalog-database ads \
--hcatalog-table fn_ads_goods_portrait \
--hcatalog-partition-keys pt \
--columns goods_id,datasource,cat_id,first_cat_id,price,expre_cnt_1w,clk_cnt_1w,add_cart_cnt_1w,collect_cnt_1w,sales_vol_1w,ord_cnt_1w,gmv_1w \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -hfn-rec.cznqgcwo1pjt.us-east-1.rds.amazonaws.com -udatagroup20200925 -p8SxDdHGDZyrsesCgfdEZvJmr <<EOF
rename table search.fn_ads_goods_portrait to search.fn_ads_goods_portrait_pre,search.fn_ads_goods_portrait_new to search.fn_ads_goods_portrait;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
