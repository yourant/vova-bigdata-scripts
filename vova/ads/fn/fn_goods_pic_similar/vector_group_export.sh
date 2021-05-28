#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
drop table if exists trigram_shopping.min_price_goods_new;
drop table if exists trigram_shopping.min_price_goods_pre;
"

mysql -h trigram-fn.cn899du7tges.us-east-1.rds.amazonaws.com -u datagw2021052812 -pIen3aingae%w2xa5OhCei -e "${sql}"

sql="
CREATE TABLE trigram_shopping.min_price_goods_new
(
    id                 int(11)     NOT NULL AUTO_INCREMENT,
    goods_id           bigint(20)  NOT NULL COMMENT 'fn商品id',
    min_price_goods_id bigint(20)  NOT NULL COMMENT 'fn低价商品id',
    strategy           varchar(60) NOT NULL DEFAULT '' COMMENT '低价策略目前只有lowest_price',
    group_number       varchar(60) NOT NULL DEFAULT '' COMMENT '分组号',
    min_show_price     decimal(14, 2)  NOT NULL DEFAULT '0.00' COMMENT '低价商品的价格',
    create_time        timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time   timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY goods_id (goods_id),
    KEY min_price_goods_id (min_price_goods_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
"

mysql -h trigram-fn.cn899du7tges.us-east-1.rds.amazonaws.com -u datagw2021052812 -pIen3aingae%w2xa5OhCei -e "${sql}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://trigram-fn.cn899du7tges.us-east-1.rds.amazonaws.com/trigram_shopping?rewriteBatchedStatements=true \
--username datagw2021052812 --password Ien3aingae%w2xa5OhCei \
--table min_price_goods_new \
--m 1 \
--columns goods_id,min_price_goods_id,strategy,group_number,min_show_price \
--hcatalog-database ads \
--hcatalog-table fn_ads_min_price_goods \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch



#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h trigram-fn.cn899du7tges.us-east-1.rds.amazonaws.com -u datagw2021052812 -pIen3aingae%w2xa5OhCei <<EOF
rename table trigram_shopping.min_price_goods to trigram_shopping.min_price_goods_pre,
             trigram_shopping.min_price_goods_new to trigram_shopping.min_price_goods;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1datagw2021052812
fi