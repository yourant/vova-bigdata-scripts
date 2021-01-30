#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
drop table if exists themis.flash_sale_coupon_goods_new;
drop table if exists themis.flash_sale_coupon_goods_pre;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

sql="
CREATE TABLE IF NOT EXISTS themis.flash_sale_coupon_goods_new
(
    id               int(11)        unsigned NOT NULL AUTO_INCREMENT,
    goods_id         int(20)        NOT NULL DEFAULT '0' COMMENT '商品id',
    first_cat_id     int(10)        NOT NULL DEFAULT '0' COMMENT 'cat_id',
    gmv              decimal(15, 2) NOT NULL DEFAULT '0' COMMENT 'gmv',
    gcr              decimal(15, 4) NOT NULL DEFAULT '0' COMMENT 'gcr',
    clicks           int(10)        NOT NULL DEFAULT '0' COMMENT 'clicks',
    gmv_rank         int(11)        NOT NULL DEFAULT '0' COMMENT 'gmv_rank gmv desc',
    gcr_rank         int(11)        NOT NULL DEFAULT '0' COMMENT 'gcr_rank gcr desc',
    create_time      timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY goods_id (goods_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--table flash_sale_coupon_goods_new \
--m 1 \
--columns goods_id,first_cat_id,gmv_rank,gcr_rank,gmv,gcr,clicks \
--hcatalog-database ads \
--hcatalog-table ads_flash_sale_coupon_goods \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.flash_sale_coupon_goods to themis.flash_sale_coupon_goods_pre,
             themis.flash_sale_coupon_goods_new to themis.flash_sale_coupon_goods;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi