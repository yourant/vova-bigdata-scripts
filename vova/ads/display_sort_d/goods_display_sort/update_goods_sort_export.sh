#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql


sql="
drop table if exists themis.goods_display_sort_new;
"
mysql -h vovadb.cei8p8whxxwd.us-east-1.rds.amazonaws.com -u dbg20200517 -poghi3Cait3aixeGei\<gh= -e "${sql}"
sql="
CREATE TABLE IF NOT EXISTS themis.goods_display_sort_new
(
    id int(11) NOT NULL AUTO_INCREMENT,
    goods_id         int(11) UNSIGNED NOT NULL COMMENT '商品id',
    impressions      int(11)          NOT NULL DEFAULT 0 COMMENT '列表展示',
    clicks           int(11)          NOT NULL DEFAULT 0 COMMENT '列表点击',
    users            int(11)          NOT NULL DEFAULT 0 COMMENT '详情访问',
    sales_order      int(11)          NOT NULL DEFAULT 0 COMMENT '销量',
    gmv              decimal(10, 2)   NOT NULL DEFAULT 0.00 COMMENT '成交额',
    project_name     varchar(10)      NOT NULL DEFAULT 'vova',
    last_update_time timestamp        NULL     DEFAULT NULL,
    platform         varchar(10)               DEFAULT 'web',
    gender enum('male','female','unknown') DEFAULT 'unknown',
    PRIMARY KEY (id),
    UNIQUE KEY goods_id (goods_id,project_name,platform,gender),
    KEY sales_order (sales_order) USING BTREE,
    KEY gmv (gmv) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='商品排序';
"

mysql -h vovadb.cei8p8whxxwd.us-east-1.rds.amazonaws.com -u dbg20200517 -poghi3Cait3aixeGei\<gh= -e "${sql}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://vovadb.cei8p8whxxwd.us-east-1.rds.amazonaws.com/themis?rewriteBatchedStatements=true \
--username dbg20200517 --password oghi3Cait3aixeGei<gh= \
--table goods_display_sort_new \
--m 5 \
--columns goods_id,impressions,clicks,users,sales_order,gmv,project_name,last_update_time,platform,gender \
--hcatalog-database ads \
--hcatalog-table ads_vova_goods_display_sort \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists themis.goods_display_sort_pre;
"
mysql -h vovadb.cei8p8whxxwd.us-east-1.rds.amazonaws.com -u dbg20200517 -poghi3Cait3aixeGei\<gh= -e "${sql}"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h vovadb.cei8p8whxxwd.us-east-1.rds.amazonaws.com -u dbg20200517 -poghi3Cait3aixeGei\<gh= <<EOF
rename table themis.goods_display_sort to themis.goods_display_sort_pre,themis.goods_display_sort_new to themis.goods_display_sort;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
