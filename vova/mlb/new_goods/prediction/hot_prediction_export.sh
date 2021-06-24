#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
drop table if exists themis.new_goods_predicte_result_new;
drop table if exists themis.new_goods_predicte_result_pre;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

sql="
CREATE TABLE IF NOT EXISTS themis.new_goods_predicte_result_new
(
    id               int(11)       NOT NULL AUTO_INCREMENT,
    goods_id         bigint(20)    NOT NULL COMMENT '商品id',
    cat_id           int(20)       NOT NULL COMMENT 'cat_id',
    predicte_score   decimal(18, 10) NOT NULL COMMENT '预测结果',
    rank             int(20)       NOT NULL COMMENT 'rank',
    create_time      timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY goods_id (goods_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table new_goods_predicte_result_new \
--m 1 \
--columns goods_id,cat_id,predicte_score,rank \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_new_goods_predicte_result \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.new_goods_predicte_result to themis.new_goods_predicte_result_pre,
             themis.new_goods_predicte_result_new to themis.new_goods_predicte_result;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

