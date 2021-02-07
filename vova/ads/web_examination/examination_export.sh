#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date +"%Y-%m-%d"`
fi

sql="
drop table if exists backend.vova_web_goods_examination_new;
drop table if exists backend.vova_web_goods_examination_pre;
"

mysql -h rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pkkooxGjFy7Vgu21x -e "${sql}"

sql="
CREATE TABLE backend.vova_web_goods_examination_new
(
    id               int(11)        NOT NULL AUTO_INCREMENT,
    goods_id           bigint(20)     NOT NULL COMMENT '商品id',
    cat_id             bigint(20)     NOT NULL COMMENT 'cat_id',
    first_cat_id      bigint(20)     NOT NULL COMMENT 'first_cat_id',
    second_cat_id     bigint(20)     NOT NULL COMMENT 'second_cat_id',
    impressions        bigint(20)     NOT NULL DEFAULT '0' COMMENT '测试impressions',
    ctr                decimal(20, 6) NOT NULL DEFAULT '0.00' COMMENT '测试ctr',
    gcr                decimal(20, 6) NOT NULL DEFAULT '0.00' COMMENT '测试gcr',
    gmv_cr             decimal(20, 6) NOT NULL DEFAULT '0.00' COMMENT 'gmv/曝光量',
    goods_score       decimal(20, 6) NOT NULL DEFAULT '0.00' COMMENT '测试goods_score,（点击量+加车量+销量*2）*100/曝光量',
    gcr_1w             decimal(20, 6) NOT NULL DEFAULT '0.00' COMMENT '近1周gcr',
    gmv_cr_1w          decimal(20, 6) NOT NULL DEFAULT '0.00' COMMENT 'gmv/曝光量',
    impressions_1w     bigint(20) NOT NULL DEFAULT '0.00' COMMENT '近1周impressions',
    test_goods_status  tinyint(4)   NOT NULL DEFAULT '0' COMMENT '测款状态',
    test_goods_status_comment  varchar(30)   NOT NULL DEFAULT '' COMMENT '测款状态: 1:first_level_testing, 2:first_level_finished, 3:second_level_finished, 4:third_level_finished',
    test_goods_result_status  tinyint(4)   NOT NULL DEFAULT '0' COMMENT '测款结果状态',
    test_goods_result_comment  varchar(30)   NOT NULL DEFAULT '' COMMENT '测款结果: 1:pending, 2:first_level_success, 3:first_level_failure, 4:second_level_success, 5:second_level_failure, 6:third_level_success, 7:third_level_failure',
    add_test_time      datetime       NOT NULL DEFAULT '1970-01-01 00:00:01' COMMENT '进入测款时间',
    status_change_time datetime       NOT NULL DEFAULT '1970-01-01 00:00:01' COMMENT '最近一次测款状态变更时间',
    create_time        timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time   timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY goods_id (goods_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 ;
"

mysql -h rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pkkooxGjFy7Vgu21x -e "${sql}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com/backend?rewriteBatchedStatements=true \
--username bimaster --password kkooxGjFy7Vgu21x \
--table vova_web_goods_examination_new \
--m 1 \
--columns goods_id,cat_id,impressions,ctr,gmv_cr,goods_score,gmv_cr_1w,impressions_1w,test_goods_status,test_goods_status_comment,test_goods_result_status,test_goods_result_comment,add_test_time,status_change_time,gcr,gcr_1w,first_cat_id,second_cat_id  \
--hcatalog-database ads \
--hcatalog-table ads_vova_web_goods_examination_summary_history_export \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch



#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pkkooxGjFy7Vgu21x <<EOF
rename table backend.vova_web_goods_examination to backend.vova_web_goods_examination_pre,
             backend.vova_web_goods_examination_new to backend.vova_web_goods_examination;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

