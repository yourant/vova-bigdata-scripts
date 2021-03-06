create table if not exists dwb.dwb_fd_goods_test_channel (
project                             string COMMENT '组织',
cat_name                            string COMMENT '品类名',
end_day                             string COMMENT '测款结束日期',
selection_mode                      string COMMENT '选款方式',
selection_channel                   string COMMENT '选款渠道',
channel_type                        string COMMENT '渠道类型',
virtual_goods_id                    bigint comment '商品虚拟id',
test_result                         string COMMENT '测试结果',
last_7_days_goods_sales             decimal(15,4) COMMENT '成功商品近7天销售额',
last_7_days_cat_sales               decimal(15,4) COMMENT '成功商品同品类商品近7天销售额',
sale_rate                           decimal(15,4) COMMENT '成功商品近7天销售额与同品类销售额占比'

) comment '测款渠道数据报表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");
