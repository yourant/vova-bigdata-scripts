CREATE TABLE if not EXISTS dwb.dwb_fd_last_7_days_goods_rpt(
    pt string comment'统计时间',
    project_name string comment'组织名称',
    goods_id bigint comment'商品ID',
    goods_num bigint comment'近7天销售件数'
)comment'最近7天销售件数大于35件且下架商品的明细表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");