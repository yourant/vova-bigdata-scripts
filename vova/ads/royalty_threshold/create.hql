[9531]招商提成商品数据 报表需求

需要有算法同学计算一个提成阈值;
即: 斜率取-1的时候的模型y值; 拟合出来的点的值向下取整

-- 取数部分
drop table ads.ads_vova_royalty_threshold_detail_d;
CREATE external TABLE ads.ads_vova_royalty_threshold_detail_d
(
    datasource           string        COMMENT 'datasource',
    first_cat_id         bigint        COMMENT '一级品类ID',
    region_code          string        COMMENT '国家code',
    group_id             bigint        COMMENT '商品组id',
    gmv                  decimal(14,4) COMMENT 'gmv'
) COMMENT '提成阈值计算取数' PARTITIONED BY (pt STRING)
LOCATION "s3://vova-mlb/REC/data/vova_fd_salary/vova/threshold_get/"
STORED AS PARQUETFILE;

取数执行完成后发消息：
sh /mnt/vova-bigdata-scripts/common/job_message_put.sh
--jname=ads_vova_royalty_threshold_detail_d --from=data --to=java_server --jtype=1D --retry=0


-- 算法 输出表：
drop table ads.ads_vova_royalty_threshold_d;
CREATE external TABLE ads.ads_vova_royalty_threshold_d
(
    datasource           string        COMMENT 'datasource',
    first_cat_id         bigint        COMMENT '一级品类ID',
    region_code          string        COMMENT '国家code',
    month_sale_threshold decimal(14,4) COMMENT '月销额阈值',
    rank_threshold       decimal(14,4) COMMENT '商品序数阈值'
) COMMENT '提成阈值计算取数' PARTITIONED BY (pt STRING)
LOCATION "s3://vova-mlb/REC/data/vova_fd_salary/vova/threshold_out/"
STORED AS PARQUETFILE;

算法执行完成后发消息：
sh /mnt/vova-bigdata-scripts/common/job_message_get.sh
--jname=ads_vova_royalty_threshold_d --from=mlb --to=data --jtype=1D --retry=0






