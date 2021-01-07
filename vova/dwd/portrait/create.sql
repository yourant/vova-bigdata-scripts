CREATE TABLE dwd.dwd_vova_fact_buyer_portrait_base
(
    datasource   STRING,
    buyer_id     BIGINT,
    tag_id       STRING,
    tag_name     STRING,
    cnt          BIGINT,
    first_cat_id BIGINT,
    act_type_id  BIGINT
)
    PARTITIONED BY (pt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
   ;

DROP TABLE IF EXISTS dwd.dwd_vova_buyer_portrait_weight;
CREATE TABLE dwd.dwd_vova_buyer_portrait_weight
(
    datasource        string COMMENT '数据平台',
    act_type_id       bigint COMMENT '1：购买行为，2：浏览行为，3：评论行为，4：收藏行为，5：取消收藏行为，6：加入购物车行为，7：退款行为；，8：语言、国家，9:活跃时间段',
    act_weight_detail decimal(10, 5) COMMENT '行为权重',
    is_time_reduce    bigint COMMENT '是否时间衰减,0=false,1=true',
    prefer_type       bigint COMMENT '偏好类型，1:brand,2:category,3:price',
    create_time       timestamp COMMENT '创建时间'
) COMMENT '买家画像权重表 '
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' NULL DEFINED AS '' STORED AS PARQUET;

-- 品牌偏好
insert into dwd.dwd_vova_buyer_portrait_weight
select 'vova', 2, 1, 1, 1, current_timestamp();
insert into dwd.dwd_vova_buyer_portrait_weight
select 'vova', 4, 3, 1, 1, current_timestamp();
insert into dwd.dwd_vova_buyer_portrait_weight
select 'vova', 6, 4, 1, 1, current_timestamp();
insert into dwd.dwd_vova_buyer_portrait_weight
select 'vova', 1, 5, 1, 1, current_timestamp();
-- 品类偏好
insert into dwd.dwd_vova_buyer_portrait_weight
select 'vova', 2, 1, 1, 2, current_timestamp();
insert into dwd.dwd_vova_buyer_portrait_weight
select 'vova', 4, 3, 1, 2, current_timestamp();
insert into dwd.dwd_vova_buyer_portrait_weight
select 'vova', 6, 5, 1, 2, current_timestamp();
insert into dwd.dwd_vova_buyer_portrait_weight
select 'vova', 1, 4, 1, 2, current_timestamp();