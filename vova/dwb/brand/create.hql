drop table tmp.tmp_brand;
CREATE TABLE IF NOT EXISTS tmp.tmp_brand
(
    event_date          date COMMENT '事件发生日期',
    region_code STRING COMMENT '国家',
    platform STRING,
    first_cat_name STRING,
    is_brand STRING,
    impression_uv       bigint,
    click_uv            bigint,
    add_cart_uv         bigint,
    add_cart_success_uv bigint,
    product_detail_uv   bigint,
    pay_num             bigint,
    pay_user            bigint,
    gmv                 DECIMAL(14, 4),
    dau                 bigint
) COMMENT 'brand' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

drop table dwb.dwb_vova_brand;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_brand
(
    event_date          date COMMENT '事件发生日期',
    region_code STRING COMMENT '国家',
    platform STRING,
    first_cat_name STRING,
    total_impression_uv       bigint,
    total_click_uv            bigint,
    total_add_cart_uv         bigint,
    total_add_cart_success_uv bigint,
    total_product_detail_uv   bigint,
    total_pay_num             bigint,
    total_pay_user            bigint,
    total_gmv                 DECIMAL(15, 4),
    total_dau                 bigint,
    brand_impression_uv       bigint,
    brand_click_uv            bigint,
    brand_add_cart_uv         bigint,
    brand_add_cart_success_uv bigint,
    brand_product_detail_uv   bigint,
    brand_pay_num             bigint,
    brand_pay_user            bigint,
    brand_gmv                 DECIMAL(15, 4),
    brand_dau                 bigint,
    not_brand_impression_uv       bigint,
    not_brand_click_uv            bigint,
    not_brand_add_cart_uv         bigint,
    not_brand_add_cart_success_uv bigint,
    not_brand_product_detail_uv   bigint,
    not_brand_pay_num             bigint,
    not_brand_pay_user            bigint,
    not_brand_gmv                 DECIMAL(15, 4),
    not_brand_dau                 bigint
) COMMENT 'brand' PARTITIONED BY (pt STRING)
   STORED AS PARQUETFILE;

