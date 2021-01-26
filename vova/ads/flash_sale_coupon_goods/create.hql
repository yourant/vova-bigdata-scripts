DROP TABLE IF EXISTS ads.ads_flash_sale_coupon_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_flash_sale_coupon_goods
(
    goods_id     BIGINT COMMENT 'goods_id',
    first_cat_id BIGINT COMMENT 'first_cat_id',
    gmv          decimal(15, 2) COMMENT 'gmv',
    gcr          decimal(15, 4) COMMENT 'gcr',
    gmv_rank     BIGINT COMMENT 'gmv_rank gmv desc',
    gcr_rank     BIGINT COMMENT 'gcr_rank gcr desc'
) COMMENT 'flashsale红包商品' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS themis.flash_sale_coupon_goods
(
    id               int(11)        unsigned NOT NULL AUTO_INCREMENT,
    goods_id         int(20)        NOT NULL DEFAULT '0' COMMENT '商品id',
    first_cat_id     int(10)        NOT NULL DEFAULT '0' COMMENT 'cat_id',
    gmv_rank         int(11)        NOT NULL DEFAULT '0' COMMENT 'gmv_rank gmv desc',
    gcr_rank         int(11)        NOT NULL DEFAULT '0' COMMENT 'gcr_rank gcr desc',
    create_time      timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY goods_id (goods_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;







