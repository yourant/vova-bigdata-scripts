drop table mlb.mlb_vova_rec_b_g_base_d;
CREATE external TABLE mlb.mlb_vova_rec_b_g_base_d
(
    datasource       string comment '数据平台',
    goods_id         BIGINT COMMENT '商品ID',
    virtual_goods_id BIGINT COMMENT '商品虚拟ID',
    cp_goods_id      BIGINT COMMENT '克隆商品ID',
    brand_id         BIGINT COMMENT '侵权商品',
    goods_sn         string COMMENT '商品所属sn',
    goods_name       string COMMENT '商品名称',
    goods_desc       string COMMENT '商品描述',
    sale_status      string comment '销售状态',
    keywords         string COMMENT '关键词',
    add_time         timestamp COMMENT '添加时间',
    is_on_sale       BIGINT COMMENT '真实是否在售,1:已上架，0：已下架',
    is_complete      BIGINT comment '编辑是否完成',
    is_new           BIGINT COMMENT '是否是新品',
    cat_id           BIGINT COMMENT '商品类目ID',
    first_cat_id     BIGINT COMMENT '商品一级类目',
    first_cat_name   string COMMENT '商品一级类目',
    second_cat_id    BIGINT COMMENT '商品二级类目',
    second_cat_name  string COMMENT '商品二级类目',
    third_cat_id     BIGINT COMMENT '商品三级类目',
    third_cat_name   string COMMENT '商品三级类目',
    mct_id           BIGINT COMMENT '商品所属商家',
    shop_price       DECIMAL(14, 4) comment '商品价格',
    shipping_fee     DECIMAL(14, 4) comment '商品运费',
    goods_weight     DECIMAL(14, 4) comment '商品重量',
    first_on_time    timestamp      comment '第一次上线时间',
    first_off_time   timestamp      comment '第一次下线时间',
    last_on_time     timestamp      comment '最后一次上线时间',
    last_off_time    timestamp      comment '最后一次下线时间',
    goods_thumb      string         comment '商品主图'
) COMMENT 'dim_goods商品维度每天在架商品快照' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE
LOCATION "s3://vova-mlb/REC/data/base/rec_goods_base/"
;

sh update.sh
sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=ads_rec_b_g_base_d --from=data --to=java_server --jtype=1D --retry=0

sh /mnt/vova-bigdata-scripts/common/job_message_get.sh --jname=ads_rec_b_g_base_d --from=data --to=java_server --valid_hour=1


msck repair table mlb.mlb_vova_rec_b_g_name_emb_d;
create external TABLE mlb.mlb_vova_rec_b_g_name_emb_d
(
    goods_id       bigint COMMENT '商品id',
    goods_name_emb string COMMENT '商品标题embedding'
) COMMENT '商品标题embedding表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/base/pre_emb/rec_g_name_emb_d/"
STORED AS textfile;



