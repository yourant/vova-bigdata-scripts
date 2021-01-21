--商品维度汇总表
drop table if exists dws.dws_vova_goods_1d;
create table if  not exists  dws.dws_vova_goods_1d (
    `datasource`                  string COMMENT 'd_来源',
    `ctry`                        string COMMENT 'd_国家',
    `os_type`                     string COMMENT 'd_系统类型,ios|android',
    `gs_id`                       bigint COMMENT 'd_商品id',
    `gender`                      string COMMENT 'd_性别',
    `clk_uv`                      bigint COMMENT 'i_商品点击uv',
    `clk_cnt`                     bigint COMMENT 'i_商品点击量',
    `expre_uv`                    bigint COMMENT 'i_商品曝光uv',
    `expre_cnt`                   bigint COMMENT 'i_商品曝光数',
    `add_cart_uv`                 bigint COMMENT 'i_商品加购点击uv',
    `add_cart_cnt`                bigint COMMENT 'i_商品加购点击量',
    `collect_cnt`                 bigint COMMENT 'i_收藏量',
    `sr_uv`                       bigint COMMENT 'i_商品搜索点击uv',
    `sr_cnt`                      bigint COMMENT 'i_商品搜索点击量',
    `ord_uv`                      BIGINT COMMENT 'i_子订单uv',
    `ord_cnt`                     BIGINT COMMENT 'i_子订单量',
    `pay_uv`                      BIGINT COMMENT 'i_支付uv',
    `pay_cnt`                     BIGINT COMMENT 'i_支付数量',
    `sales_vol`                   BIGINT COMMENT 'i_销量',
    `gmv`                         double COMMENT 'i_gmv',
    `shop_price`                  double COMMENT 'i_价格',
    `cat_id`                      bigint COMMENT 'i_商品类目id',
    `first_cat_id`                bigint COMMENT 'i_商品第一类目id',
    `first_cat_name`              string COMMENT 'i_商品第一类目name'
) PARTITIONED BY ( pt string)
 COMMENT '商品与（datasource，ctry，os_type，gender可选）联合维度表'
     STORED AS PARQUETFILE;

