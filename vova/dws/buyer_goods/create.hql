-- 用户商品联合维度用户行为统计表
drop table if exists dws.dws_vova_buyer_goods_behave;
create table if  not exists  dws.dws_vova_buyer_goods_behave (
    `buyer_id`            bigint      COMMENT 'd_用户id',
    `gs_id`               bigint      COMMENT 'd_商品id',
    `cat_id`              bigint      COMMENT 'i_品类id',
    `first_cat_id`       bigint       COMMENT 'i_一级品类id',
    `second_cat_id`       bigint      COMMENT 'i_二级品类id',
    `brand_id`            bigint      COMMENT 'i_品牌id',
    `price_range`         int         COMMENT 'i_价格区间',
    `expre_cnt`           bigint      COMMENT '1_曝光次数',
    `clk_cnt`             bigint      COMMENT 'i_点击次数',
    `clk_valid_cnt`       bigint      COMMENT 'i_有效点击次数',
    `collect_cnt`         bigint      COMMENT 'i_收藏次数',
    `add_cat_cnt`         bigint      COMMENT 'i_加购次数',
    `ord_cnt`             bigint      COMMENT 'i_购买次数',
    `sales_vol`           bigint      COMMENT 'i_销量',
    `gmv`                 decimal(13,2)      COMMENT 'i_gmv'
) PARTITIONED BY (pt string)
 COMMENT '用户商品联合维度用户行为统计表'
     STORED AS PARQUETFILE;


-- 价格区间字典表
drop table if exists tmp.tmp_vova_dictionary_price_range_type;
create table if  not exists  tmp.tmp_vova_dictionary_price_range_type (
    `id`                  bigint      COMMENT 'd_id',
    `min_val`             int         COMMENT 'i_最小值',
    `max_val`             int         COMMENT 'i_最大值',
    `comment`             string      COMMENT 'i_注释'
) COMMENT '价格区间字典表'
     STORED AS PARQUETFILE;

insert into tmp.tmp_vova_dictionary_price_range_type values(1,0,5,'[0,5)');
insert into tmp.tmp_vova_dictionary_price_range_type values(2,5,15,'[5,15)');
insert into tmp.tmp_vova_dictionary_price_range_type values(3,15,30,'[15,30)');
insert into tmp.tmp_vova_dictionary_price_range_type values(4,30,50,'[30,50)');
insert into tmp.tmp_vova_dictionary_price_range_type values(5,50,100,'[50,100)');
insert into tmp.tmp_vova_dictionary_price_range_type values(6,100,200,'[100,200)');
insert into tmp.tmp_vova_dictionary_price_range_type values(7,200,100000000,'[200,-)');