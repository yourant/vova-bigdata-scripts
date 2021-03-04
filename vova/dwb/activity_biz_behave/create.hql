drop table if exists dwb.dwb_vova_activity_biz_behave;
create table if  not exists dwb.dwb_vova_activity_biz_behave (
    `biz_type`                      string        COMMENT  'biz_type',
    `first_cat_name`                string        COMMENT  '一级品类名称',
    `second_cat_name`               string        COMMENT  '二级品类名称',
    `brand`                         string        COMMENT  '是否brand',
    `goods_cnt`                     int           COMMENT  '商品数量',
    `avg_price`                     decimal(13,2) COMMENT  '商品均价',
    `on_sale_goods_cnt`             int           COMMENT '在架商品数量',
    `gmv`                           decimal(13,2) COMMENT 'GMV',
    `sale_avg_price`                decimal(13,2) COMMENT '出单商品均价'
) COMMENT '货品池数量监控报表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;


CREATE TABLE `dwb`.`dwb_vova_dictionary_activity_map`(
  `table_name` string COMMENT '活动表名',
  `desc` string COMMENT '活动简介',
  `is_on` int COMMENT '是否匹配');

insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_ac_brand','ac brand activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_best_sale','super sale activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_clearance_sale','clearance sale activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_daily_selection','daily selection activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_home_garden','home garden activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_men_cloth_and_shoes','men cloth and shoes activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_new_user','new user activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_outlets','outlets activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_women_cloth','women cloth activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_women_clothing_clearance_sale','women cloth clearance activity',1);
insert into table  `dwb`.`dwb_vova_dictionary_activity_map` values('ads.ads_vova_activity_chinese_new_year','chinese new year activity',0);