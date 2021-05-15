【数据】[9034]知识图谱个性化热榜数据计算
根据算法提供的mlb.mlb_vova_rec_m_user_kg_tag_d的用户对应的榜单，拆分出每个榜单
并将每个榜单对应的goods_id进行排序
创建时间及开发人员：2020/4/24,戴飞俊

drop table if exists ads.ads_vova_goods_attribute_label_data;
CREATE EXTERNAL TABLE `ads.ads_vova_goods_attribute_label_data`(
goods_id bigint COMMENT '商品id',
attr_key string COMMENT '属性key',
attr_value string COMMENT '属性value',
cat_attr_id bigint COMMENT '类目属性分类id',
second_cat_id BIGINT COMMENT '品类id'
) COMMENT '知识图谱-商品属性标签数据' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;