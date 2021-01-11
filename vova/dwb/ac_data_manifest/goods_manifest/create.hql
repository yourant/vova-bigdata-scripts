--【数据】[4946]AC商品表现数据报表
-- 筛选项
-- 时间	默认展示前一日当日数据	1日，3日，5日
-- 国家	默认展示all	展示top20国家	gb,fr,de,it,es,nl,pt,es,us,cs,pl,be,mx,si,ru,jp,br,tw,na,au
-- 平台	默认展示all	all，Android，ios，web
-- 店铺	默认展示all
--
-- 商品数据表现（统计有销量商品数据）
-- 							click/impression		加购UV/商详页UV			"订单状态：
-- sku_pay_status 2-已支付"	下单UV/商详UV		销量/impression
-- 时间	平台	二级品类	国家	goods id	店铺	impression	click	CTR	加购UV	加购率	下单UV	支付UV	商详至下单转化率	销量	CR

-- [5264]— AC数据报表- 商品数据表现
-- AC数据报表- 商品数据表现 添加商品gmv一列 记录商品销售产生的gmv（加在销量列后）

AC数据报表- 商品数据表现

Drop table dwb.dwb_vova_goods_manifest;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_goods_manifest (
datasource                string                COMMENT 'd_datasource',
region_code               string                COMMENT 'd_国家',
source_platform           string                COMMENT 'd_平台:all,Android,ios,web',
mct_id                    string                COMMENT 'd_店铺',
last_days                 string                COMMENT 'd_近N天',
goods_id                  string                COMMENT 'd_商品id',
second_cat_name           string                COMMENT 'i_二级品类名称',
impression_pv             bigint                COMMENT 'i_商品曝光量',
click_pv                  bigint                COMMENT 'i_商品点击量',
add_cart_uv               bigint                COMMENT 'i_加购UV',
pd_uv                     bigint                COMMENT 'i_商详页UV',
order_uv                  bigint                COMMENT 'i_下单UV',
pay_uv                    bigint                COMMENT 'i_支付UV',
goods_number              bigint                COMMENT 'i_销量',
gmv                       DECIMAL(14, 4)        COMMENT 'i_gmv'
) COMMENT '商品数据表现（统计有销量商品数据）' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_manifest/"
;




