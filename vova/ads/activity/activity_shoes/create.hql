[9345] 母亲节活动会场新增数据源需求
https://zt.gitvv.com/index.php?m=task&f=view&taskID=34326

鞋子活动数据源
https://docs.google.com/spreadsheets/d/13gxYOib7gLwMR79bB3UygJBGjMZtebj3cKF2jubR15o/edit#gid=272309961

活动会场接入算法服务
*注
使用近7日全部行为进行计算，每日更新一次。
主流国家为：法、德、英、意、西（FR/DE/GB/IT/ES）
gsn和主图去重，取售出均价（gmv/order）最低价的
Shoes(5777)

（1）best sellers	biz_type（shoes_best）
标记	名称	筛选条件
		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
	非brand商品（主流国家）P1	≥400	≥0.013	≥0.013	≥30	≥60	<15%	分国家，英法德意西取符合条件的全部商品	模型
	非brand商品（其它国家）P1	≥500	≥0.015	≥0.015	≥50	≥60	<15%	取全站数据	模型
	非brand的top100商品（分国家）P2	分国家，英法德意西取各个国家与上一条排重后的order数top100商品，除此之外的其他国家取全站与上一条排重后的top100商品。							模型
预期展示400-600个商品


（2）new arrivals	biz_type（shoes_new）
标记	名称	筛选条件
		impressions	ctr	rate	sales_order	gcr	非物流退款率（近一个月）	取数	排序
	非brand商品（主流国家）P1	100-5000	≥0.02	≥0.015	≥2	≥100	<15%	分国家，英法德意西取符合条件的全部商品	模型
	非brand商品（其它国家）P1	100-10000	≥0.025	≥0.03	≥2	≥100	<15%	取全站数据	模型
6	增加graph embedding协同	参数和首页保持一致，500个							模型

预期展示600-1000个商品


Athletic Shoes	biz_type（shoes_athletic）
标记	名称	筛选条件
		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
	Athletic Shoes 非brand商品（主流国家）	≥100	≥0.013	≥0.01	≥15	≥100	<15%	分国家，英法德意西取符合条件的全部商品	模型
	Athletic Shoes 非brand商品（其它国家）	≥200	≥0.015	≥0.015	≥20	≥120	<15%	取全站数据	模型
预期展示250-400个商品

Casual Shoes	biz_type（shoes_casual）
标记	名称	筛选条件
		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
	Casual Shoes 非brand商品（主流国家）	≥100	≥0.013	≥0.01	≥15	≥80	<15%	分国家，英法德意西取符合条件的全部商品	模型
	Casual Shoes 非brand商品（其它国家）	≥200	≥0.015	≥0.015	≥20	≥100	<15%	取全站数据	模型
预期展示250-400个商品

Sandals	biz_type（shoes_sandals）
标记	名称	筛选条件
		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
	Sandals 非brand商品（主流国家）	≥50	≥0.01	≥0.01	≥10	≥60	<15%	分国家，英法德意西取符合条件的全部商品	模型
	Sandals 非brand商品（其它国家）	≥200	≥0.01	≥0.01	≥10	≥80	<15%	取全站数据	模型
预期展示250-400个商品

Slippers	biz_type（shoes_slippers）
标记	名称	筛选条件
		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
	Slippers 非brand商品（主流国家）	≥50	≥0.01	≥0.01	≥10	≥60	<15%	分国家，英法德意西取符合条件的全部商品	模型
	Slippers 非brand商品（其它国家）	≥200	≥0.01	≥0.01	≥10	≥80	<15%	取全站数据	模型
预期展示250-400个商品

Fashion & Dress Shoes	biz_type（shoes_fashion）
标记	名称	筛选条件
		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
	Fashion & Dress Shoes 非brand商品（主流国家）	≥50	≥0.01	≥0.01	≥10	≥60	<15%	分国家，英法德意西取符合条件的全部商品	模型
	Fashion & Dress Shoes 非brand商品（其它国家）	≥200	≥0.01	≥0.01	≥10	≥80	<15%	取全站数据	模型
预期展示250-400个商品

Boots	biz_type（shoes_boots）
标记	名称	筛选条件
		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
	Boots 非brand商品（主流国家）	≥50	≥0.01	≥0.01	≥10	≥60	<15%	分国家，英法德意西取符合条件的全部商品	模型
	Boots 非brand商品（其它国家）	≥200	≥0.01	≥0.01	≥10	≥80	<15%	取全站数据	模型

有效期6个月

create table if  not exists ads.ads_vova_activity_shoes (
  goods_id                  bigint COMMENT 'i_商品ID',
  region_id                 int    COMMENT 'i_国家id',
  biz_type                  STRING COMMENT 'i_biz type',
  rp_type                   int    COMMENT 'i_rp type',
  first_cat_id              int    COMMENT 'd_一级品类id',
  second_cat_id             int    COMMENT 'd_二级品类id',
  rank                      bigint COMMENT 'd_排名'
)COMMENT '鞋子活动' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;

