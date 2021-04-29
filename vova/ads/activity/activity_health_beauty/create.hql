[9345] 母亲节活动会场新增数据源需求
https://zt.gitvv.com/index.php?m=task&f=view&taskID=34326

美妆活动数据源
https://docs.google.com/spreadsheets/d/13gxYOib7gLwMR79bB3UygJBGjMZtebj3cKF2jubR15o/edit#gid=1304577585

活动会场接入算法服务
*注
使用近7日全部行为进行计算，每日更新一次。
主流国家为：法、德、英、意、西（FR/DE/GB/IT/ES）
gsn和主图去重，取售出均价（gmv/order）最低价的
Health & Beauty(5769)

按照gcr排序
 （1）best sellers	biz_type（hb_best）
 标记	名称	筛选条件
 		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
 	非brand商品（主流国家）P1	≥400	≥0.015	≥0.015	≥20	≥40	<15%	分国家，英法德意西取符合条件的全部商品	模型
 	非brand商品（其它国家）P1	≥500	≥0.015	≥0.015	≥20	≥60	<15%	取全站数据	模型
 	非brand的top100商品（分国家）P2	分国家，英法德意西取各个国家与上一条排重后的order数top100商品，除此之外的其他国家取全站与上一条排重后的top100商品。							模型
 预期展示400-600个商品

 （2）new arrivals	biz_type（hb_new）
 标记	名称	筛选条件
 		impressions	ctr	rate	sales_order	gcr	非物流退款率（近一个月）	取数	排序
 	非brand商品（主流国家）P1	100-5000	≥0.025	≥0.02	≥2	≥40	<15%	分国家，英法德意西取符合条件的全部商品	模型
 	非brand商品（其它国家）P1	100-10000	≥0.03	≥0.03	≥2	≥60	<15%	取全站数据	模型
 6	增加graph embedding协同	参数和首页保持一致，500个							模型
 									模型
 预期展示600-1000个商品							过滤高退款率表

 （3）Makeup	biz_type（hb_makeup）
 标记	名称	筛选条件
 		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
 	Makeup 非brand商品（主流国家）	≥50	≥0.015	≥0.015	≥5	≥30	<15%	分国家，英法德意西取符合条件的全部商品	模型
 	Makeup 非brand商品（其它国家）	≥100	≥0.015	≥0.015	≥15	≥30	<15%	取全站数据	模型
 预期展示250-400个商品


 （4）Fragrances & Deodorants	biz_type（hb_fragrances）
 标记	名称	筛选条件
 		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
 	Fragrances & Deodorants 非brand商品（主流国家）	≥50	≥0.013	≥0.01		≥40	<15%	分国家，英法德意西取符合条件的全部商品	模型
 	Fragrances & Deodorants 非brand商品（其它国家）	≥100	≥0.013	≥0.01	≥10	≥40	<15%	取全站数据	模型
 预期展示250-400个商品


 （5）Nails & Tools	biz_type（hb_nails）
 标记	名称	筛选条件
 		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
 	Nails & Tools 非brand商品（主流国家）	≥50	≥0.015	≥0.015	≥5	≥35	<15%	分国家，英法德意西取符合条件的全部商品	模型
 	Nails & Tools 非brand商品（其它国家）	≥100	≥0.015	≥0.015	≥10	≥35	<15%	取全站数据	模型
 预期展示250-400个商品

 （6）Health Care	biz_type（hb_care）
 标记	名称	筛选条件
 		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
 	Health Care 非brand商品（主流国家）	≥50	≥0.015	≥0.015	≥5	≥40	<15%	分国家，英法德意西取符合条件的全部商品	模型
 	Health Care 非brand商品（其它国家）	≥100	≥0.015	≥0.015	≥10	≥40	<15%	取全站数据	模型
 预期展示250-400个商品

 （7）Hair Care & Styling	biz_type（hb_hair）
 标记	名称	筛选条件
 		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
 	Hair Care & Styling 非brand商品（主流国家）	≥50	≥0.01	≥0.01		≥35	<15%	分国家，英法德意西取符合条件的全部商品	模型
 	Hair Care & Styling 非brand商品（其它国家）	≥100	≥0.015	≥0.01		≥45	<15%	取全站数据	模型
 预期展示250-400个商品

 （8）Skin Care	biz_type（hb_skin）
 标记	名称	筛选条件
 		impressions	ctr	rate	gmv	gcr	非物流退款率（近一个月）	取数	排序
 	Skin Care 非brand商品（主流国家）	≥50	≥0.01	≥0.01		≥35	<15%	分国家，英法德意西取符合条件的全部商品	模型
 	Skin Care 非brand商品（其它国家）	≥100	≥0.015	≥0.015		≥40	<15%	取全站数据	模型
 预期展示250-400个商品

有效期6个月


create table if  not exists ads.ads_vova_activity_health_beauty (
    goods_id                  bigint COMMENT 'i_商品ID',
    region_id                 int    COMMENT 'i_国家id',
    biz_type                  STRING COMMENT 'i_biz type',
    rp_type                   int    COMMENT 'i_rp type',
    first_cat_id              int    COMMENT 'd_一级品类id',
    second_cat_id             int    COMMENT 'd_二级品类id',
    rank                      bigint COMMENT 'd_排名'
)COMMENT '美妆活动' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;









