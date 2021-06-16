[9561]消息中心-推送报表v2-presto
任务描述
需求背景：
由于用户对推送消息的点击已经埋点，用户点击推送后的行为追踪用埋点统计。
需求内容：
https://docs.google.com/spreadsheets/d/1bn0HVljXL3liufff73-X62r6fcIa192P8S6Ba6NIZsw/edit#gid=0


需求背景及目的
需要对报表的统计逻辑进行修改


需求内容
dashboard名：消息中心-推送报表v2-presto
表一：消息中心-推送报表-presto
日期	datasource	x小时内点击	platform	region_code	config_id	main_channel	任务频次	尝试推送	上传成功量	上传成功率	推送权限打开率	推送点击数	商品曝光数	商品曝光uv	商详页曝光数	商详页曝光uv	商品曝光数(除商详)	商品曝光uv(除商详)	加购数	加购uv	下单数	下单uv	支付数	支付uv	gmv	brand_gmv	no_brand_gmv
		删掉

维度说明
筛选项	枚举值
Time range
datasource
platform
region_code
x小时内点击	当天内点击，改为限制条件
config_id
main_channel

字段说明

表名	消息中心-推送报表-presto
字段名	字段业务口径（定义、公式或逻辑）	备注	页面名称	pagecode	event_name	触发时机	元素名称	type	list_type	element_name	element_id	element_type	element_position	extra
日期			此部分需给出相应埋点
datasource
x小时内点击	删掉
platform
region_code
config_id
main_channel
任务频次
尝试推送	取api表里的数据，app_push_log
上传成功量
上传成功率
推送权限打开率
推送点击数	取埋点数据，用埋点里的element_id（传的是消息id）关联config_id，圈出对应config_id下的用户，再分别计算用户行为，消息id关联config_id		push点击	PushUserDecide	click	推送消息被点击	推送消息	normal		PushUserDecide	message_id			"""link"":"""",
 ""list_uri"":"""",
""element_content"":"""""
	商品曝光数
	商品曝光uv
	商详页曝光数
	商详页曝光uv
	商品曝光数(除商详)
	商品曝光uv(除商详)
	加购数
	加购uv
	下单数
	下单uv
	支付数
	支付uv
	点击推送当日gmv
	brand_gmv
	no_brand_gmv
	点击推送当次启动gmv	点击推送之后，当个session_id内的gmv
	brand_gmv
	no_brand_gmv

@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

CREATE TABLE IF NOT EXISTS dwb.dwb_vova_push_click_behavior
(
    datasource            string,
    platform              string,
    region_code           string,
    config_id             string,
    main_channel          string,
    job_rate              string COMMENT '任务频次',
    try_num               bigint COMMENT '尝试推送',
    push_num              bigint COMMENT '上传成功量', -- 上传成功率= push_num/try_num
    success_num           bigint COMMENT '推送成功', -- 推送权限打开率=success_num/push_num
    push_click_uv         bigint COMMENT '推送点击数',
    impressions_pv        bigint COMMENT '商品曝光数',
    impressions_uv        bigint COMMENT '商品曝光uv',
    impressions_pd_pv     bigint COMMENT '商详页曝光数',
    impressions_pd_uv     bigint COMMENT '商详页曝光uv',
    impressions_ex_pd_pv  bigint COMMENT '商品曝光数(除商详)',
    impressions_ex_pd_uv  bigint COMMENT '商品曝光uv(除商详)',
    carts                 bigint COMMENT '加购数',
    carts_uv              bigint COMMENT '加购uv',
    orders                bigint COMMENT '下单数',
    orders_uv             bigint COMMENT '下单uv',
    pays                  bigint COMMENT '支付数',
    pays_uv               bigint COMMENT '支付uv',
    gmv                   decimal(14,4) COMMENT '点击推送当日gmv',
    brand_gmv             decimal(14,4) COMMENT 'brand_gmv',
    no_brand_gmv          decimal(14,4) COMMENT 'no_brand_gmv',
    session_gmv           decimal(14,4) COMMENT '点击推送当次启动gmv',
    session_brand_gmv     decimal(14,4) COMMENT 'brand_gmv',
    session_no_brand_gmv  decimal(14,4) COMMENT 'no_brand_gmv'

) COMMENT '推送点击报表' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE
;

