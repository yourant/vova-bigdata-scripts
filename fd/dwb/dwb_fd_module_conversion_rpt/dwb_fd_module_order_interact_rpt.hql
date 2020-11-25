CREATE TABLE if not EXISTS `dwb.dwb_fd_module_order_interact_rpt`(
    `module_name` string COMMENT '', 
    `platform` string COMMENT '', 
    `project_name` string COMMENT '', 
    `country_code` string COMMENT '', 
    `cat_id` int COMMENT '', 
    `cat_name` string COMMENT '', 
    `domain_userid` string COMMENT 'domainID', 
    `virtual_goods_id` bigint COMMENT 'ID', 
    `goods_id` bigint COMMENT 'ID', 
    `order_id` bigint COMMENT 'ID', 
    `impression_domain_userid` string COMMENT 'impression', 
    `click_domain_userid` string COMMENT 'click', 
    `pageview_domain_userid` string COMMENT 'pv', 
    `action_domain_userid` string COMMENT 'action', 
    `goods_number` int COMMENT '', 
    `shop_price` decimal(10,2) COMMENT '', 
    `order_amount` decimal(10,2) COMMENT '', 
    `shipping_fee` decimal(10,2) COMMENT '', 
    `goods_price` decimal(10,2) COMMENT '')
COMMENT 'banner'
PARTITIONED BY ( 
`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc
TBLPROPERTIES ("orc.compress"="SNAPPY");



INSERT overwrite table dwb.dwb_fd_module_order_interact_rpt partition (pt = '${hiveconf:pt}')
SELECT s.module_name,
       s.platform_type,
       s.project                     as project_name,
       s.country,
       t.cat_id,
       t.cat_name,
       s.domain_userid,
       t.virtual_goods_id,
       t.goods_id,
       t.order_id,
       s.impression_duid,
       s.click_duid,
       s.pv_duid,
       s.action_duid,
       t.goods_number,
       t.shop_price,
       t.order_amount,
       t.shipping_fee,
       t.goods_number * t.shop_price as goods_price
FROM (
         SELECT module_name,
                domain_userid,
                collect_set(project)[0]                                                        as project,
                collect_set(platform_type)[0]                                                  as platform_type,
                collect_set(country)[0]                                                        as country,
                IF(sum(IF(event_step = 'module_impression', 1, 0)) > 0, domain_userid, '')     as impression_duid,
                IF(sum(IF(event_step = 'module_click', 1, 0)) > 0, domain_userid, '')          as click_duid,
                IF(sum(IF(event_step = 'module_pv', 1, 0)) > 0, domain_userid, '')             as pv_duid,
                IF(sum(IF(event_step in ('add', 'goods_click'), 1, 0)) > 0, domain_userid, '') as action_duid
         FROM dwb.dwb_fd_common_module_interact
         WHERE pt = '${hiveconf:pt}' AND module_name IS NOT NULL AND module_name <> ''
         group by module_name, domain_userid
     ) s
         LEFT JOIN (
    SELECT interact_duids.module_name,
           interact_duids.domain_userid,
           interact_duids.platform_type,
           interact_duids.country,
           order_duids.goods_id,
           order_duids.virtual_goods_id,
           order_duids.order_id,
           order_duids.cat_id,
           order_duids.cat_name,
           order_duids.goods_number,
           order_duids.shop_price,
           order_duids.order_amount,
           order_duids.shipping_fee
    FROM (
             SELECT module_name,
                    domain_userid,
                    goods_id,
                    collect_set(platform_type)[0]  as platform_type,
                    collect_set(country)[0]        as country
             FROM dwb.dwb_fd_common_module_interact
             WHERE event_name IN ('goods_click', 'add')
               and pt >= date_add('${hiveconf:pt}',-3)
               and pt <= '${hiveconf:pt}'
             GROUP BY module_name, domain_userid, goods_id
         ) interact_duids
             INNER JOIN (
        SELECT sp_duid,
               virtual_goods_id,
               goods_id,
               order_id,
               cat_id,
               cat_name,
               goods_number,
               shop_price,
               order_amount,
               shipping_fee,
               project_name
        FROM dwd.dwd_fd_order_goods_info
        WHERE pt = '${hiveconf:pt}' and (date(from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:pt}'
or date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:pt}'
or date(from_unixtime(event_date,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:pt}')
    ) order_duids ON order_duids.sp_duid = interact_duids.domain_userid
        AND order_duids.virtual_goods_id = interact_duids.goods_id
) t ON t.module_name = s.module_name AND t.domain_userid = s.domain_userid;
