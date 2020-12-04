
CREATE TABLE IF NOT EXISTS `dwb`.`dwb_fd_landing_check`
(
    `project`              string,
    `platform_type`        string,
    `country`              string,
    `page_url`             string,
    `url_position`         string,
    `url_virtual_goods_id` string,
    `absolute_position`    string,
    `virtual_goods_id`     string,
    `impression_uv`        bigint,
    `impression`           bigint
)
    PARTITIONED BY (
        `pt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUET;

-- ---
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- ---
--
-- INSERT overwuurite table `dwb`.`dwb_fd_landing_check` partition (pt = "${hiveconf:pt}")
-- select project,
--        platform_type,
--        country,
--        page_url,
--        url_position,
--        url_virtual_goods_id,
--        absolute_position,
--        virtual_goods_id,
--        count(distinct session_id) as impression_uv,
--        count(*)                   as impresion,
--        pt
-- from (
--          select pt,
--                 project,
--                 platform_type,
--                 country,
--                 page_url,
--                 session_id,
--                 landing_goods.postion + 1            as url_position,
--                 landing_goods.virtual_goods_id       as url_virtual_goods_id,
--
--                 goods_event_struct.absolute_position as absolute_position,
--                 goods_event_struct.virtual_goods_id
--          from ods_fd_snowplow.ods_fd_snowplow_goods_event
--                   lateral view posexplode(split(regexp_extract(page_url, '(mid=)([[0-9]+(%|\\+)[0-9]+]+)', 2),
--                                                 '((%[0-9a-zA-Z][0-9a-zA-Z]?)|(\\+))')) landing_goods as postion, virtual_goods_id
--          where pt = "${hiveconf:pt}"
--            and instr(page_url, 'mid=') > 0
--            and mkt_source is not null
--            and event_name = "goods_impression"
--            and page_code = 'list'
--            and landing_goods.postion + 1 = goods_event_struct.absolute_position
--            and landing_goods.virtual_goods_id != goods_event_struct.virtual_goods_id) impresion_detail
-- group by pt, page_url, project, platform_type, country, url_position, url_virtual_goods_id, absolute_position,
--          virtual_goods_id;



