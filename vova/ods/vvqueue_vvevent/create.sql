create external TABLE ods_vova_ext.ods_vova_vvqueue_vvevent_arc
(
json_str                string      COMMENT 'json_str'
) COMMENT '留存红包' PARTITIONED BY (pt STRING,hour STRING)
LOCATION "s3://bigdata-offline/warehouse/pdb/vova/vvqueue/vvqueue-vvevent"
;

drop table if exists ods_vova_ext.ods_vova_vvqueue_vvevent;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_vova_ext.ods_vova_vvqueue_vvevent
(
    project           string,
    plat_form         string,
    event_type        string,
    event_fingerprint string,
    device_id         string,
    uid               string,
    language          string,
    cur_time          string,
    extra             string
) COMMENT '留存红包'
    PARTITIONED BY (pt string);