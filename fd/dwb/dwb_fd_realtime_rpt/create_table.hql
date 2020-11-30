CREATE TABLE  if not exists dwb.dwb_fd_realtime_rpt (
  `derived_date` string,
  `project` string,
  `platform` string,
  `country` string,
  `h0`  DOUBLE ,
  `h1`  DOUBLE ,
  `h2`  DOUBLE ,
  `h3`  DOUBLE ,
  `h4`  DOUBLE ,
  `h5`  DOUBLE ,
  `h6`  DOUBLE ,
  `h7`  DOUBLE ,
  `h8`  DOUBLE ,
  `h9`  DOUBLE ,
  `h10` DOUBLE ,
  `h11` DOUBLE ,
  `h12` DOUBLE ,
  `h13` DOUBLE ,
  `h14` DOUBLE ,
  `h15` DOUBLE ,
  `h16` DOUBLE ,
  `h17` DOUBLE ,
  `h18` DOUBLE ,
  `h19` DOUBLE ,
  `h20` DOUBLE ,
  `h21` DOUBLE ,
  `h22` DOUBLE ,
  `h23` DOUBLE
)partitioned by(pt string,class string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
TBLPROPERTIES ("parquet.compress"="SNAPPY");