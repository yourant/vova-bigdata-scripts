[5958]（superset报表|AC）Facebook 商品广告花费-vo
mysql> desc facebook_goods_daily_report;
+--------------+------------------+------+-----+------------+----------------+
| Field        | Type             | Null | Key | Default    | Extra          |
+--------------+------------------+------+-----+------------+----------------+
| id           | int(11) unsigned | NO   | PRI | NULL       | auto_increment |
| account_name | varchar(128)     | NO   | MUL |            |                |
| site_code    | varchar(10)      | NO   | MUL |            |                |
| goods_id     | int(11)          | NO   | MUL | 0          |                |
| cost         | decimal(20,2)    | YES  |     | 0.00       |                |
| date         | date             | NO   | MUL | 1900-01-01 |                |
| goods_amount | decimal(20,2)    | YES  |     | 0.00       |                |
+--------------+------------------+------+-----+------------+----------------+
7 rows in set (0.00 sec)

Facebook 商品广告花费-vo
mysql数据 不迁移 不调度

mysql -h zkmarket.cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u vomktread -pvova_mkt_read
report_ychen

select
    site_code, account_name, goods_id,
    sum(cost), sum(goods_amount)

from
    facebook_goods_daily_report
group by site_code, account_name, goods_id

select
    sum(goods_amount)
from
    facebook_goods_daily_report