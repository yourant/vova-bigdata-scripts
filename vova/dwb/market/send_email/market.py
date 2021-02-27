# !/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb
import sys

# 打开数据库连接
db = MySQLdb.connect(host="db-logistics-w.gitvv.com", user="vvreport4vv",
                     passwd="nTTPdJhVp!DGv5VX4z33Fw@tHLmIG8oS", db="themis_logistics_report")

# 使用cursor()方法获取操作游标
cursor = db.cursor()

# SQL 查询语句
sql = """
SELECT rm.event_date,
       ifnull(rm.tot_gmv,0) + ifnull(rm.tot_lucky_gmv,0) as lucky_gmv,
       rm.tot_gmv,
       rm.tot_dau,
       rm.tot_activate as tot_install,
       rm.android_gmv,
       rm.android_dau,
       concat(round(rm.android_paid_device / rm.android_dau * 100, 2), '%')        AS android_cr,
       rm.android_activate as android_install,
       rm.ios_gmv,
       rm.ios_dau,
       concat(round(rm.ios_paid_device / rm.ios_dau * 100, 2), '%')                AS ios_cr,
       rm.ios_activate as ios_install,
       concat(round(rm.tot_android_1b_ret / tmp1.android_dau * 100, 2), '%')       AS tot_android_1d_cohort,
       concat(round(rm.tot_android_7b_ret / tmp7.android_dau * 100, 2), '%')       AS tot_android_7d_cohort,
       concat(round(rm.tot_android_28b_ret / tmp28.android_dau * 100, 2), '%')     AS tot_android_28d_cohort,
       concat(round(rm.new_android_1b_ret / tmp1.android_activate * 100, 2), '%')   AS new_android_1d_cohort,
       concat(round(rm.new_android_7b_ret / tmp7.android_activate * 100, 2), '%')   AS new_android_7d_cohort,
       concat(round(rm.new_android_28b_ret / tmp28.android_activate * 100, 2), '%') AS new_android_28d_cohort,
       concat(round(rm.tot_ios_1b_ret / tmp1.ios_dau * 100, 2), '%')               AS tot_ios_1d_cohort,
       concat(round(rm.tot_ios_7b_ret / tmp7.ios_dau * 100, 2), '%')               AS tot_ios_7d_cohort,
       concat(round(rm.tot_ios_28b_ret / tmp28.ios_dau * 100, 2), '%')             AS tot_ios_28d_cohort,
       concat(round(rm.new_ios_1b_ret / tmp1.ios_activate * 100, 2), '%')           AS new_ios_1d_cohort,
       concat(round(rm.new_ios_7b_ret / tmp7.ios_activate * 100, 2), '%')           AS new_ios_7d_cohort,
       concat(round(rm.new_ios_28b_ret / tmp28.ios_activate * 100, 2), '%')         AS new_ios_28d_cohort
FROM dwb_vova_market_process rm
         LEFT JOIN (SELECT date_add(rm1.event_date, INTERVAL 1 DAY) AS interval_1,
                            rm1.android_dau,
                            rm1.ios_dau,
                            rm1.android_install,
                            rm1.ios_install,
                            rm1.android_activate,
                            rm1.ios_activate,
                            rm1.datasource,
                            rm1.region_code
                     FROM dwb_vova_market_process rm1) AS tmp1 ON tmp1.interval_1 = rm.event_date
    AND tmp1.datasource = rm.datasource
    AND tmp1.region_code = rm.region_code
         LEFT JOIN (SELECT date_add(rm1.event_date, INTERVAL 7 DAY) AS interval_7,
                            rm1.android_dau,
                            rm1.ios_dau,
                            rm1.android_install,
                            rm1.ios_install,
                            rm1.android_activate,
                            rm1.ios_activate,
                            rm1.datasource,
                            rm1.region_code
                     FROM dwb_vova_market_process rm1) AS tmp7 ON tmp7.interval_7 = rm.event_date
    AND tmp7.datasource = rm.datasource
    AND tmp7.region_code = rm.region_code
         LEFT JOIN (SELECT date_add(rm1.event_date, INTERVAL 28 DAY) AS interval_28,
                            rm1.android_dau,
                            rm1.ios_dau,
                            rm1.android_install,
                            rm1.ios_install,
                            rm1.android_activate,
                            rm1.ios_activate,
                            rm1.datasource,
                            rm1.region_code
                     FROM dwb_vova_market_process rm1) AS tmp28
                    ON tmp28.interval_28 = rm.event_date
                        AND tmp28.datasource = rm.datasource
                        AND tmp28.region_code = rm.region_code
WHERE rm.event_date < date(now())
  AND rm.event_date > date_sub(now(), INTERVAL 31 DAY)
  AND rm.datasource = 'vova'
  AND rm.region_code = 'all'
ORDER BY rm.event_date DESC;
"""

try:
    cursor.execute(sql)
    # 获取所有记录列表
    market_report = ""
    results = cursor.fetchall()
    for row in results:
        market_report = market_report + "<tr>"
        for i in range(len(row)):
            market_report = market_report + "<td>" + str(row[i]) + "</td>"
        market_report = market_report + "</tr>"
except:
    sys.exit(-1)

sql = """
SELECT rm.event_date,
       rm.tot_gmv,
       rm.tot_dau,
       rm.tot_activate as tot_install,
       rm.android_gmv,
       rm.android_dau,
       concat(round(rm.android_paid_device / rm.android_dau * 100, 2), '%')        AS android_cr,
       rm.android_activate as android_install,
       rm.ios_gmv,
       rm.ios_dau,
       concat(round(rm.ios_paid_device / rm.ios_dau * 100, 2), '%')                AS ios_cr,
       rm.ios_activate as ios_install,
       concat(round(rm.tot_android_1b_ret / tmp1.android_dau * 100, 2), '%')       AS tot_android_1d_cohort,
       concat(round(rm.tot_android_7b_ret / tmp7.android_dau * 100, 2), '%')       AS tot_android_7d_cohort,
       concat(round(rm.tot_android_28b_ret / tmp28.android_dau * 100, 2), '%')     AS tot_android_28d_cohort,
       concat(round(rm.new_android_1b_ret / tmp1.android_activate * 100, 2), '%')   AS new_android_1d_cohort,
       concat(round(rm.new_android_7b_ret / tmp7.android_activate * 100, 2), '%')   AS new_android_7d_cohort,
       concat(round(rm.new_android_28b_ret / tmp28.android_activate * 100, 2), '%') AS new_android_28d_cohort,
       concat(round(rm.tot_ios_1b_ret / tmp1.ios_dau * 100, 2), '%')               AS tot_ios_1d_cohort,
       concat(round(rm.tot_ios_7b_ret / tmp7.ios_dau * 100, 2), '%')               AS tot_ios_7d_cohort,
       concat(round(rm.tot_ios_28b_ret / tmp28.ios_dau * 100, 2), '%')             AS tot_ios_28d_cohort,
       concat(round(rm.new_ios_1b_ret / tmp1.ios_activate * 100, 2), '%')           AS new_ios_1d_cohort,
       concat(round(rm.new_ios_7b_ret / tmp7.ios_activate * 100, 2), '%')           AS new_ios_7d_cohort,
       concat(round(rm.new_ios_28b_ret / tmp28.ios_activate * 100, 2), '%')         AS new_ios_28d_cohort
FROM dwb_vova_market_cb rm
         LEFT JOIN (SELECT date_add(rm1.event_date, INTERVAL 1 DAY) AS interval_1,
                            rm1.android_dau,
                            rm1.ios_dau,
                            rm1.android_install,
                            rm1.ios_install,
                            rm1.android_activate,
                            rm1.ios_activate,
                            rm1.datasource,
                            rm1.region_code
                     FROM dwb_vova_market_cb rm1) AS tmp1 ON tmp1.interval_1 = rm.event_date
    AND tmp1.datasource = rm.datasource
    AND tmp1.region_code = rm.region_code
         LEFT JOIN (SELECT date_add(rm1.event_date, INTERVAL 7 DAY) AS interval_7,
                            rm1.android_dau,
                            rm1.ios_dau,
                            rm1.android_install,
                            rm1.ios_install,
                            rm1.android_activate,
                            rm1.ios_activate,
                            rm1.datasource,
                            rm1.region_code
                     FROM dwb_vova_market_cb rm1) AS tmp7 ON tmp7.interval_7 = rm.event_date
    AND tmp7.datasource = rm.datasource
    AND tmp7.region_code = rm.region_code
         LEFT JOIN (SELECT date_add(rm1.event_date, INTERVAL 28 DAY) AS interval_28,
                            rm1.android_dau,
                            rm1.ios_dau,
                            rm1.android_install,
                            rm1.ios_install,
                            rm1.android_activate,
                            rm1.ios_activate,
                            rm1.datasource,
                            rm1.region_code
                     FROM dwb_vova_market_cb rm1) AS tmp28
                    ON tmp28.interval_28 = rm.event_date
                        AND tmp28.datasource = rm.datasource
                        AND tmp28.region_code = rm.region_code
WHERE rm.event_date < date(now())
  AND rm.event_date > date_sub(now(), INTERVAL 31 DAY)
  AND rm.datasource = 'vova'
  AND rm.region_code = 'all'
ORDER BY rm.event_date DESC;
"""

try:
    # 执行SQL语句
    cursor.execute(sql)
    # 获取所有记录列表
    market_cb_report = ""
    results = cursor.fetchall()
    for row in results:
        market_cb_report = market_cb_report + "<tr>"
        for i in range(len(row)):
            market_cb_report = market_cb_report + "<td>" + str(row[i]) + "</td>"
        market_cb_report = market_cb_report + "</tr>"
except:
    sys.exit(-1)

db.close()
import datetime

datetime = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
html = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Vova APP 大盘及渠道报表 - {datetime}</title>
    <style type="text/css">

        .table-blue {
            border-collapse: collapse;
            margin-top: 20px;
            margin-bottom: 50px;
        }

        .table-blue thead th, .table-blue thead tr {
            background-color: rgb(81, 130, 187);
            color: #fff;
            border-width: 1px;
            border-color: #fff;
            border-style: solid;
            font-size: 14px;
            font-weight: bold;
            white-space: nowrap;
        }

        .table-blue tbody tr, .table-blue tbody td {
            border-collapse: collapse;
            border-width: 1px;
            border-style: solid;
            mso-cellspacing: 0;
            border-color: rgb(81, 130, 187);
            width: 80px;
            color: #000000;
        }

        .table-blue tbody td, .table-blue tbody th {
            font-size: 12px;
            font-weight: bold;
            color: #000000;
            text-align: center;
            white-space: nowrap;
        }

        .table-blue caption {
            font-size: 24px;
            color: rgb(81, 130, 187);
            font-weight: bolder;
            margin-bottom: 10px;
            margin-top: 10px;
        }

        .date {
            width: 80px;
        }

    </style>
</head>
<body>
<!--sheet 1-->
<table class="table-blue">
    <caption>Vova APP 每日报表 - [{datetime}] </caption>
    <thead>
    <tr>
        <th></th>
        <th colspan="4">Total</th>
        <th colspan="4">安卓</th>
        <th colspan="4">iOS</th>
        <th colspan="3">全体用户留存 - 安卓</th>
        <th colspan="3">新用户留存 - 安卓</th>
        <th colspan="3">全体用户留存 - iOS</th>
        <th colspan="3">新用户留存 - iOS</th>
    </tr>
    <tr>
        <th>Date</th>
        <th>GMV（含一元夺宝）</th>
        <th>GMV</th>
        <th style="padding-left: 5px;padding-right: 5px">DAU</th>
        <th style="padding-left: 5px;padding-right: 5px">Install</th>
        <th>GMV</th>
        <th style="padding-left: 5px;padding-right: 5px">DAU</th>
        <th>CR(UV)</th>
        <th style="padding-left: 5px;padding-right: 5px">Install</th>
        <th>GMV</th>
        <th style="padding-left: 5px;padding-right: 5px">DAU</th>
        <th>CR(UV)</th>
        <th style="padding-left: 5px;padding-right: 5px">Install</th>
        <th>1天前</th>
        <th>7天前</th>
        <th>28天前</th>
        <th>1天前</th>
        <th>7天前</th>
        <th>28天前</th>
        <th>1天前</th>
        <th>7天前</th>
        <th>28天前</th>
        <th>1天前</th>
        <th>7天前</th>
        <th>28天前</th>
    </tr>
    </thead>
    <tbody>
    {market_report}
    </tbody>
</table>

<!--sheet 2-->
<table class="table-blue">
    <caption>Vova APP 去cb国家 每日报表 - [{datetime}] </caption>
    <thead>
    <tr>
        <th></th>
        <th colspan="3">Total</th>
        <th colspan="4">安卓</th>
        <th colspan="4">iOS</th>
        <th colspan="3">全体用户留存 - 安卓</th>
        <th colspan="3">新用户留存 - 安卓</th>
        <th colspan="3">全体用户留存 - iOS</th>
        <th colspan="3">新用户留存 - iOS</th>
    </tr>
    <tr>
        <th>Date</th>
        <th>GMV</th>
        <th style="padding-left: 5px;padding-right: 5px">DAU</th>
        <th style="padding-left: 5px;padding-right: 5px">Install</th>
        <th>GMV</th>
        <th style="padding-left: 5px;padding-right: 5px">DAU</th>
        <th>CR(UV)</th>
        <th style="padding-left: 5px;padding-right: 5px">Install</th>
        <th>GMV</th>
        <th style="padding-left: 5px;padding-right: 5px">DAU</th>
        <th>CR(UV)</th>
        <th style="padding-left: 5px;padding-right: 5px">Install</th>
        <th>1天前</th>
        <th>7天前</th>
        <th>28天前</th>
        <th>1天前</th>
        <th>7天前</th>
        <th>28天前</th>
        <th>1天前</th>
        <th>7天前</th>
        <th>28天前</th>
        <th>1天前</th>
        <th>7天前</th>
        <th>28天前</th>
    </tr>
    </thead>
    <tbody>{market_cb_report}</tbody>
</table>
</body>
</html>
'''.replace("\n", "").replace("{datetime}", datetime).replace("{market_report}", market_report).replace(
    "{market_cb_report}", market_cb_report)

import smtplib
import email.utils
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class SendEmail:
    def send_email(self, html):
        SENDER = 'services@vova.com'  # 邮箱名
        SENDERNAME = 'vova-data'

        #RECIPIENT_2 = ['qi.zhong@gmail.com', 'ychen@i9i8.com', 'lchen@i9i8.com', 'mxu2@i9i8.com', 'fchen1@i9i8.com', 'jchu@i9i8.com', 'txu@i9i8.com', 'jyji@vova.com.hk','zysong1@vova.com.hk', 'fjzhang@i9i8.com']
        RECIPIENT_2 = ['zyzheng@i9i8.com']
        RECIPIENT_1 = ",".join(RECIPIENT_2)
        USERNAME_SMTP = "AKIA3TUGTP557OGE6WXZ"  # 带有邮件权限的 IAM 帐号
        PASSWORD_SMTP = "BMcrPXE0Ea9QtSvFCgYjArq5Q/M294T471o3CK9rPPXv"  # 带有邮件权限的 IAM 密码
        HOST = "email-smtp.us-east-1.amazonaws.com"
        PORT = 587
        SUBJECT = 'Vova APP 大盘及渠道报表'

        BODY_HTML = html

        msg = MIMEMultipart('alternative')
        msg['Subject'] = SUBJECT
        msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
        msg['To'] = RECIPIENT_1
        part = MIMEText(BODY_HTML, 'html')
        msg.attach(part)

        try:
            server = smtplib.SMTP(HOST, PORT)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(USERNAME_SMTP, PASSWORD_SMTP)
            server.sendmail(SENDER, RECIPIENT_2, msg.as_string())
            server.close()
        except Exception as e:
            print(e)
            sys.exit(-1)
        else:
            print(0)
            sys.exit(0)


if __name__ == "__main__":
    sender = SendEmail()
    sender.send_email(html)
