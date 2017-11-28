# C&A_association.py
# coding:utf-8
import csv
import MySQLdb
import os
import datetime


def getOrderData(db):
    out_data = {}
    cursor = db.cursor()

    filter_date = datetime.date.today() - datetime.timedelta(days=30)
    print filter_date

    cursor.execute('select orderID,sku from posorder_v1 where date>="%s"' % (filter_date))
    data = cursor.fetchall()

    # 创建订单横表数据
    for item in data:
        if item[0] not in out_data:
            out_data[item[0]] = [item[1][0:9]]
        else:
            out_data[item[0]] = out_data[item[0]] + [item[1][0:9]]

    # 提出单一商品订单
    out_data_list = []
    for item in out_data.items():
        if len(item[1]) >= 2:
            out_data_list.append(item[1])
    return out_data_list


def apriori(data):
    # data = [['a', 'a', 'b'], ['a', 'b', 'c'], ['b', 'c', 'd', 'w'], ['a', 'b']]
    # data_all = []
    data_dict = {}
    for item in data:
        right_list = item
        for sku in item:
            for right_sku in right_list:
                if sku != right_sku:
                    sku_item = sku + '->' + right_sku
                    # data_all.append(sku_item)
                    if sku_item not in data_dict:
                        data_dict[sku_item] = 1
                    else:
                        data_dict[sku_item] += 1
    # print data_all
    # print data_dict
    result = sorted(data_dict.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)[:100]
    # print result
    return result


def getReportWeek():
    cursor = db.cursor()
    # 查询订单最大日期
    cursor.execute('select DATE_FORMAT(max(t.date),"%Y%m%d") from posorder_v1 t')
    max_date = cursor.fetchall()[0][0]
    # print 'max_date:', type(max_date)

    # 查询订单日期所在的周数
    cursor.execute('select CALYEAR,CONCAT(CALYEAR,CALWEEK) from t_calendar where RTDATEF<="%s" and RTDATEU>="%s"'\
                   % (max_date,max_date))
    cal_date = cursor.fetchall()
    year = cal_date[0][0]
    week = cal_date[0][1]
    # print year, week
    return year, week


def resultInDB(result, year, week):
    cursor = db.cursor()
    cursor.execute('delete from result_association_offline where week = "%s"' % week)
    for item in result:
        print item[0].split('->')[0], item[0].split('->')[1], item[1]
        cursor.execute('insert into result_association_offline values("%s", "%s", "%s", "%s", "%d", "%s")' %
                       (week, year, item[0].split('->')[0], item[0].split('->')[1], item[1], datetime.datetime.now()))
    db.commit()


if __name__ == '__main__':

    # path = 'D:\project\project-CA\project-CA\source_file\order'
    path = '/Users/peter/work/project/project-CA/source_file/order'
    db = MySQLdb.connect("10.124.130.161", "dauser", "q24nxg6mS3", "dataanalyze")


    # 获取与装置数据
    data = getOrderData(db)

    # print data
    # 计算关联结果
    result = apriori(data)

    # print result
    # 获取报表年月
    year, week = getReportWeek()

    # print year,week
    # 将关联分析结算结果写入数据库
    resultInDB(result, year, week)

    db.close()
