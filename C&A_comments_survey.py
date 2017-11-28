# C&A_comments.py
# coding:utf-8
import csv
import MySQLdb
import os
import datetime
import json
import ECC_CatCon as CatCon
import pymssql


class CCAgent:
    def __init__(self):
        self.cc_client = CatCon.CatConClient()
        self.cc_client.addServer(cc_host, cc_port)
        self.cc_client.usePersistentConnection(True)

    def getBeforeSplitPoint(self, text, point):
        if isinstance(point, int) == False or point < 0 or point >= len(text):
            return 0
        for index in range(point, 0, -1):
            # , | ! | ; | ? | ， | 。 | ！ | ？ | ； |
            if text[index] == ',' or text[index] == ';' or text[index] == '!' or text[index] == '?' or text[index] == '\\':
                return index + 1
            if (text[index:index + 3] == '\xe3\x80\x82' or text[index:index + 3] == '\xef\xbc\x81' or
                text[index:index + 3] == '\xef\xbc\x8c' or text[index:index + 3] == '\xef\xbc\x9b' or
                    text[index:index + 3] == '\xef\xbc\x9f'):
                return index + 3

    def getAfterSplitPoint(self, text, point):
        if isinstance(point, int) == False or point < 0 or point >= len(text):
            return 0
        for index in range(point, len(text)):
            if (text[index] == ',' or text[index] == ';' or text[index] == '!' or text[index] == '?' or text[index] == '\\'
                or
                text[index:index + 3] == '\xe3\x80\x82' or text[index:index + 3] == '\xef\xbc\x81' or
                text[index:index + 3] == '\xef\xbc\x8c' or text[index:index + 3] == '\xef\xbc\x9b' or
                    text[index:index + 3] == '\xef\xbc\x9f'):
                return index

    def get_cc_result(self, message, projects):
        cc_result = self.cc_client.categorize(
            message,
            article_type,
            "",
            [projects],
            category_relevancy_type,
            0,
            use_relevancy_cutoff,
            cat_default_relevancy_cutoff,
            skip_qualifier_matches)
        return cc_result

    def getCCResult(self, message, project, type_line):
        cc_result = self.get_cc_result(message, project)
        cate_result = []
        if type_line == 'all':
            for cat in cc_result[project]:
                cate_item = []
                matchContents = ''
                Comments = cat.getCommentsMetadata()
                cate_item.append(Comments)
                matches = cat.getMatches()
                for match in matches:
                    start = match.getStart()
                    end = match.getEnd()
                    start1 = self.getBeforeSplitPoint(message, start)
                    end1 = self.getAfterSplitPoint(message, end)
                    matchContent = message[start1:end1]
                    matchContents += '%s#' % matchContent
                # cate_item.append(matchContents)
                cate_result.append(cate_item)
            return cate_result


def readFileList(path):
    """readFileList

    [description]
    读取文件目录下的csv文件订单
    Arguments:
        path {[type]} -- [description]读取的文件输入路径

    Returns:
        [type] -- [description]
    """
    cursor = db.cursor()
    cursor.execute('select file_name from ecom_order_filelist')
    data = cursor.fetchall()
    exist_list = []
    new_list = []
    for item in data:
        exist_list.append(item[0])

    for item in os.listdir(path):
        if item not in exist_list:
            new_list.append(item)
    print 'new_list:', new_list
    return new_list


def readCsvfile(path, file_name):
    """[readCsvfile]

    [description]

    Arguments:
        path {[type]} -- [description]
        file_name {[type]} -- [description]

    Returns:
        [list] -- [description]csv文件中的数据list
    """
    print 'start readCsvfile!'
    infile = os.path.join(path, file_name)
    print infile
    reader = csv.reader(open(infile, 'rU'))
    rowNum = 0
    data = []
    for line in reader:
        rowNum += 1
        if rowNum % 100 == 0:
            print rowNum
        try:
            data.append(line)
        except:
            print 'block line'
        # if rowNum > 20:
        #     break
    # print len(data)
    return data


def uploadDB(db, file_name, data_all):
    print 'start uploadDB!'
    cursor = db.cursor()
    cursor.execute('delete from comments_content where file_name = "%s"' % file_name)
    rowNum = 0
    for line in data_all:
        rowNum += 1
        # print line
        if rowNum > 1 and len(line) == 9:
            if rowNum % 100 == 0:
                print rowNum
            cursor.execute('insert into comments_content values("%s","%s", "%s", "%s", "%s","%s", "%s", "%s", "%s", "%s", "%s", "%s")' %
                           (file_name + str(rowNum), line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], file_name, datetime.datetime.now()))

    cursor.execute('delete from comments_filelist where file_name = "%s"' % file_name)
    cursor.execute('insert into comments_filelist values("%s", "%s")' % (file_name, datetime.datetime.now()))
    db.commit()


def comments_analysis(db, file_name, data_all):
    print 'start comments_analysis!'
    cursor = db.cursor()
    cursor.execute('delete from comments_analysis where file_name = "%s"' % file_name)
    rowNum = 0
    for line in data_all:
        rowNum += 1
        if rowNum > 1 and len(line) == 9:
            if rowNum % 100 == 0:
                print rowNum
            # print json.dumps(line[8], ensure_ascii=False)
            cc_result = cc_agent.getCCResult(line[8], 'ca_comment_ana', 'all')
            for cc_item in cc_result:
                comments_type = cc_item[0].split('/')[2]
                comments_class = cc_item[0].split('/')[3]
                # emotion = comments_class.split('_')[1]
                cursor.execute('insert into comments_analysis values("%s", "%s", "%s", "%s", "%s", "%s") ' %
                               (file_name + str(rowNum), line[8], comments_type, comments_class, file_name, datetime.datetime.now()))
    db.commit()


if __name__ == '__main__':

    cc_host = '121.199.25.132'
    cc_port = 6500
    article_type = 'text'
    category_relevancy_type = 1
    use_relevancy_cutoff = False
    cat_default_relevancy_cutoff = 0.0
    skip_qualifier_matches = False

    path = 'D:\project\project-CA\project-CA\source_file\comments'
    # path = '/Users/peter/work/project/project-CA/source_file/comments'
    db = MySQLdb.connect("10.124.130.161", "dauser", "q24nxg6mS3", "dataanalyze", charset='utf8')
    cc_agent = CCAgent()

    file_list = readFileList(path)
    for file_name in file_list:
        print file_name
        data_all = readCsvfile(path, file_name)
        uploadDB(db, file_name, data_all)
        comments_analysis(db, file_name, data_all)

    db.close()
