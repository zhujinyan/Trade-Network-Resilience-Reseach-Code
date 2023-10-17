import numpy as np
import pymssql
import os
import csv
from tqdm import  *
import traceback
#import Indicator


'''
    UN数据，读取csv并入库，计算邻接矩阵，不包括world
    需要在Connect函数里填在用户名，密码，以及数据库名
    注意：邻接矩阵入库的表格现在记为adjacency_matrix，如果要修改，需要把第39行和第217行的table名改了
'''


regions = ["usa", "china", "japan", "germany", "unitedkingdom", "india", "france", "italy", "canada", "korea","russia",
            "brazil", "australia", "spain", "mexico", "indonesia", "netherlands", "saudiarabia", "turkey", "switzerland","poland",
            "sweden", "belgium", "thailand", "ireland", "argentina", "norway", "israel", "austria", "nigeria", "southafrica",
            "bangladesh", "egypt", "denmark", "singapore", "philippines", "malaysia", "hongkong", "vietnam", "unitedarab", "pakistan",
            "chile","colombia","finland", "romania", "czechia", "newzealand", "portugal", "iran", "peru"]
regions = sorted(regions)


#   connect to database
def Connect():
    conn = pymssql.connect(host='10.51.86.201', user="", password="", database="")      # TODO：添加信息
    cur = conn.cursor()
    if not cur:
        raise (NameError, "Connection failed.")
    else:
        return conn, cur

#   create table
def create_table():
    conn, cur = Connect()

    cur.execute("""
        create table hs2_adjacency_matrix (hs int not null, period int not null, reporter varchar(40) not null, %s float, 
        %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, 
        %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, 
        %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, 
        %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, %s float, 
        %s float, CONSTRAINT PK_hs2adjacencymatrix_hsperiodreporter PRIMARY KEY CLUSTERED(hs, period, reporter)) 
    """ % (regions[0],regions[1],regions[2],regions[3],regions[4],regions[5],regions[6],regions[7],regions[8],regions[9],
           regions[10],regions[11],regions[12],regions[13],regions[14],regions[15],regions[16],regions[17],regions[18],regions[19],
           regions[20],regions[21],regions[22],regions[23],regions[24],regions[25],regions[26],regions[27],regions[28],regions[29],
           regions[30],regions[31],regions[32],regions[33],regions[34],regions[35],regions[36],regions[37],regions[38],regions[39],
           regions[40],regions[41],regions[42],regions[43],regions[44],regions[45],regions[46],regions[47],regions[48],regions[49]))
    conn.commit()

    cur.close()
    conn.close()

# change int to bigint
def to_bigint():
    conn, cur = Connect()

    for region in regions:
        cur.execute("""
            ALTER TABLE adjacency_matrix ALTER COLUMN %s bigint
        """ % region)
        conn.commit()

    cur.close()
    conn.close()

# 读csv
def fun(hs_, digit):
    conn, cur = Connect()

    length = len(str(hs_))
    if length < digit:
        hs = '0' * (digit - length) + str(hs_)
    else:
        hs = str(hs_)

    A = np.array([np.zeros((50, 50), dtype=np.float64)] * 144)
    for file in tqdm(os.listdir(os.path.join(path_dir, hs))):
        with open(os.path.join(path_dir, hs, file), 'r') as f:
            reader = csv.reader(f)


            for r in reader:
                # 跳过第一行
                if r[0] == 'Classification':
                    continue

                # if csv is empty, continue
                if r[0] == "No data matches your query or your query is too complex. Request JSON or XML format for more information.":
                    continue

                if r[12] == 'Venezuela' or r[12] == 'fr. south antarctic terr.':
                    continue

                if int(r[1]) < 2010:
                    continue

                nw = 0 if r[29] == '' else int(r[29])
                gw = 0 if r[30] == '' else int(r[30])
                tv = 0 if r[31] == '' else int(r[31])
                cif = 0 if r[32] == '' else int(r[32])
                fob = 0 if r[33] == '' else int(float(r[33]))

                region = r[9].lower()
                region = 'usa' if r[9] == 'United States of America' else region
                region = 'unitedkingdom' if r[9] == 'United Kingdom' else region
                region = 'korea' if r[9] == 'Rep. of Korea' else region
                region = 'russia' if r[9] == 'Russian Federation' else region
                region = 'saudiarabia' if r[9] == 'Saudi Arabia' else region
                region = 'southafrica' if r[9] == 'South Africa' else region
                region = 'hongkong' if r[9] == 'China, Hong Kong SAR' else region
                region = 'vietnam' if r[9] == 'Viet Nam' else region
                region = 'unitedarab' if r[9] == 'United Arab Emirates' else region
                region = 'czechia' if r[9] == 'Czech Rep.' else region
                region = 'newzealand' if r[9] == 'New Zealand' else region


                '''try:
                    if r[6] != '1' and r[6] != '2':
                            cur.execute("""
                                insert into reexport_reimport (hs, month, reporter, partner, trade_flow_code, trade_flow, netweight, grossweight, tradevalue, cifvalue, fobvalue)
                                values (%s, %s, \'%s\', \'%s\', %s, %s, %d, %d, %d, %d, %d)
                            """ % (str(hs), r[2], r[9], r[12], r[6], r[7], nw, gw, tv, cif, fob))
                            conn.commit()
                            continue
                    else:
                            cur.execute("""
                                insert into %s (hs, partner, trade_flow_code, trade_flow, netweight, grossweight, tradevalue, cifvalue, fobvalue)
                                values (%s, \'%s\', %s, \'%s\', %d, %d, %d, %d, %d)
                            """ % (region + '_' + r[2], r[21], r[12], r[6], r[7], nw, gw, tv, cif, fob))
                            conn.commit()
                except Exception:
                    pass'''

                '''以上是入库，下面是邻接矩阵'''

                if int(r[2]) > 202112:
                    continue

                partner = r[12].lower()
                partner = 'usa' if r[12] == 'United States of America' else partner
                partner = 'unitedkingdom' if r[12] == 'United Kingdom' else partner
                partner = 'korea' if r[12] == 'Rep. of Korea' else partner
                partner = 'russia' if r[12] == 'Russian Federation' else partner
                partner = 'saudiarabia' if r[12] == 'Saudi Arabia' else partner
                partner = 'southafrica' if r[12] == 'South Africa' else partner
                partner = 'hongkong' if r[12] == 'China, Hong Kong SAR' else partner
                partner = 'vietnam' if r[12] == 'Viet Nam' else partner
                partner = 'unitedarab' if r[12] == 'United Arab Emirates' else partner
                partner = 'czechia' if r[12] == 'Czech Rep.' else partner
                partner = 'newzealand' if r[12] == 'New Zealand' else partner

                if partner == region or partner == 'fr. south antarctic terr.':
                    continue

                i = (int(int(r[2]) / 100) - 2010) * 12 + (int(r[2]) % 100) - 1
                if r[6] == '1' and partner != 'world':
                    if A[i][regions.index(partner)][regions.index(region)] == 0:               # 如果这个方向还未计数，则直接写入
                        A[i][regions.index(partner)][regions.index(region)] = tv
                    else:
                        # 已经有计数，说明对方国家也有相关记录，取均值
                        A[i][regions.index(partner)][regions.index(region)] = (A[i][regions.index(partner)][regions.index(region)] + tv) / 2.0
                    #A[i][50][regions.index(region)] -= tv
                    '''if A[i][50][regions.index(region)] < 0:
                        print(regions.index(region))
                        print(A[i][50][regions.index(region)]+tv)
                        pass'''
                #elif r[6] == '1' and partner == 'world':
                    #A[i][50][regions.index(region)] += tv
                elif r[6] == '2' and partner != 'world':
                    if A[i][regions.index(region)][regions.index(partner)] == 0:
                        A[i][regions.index(region)][regions.index(partner)] = tv
                    else:
                        A[i][regions.index(region)][regions.index(partner)] = (A[i][regions.index(region)][regions.index(partner)] + tv) / 2.0
                    #A[i][regions.index(region)][50] -= tv
                    '''if A[i][regions.index(region)][50] < 0:
                        print(regions.index(region))
                        pass'''
                # elif r[6] == '2' and partner == 'world':
                #     A[i][regions.index(region)][50] += tv

    '''for i, A_ in enumerate(A):
        for m in range(50):
            sum_x = 0
            sum_y = 0
            for n in range(50):
                sum_x += A_[m][n]
                sum_y += A_[n][m]
            A_[m][50] -= sum_x
            A_[50][m] -= sum_y

        A_[50][50] = 0'''

    # for i, A_ in enumerate(A):
    #     for m in range(50):
    #         if abs(A_[m][50]) < 10:
    #             A_[m][50] = 0
    #         if abs(A_[50][m]) < 10:
    #             A_[50][m] = 0
    #     A_[50][50] = 0


    for i, A_ in enumerate(A):
        #print(A_)
        year = int(i/12) + 2010
        month = int(i % 12) + 1
        if month < 10:
            period = str(year) + '0' + str(month)
        else:
            period = str(year) + str(month)

        # print('\nenter indicator' + hs + period)
        # Indicator.indicator(hs, period, regions, A_[:50, :50])
        # print('out indicator')

        for m in range(51):
            try:
                cur.execute("""
                        insert into hs2_adjacency_matrix (hs, period, reporter, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) values (%s, %s, \'%s\', %f, %f, %f, %f, 
                        %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, 
                        %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f)
                    """ % (
                regions[0], regions[1], regions[2], regions[3], regions[4], regions[5], regions[6], regions[7], regions[8],
                regions[9], regions[10], regions[11], regions[12], regions[13], regions[14], regions[15], regions[16],
                regions[17], regions[18], regions[19], regions[20], regions[21], regions[22], regions[23], regions[24],
                regions[25], regions[26], regions[27], regions[28], regions[29], regions[30], regions[31], regions[32],
                regions[33], regions[34], regions[35], regions[36], regions[37], regions[38], regions[39], regions[40],
                regions[41], regions[42], regions[43], regions[44], regions[45], regions[46], regions[47], regions[48],
                regions[49], str(hs), str(period), regions[m], A_[m][0], A_[m][1], A_[m][2], A_[m][3], A_[m][4], A_[m][5],
                A_[m][6], A_[m][7], A_[m][8], A_[m][9], A_[m][10], A_[m][11], A_[m][12], A_[m][13], A_[m][14], A_[m][15],
                A_[m][16], A_[m][17], A_[m][18], A_[m][19], A_[m][20], A_[m][21], A_[m][22], A_[m][23], A_[m][24], A_[m][25],
                A_[m][26], A_[m][27], A_[m][28], A_[m][29], A_[m][30], A_[m][31], A_[m][32], A_[m][33], A_[m][34], A_[m][35],
                A_[m][36], A_[m][37], A_[m][38], A_[m][39], A_[m][40], A_[m][41], A_[m][42], A_[m][43], A_[m][44], A_[m][45],
                A_[m][46], A_[m][47], A_[m][48], A_[m][49]))
                conn.commit()
            except Exception:
                print(traceback.format_exc())

    cur.close()
    conn.close()


if __name__ == '__main__':
    # to_bigint()

    path_dir = r""       # TODO：须填写，例 C:\data

    try:
        create_table()
    except Exception:
        pass

    lhs6 = ['050100', '050210', '050290', '050590', '050690', '050710', '051199', '152200', '170310', '170390', '251720',
           '251730', '253090', '261800', '261900', '262011', '262019', '262021', '262029', '262030', '262040', '262060',
           '262091', '262099', '262110', '262190', '271091', '271099', '271390', '280461', '252530', '300692', '380400',
           '382510', '382520', '382530', '382541', '382549', '382550', '382561', '382569', '382590', '400400', '401700',
           '411520', '470790', '630900', '631010', '631090', '700100', '711230', '711291', '711299', '740100', '780200',
           '810297', '810530', '810730', '811020', '811100', '811213', '811222', '811252', '811292', '854810',  '391510',
            '391520', '391530', '391590', '470710', '470720', '470730', '510310', '510320',
           '510330', '510400', '520210', '520291', '520299', '550510', '550520', '720421', '810197', '810420', '810600',
           '810830', '810930', '811300', '720449', '740400', '760200', '890800', '440131', '440139', '450190', '711292',
           '720410', '720429', '720430', '720441', '720450', '750300', '790200', '800200', '810330']

    lhs4 = [ '8469',
           '8470', '8471', '8472', '8473', '8415', '8418', '8450', '8508', '8509', '8510', '8516', '8517', '8518',
           '8519', '8520', '8521', '8522', '8523', '8524', '8525', '8526', '8527', '8528', '8529', '8530', '8531',
           '9504', '8539', '8532', '8533', '8534', '9018', '9019', '9020', '9021', '9022', '84', '85', '90', '2520',
           '2524', '6806']

    lhs2 = [ '84', '85', '90']


    '''for hs in tqdm(lhs6):
        print(hs)
        fun(hs, 6)      # 第二个参数填入hs的位数，比如4位hs，就填4
        print(hs,'down')

    for hs in tqdm(lhs4):
        print(hs)
        fun(hs, 4)
        print(hs, 'down')

    for hs in tqdm(lhs2): 
        print(hs)
        fun(hs, 2)
        print(hs, 'down')'''

    for hs in tqdm(range(1, 99)):
        if hs == 77 or hs == 98:
            continue
        print(hs)
        fun(hs, 2)
        print(hs, 'done')