import os
import pymssql
import pandas as pd
import Indicator_new
import numpy as np
from concurrent.futures import ProcessPoolExecutor

'''
    UN数据，计算指标，多进程，和Indicator-new一起用
    需要在Connect函数里填在用户名，密码，以及数据库名
'''


regions = ["usa", "china", "japan", "germany", "unitedkingdom", "india", "france", "italy", "canada", "korea","russia",
            "brazil", "australia", "spain", "mexico", "indonesia", "netherlands", "saudiarabia", "turkey", "switzerland","poland",
            "sweden", "belgium", "thailand", "ireland", "argentina", "norway", "israel", "austria", "nigeria", "southafrica",
            "bangladesh", "egypt", "denmark", "singapore", "philippines", "malaysia", "hongkong", "vietnam", "unitedarab", "pakistan",
            "chile","colombia","finland", "romania", "czechia", "newzealand", "portugal", "iran", "peru"]
regions = sorted(regions)
regions.append('world')


def Connect():
    conn = pymssql.connect(host='10.51.86.201', user="", password="", database="")      # TODO: 填写信息
    cur = conn.cursor()
    if not cur:
        raise (NameError, "Connection failed.")
    else:
        return conn, cur

def create_table():
    conn, cur = Connect()

    cur.execute("""
        create table UN_multiple_indicator(ID int IDENTITY(1, 1),
                                hs int not null,
                                time int not null,
                                region varchar(20) not null,
                                binomial_cutdown int not null,
                                degree int,
                                in_degree int,
                                out_degree int,
                                clustering float,
                                clustering_weight float,
                                core_number float,
                                degree_centrality float,
                                in_degree_centrality float,
                                out_degree_centrality float,
                                eigenvector_centrality float,
                                eigenvector_centrality_wight float,
                                second_order_centrality float,
                                betweenness_centrality float,
                                betweenness_centrality_distance float,
                                closeness_centrality float,
                                closeness_centrality_distance float,
                                degree_weight_Di float,
                                degree_weight float,
                                pagerank float,
                                in_largest_cc int,
                                d_unit float,
                                d_diff_in float,
                                d_diff_out float,
                                d_diff_both float,
                                shs_constraint float,
                                shs_constraint_weight float,
                                shs_effective_size float,
                                shs_effective_size_weight float,
                                hits_0 float,
                                hits_1 float,
                                d_an_o_o_None float,
                                d_an_o_i_None float,
                                d_an_o_io_None float,
                                d_an_i_o_None float,
                                d_an_i_i_None float,
                                d_an_i_io_None float,
                                d_an_io_o_None float,
                                d_an_io_i_None float,
                                d_an_io_io_None float,
                                d_an_o_o_weight float,
                                d_an_o_i_weight float,
                                d_an_o_io_weight float,
                                d_an_i_o_weight float,
                                d_an_i_i_weight float,
                                d_an_i_io_weight float,
                                d_an_io_o_weight float,
                                d_an_io_i_weight float,
                                d_an_io_io_weight float,
                                closeness_vitality float,
                                closeness_vitality_weight float,
                                community_louvain_communities float,
                                community_greedy_modularity_communities float,
                                community_girvan_newman float,
                                c_flu_2 float,
                                c_flu_3 float,
                                c_flu_4 float,
                                c_flu_5 float,
                                c_flu_6 float,
                                c_flu_7 float,
                                c_flu_8 float,
                                c_flu_9 float,
                                community_kernighan_lin_bisection float,
                                community_asyn_lpa_communities float,
                                community_label_propagation_communities float,
                                primary key (hs, time, region, binomial_cutdown))
    """)
    conn.commit()

    cur.execute("""
        create table UN_single_indicator (ID int IDENTITY(1, 1),
                                hs int not null,
                                time int not null,
                                binomial_cutdown int not null,
                                nodes int,
                                edges int,
                                avg_degree float,
                                avg_in_degree float,
                                avg_out_degree float,
                                local_efficiency float,
                                global_efficiency float,
                                density float,
                                diameter float,
                                average_shortest_path_length float,
                                average_shortest_path_length_distance float,
                                average_clustering float,
                                average_clustering_weight float,
                                transitivity float,
                                wiener_index float,
                                dac_out_out_None float,
                                dac_out_in_None float,
                                dac_in_out_None float,
                                dac_in_in_None float,
                                dac_out_out_weight float,
                                dac_out_in_weight float,
                                dac_in_out_weight float,
                                dac_in_in_weight float,
                                length_largest_cc int,
                                primary key (hs, time, binomial_cutdown))
    """)
    conn.commit()

    conn.close()
    cur.close()

def cal(hs_high):
    '''if hs_high == 0 or hs_high == 2 or hs_high == 3 or hs_high == 7:
        return'''

    conn, cur = Connect()

    try:
        start = hs_high * 10
        end = start + 10
        print(f"process hs {start}-{end-1} in pid {os.getpid()}")
        for hs in range(start, end):
            if hs == 77 or hs == 98 or hs == 0:
                continue

            cur.execute("""
                select * from hs2_adjacency_matrix where hs = %d
            """ % hs)

            A = np.array([np.zeros((50, 50), dtype=np.float64)] * 144)

            rows = cur.fetchall()
            for row in rows:
                if row[2] == 'world':
                    continue

                mi = (int((row[1] - 201000) / 100) * 12) + (row[1] % 100 - 1)
                ri = regions.index(row[2])

                for i in range(3, 53):
                    if row[i] < 0:
                        print(str(row[1]) + ' ' + str(row[2]) + ' ' + str(i) + ' ' + str(row[i]) )
                    A[mi][ri][i-3] = row[i]

            for i, A_ in enumerate(A):
                df = pd.DataFrame(A_)

                year = int(i / 12) + 2010
                month = int(i % 12) + 1
                if month < 10:
                    time = str(year) + '0' + str(month)
                else:
                    time = str(year) + str(month)

                cur.execute("""
                    select * from UN_single_indicator where hs = %d and time = %s
                """ % (hs, time))
                row = cur.fetchall()
                if len(row) != 0:
                    continue

                print('start ' + str(hs) + ' ' + time)
                dic = Indicator_new.main(df)
                for d in dic:

                    try:
                        cur.execute("""
                            insert into UN_single_indicator (hs, time, binomial_cutdown, nodes, edges, avg_degree, avg_in_degree, avg_out_degree, local_efficiency,
                            global_efficiency, density, diameter, average_shortest_path_length, average_shortest_path_length_distance, 
                            average_clustering, average_clustering_weight, transitivity, wiener_index, 
                            dac_out_out_None, dac_out_in_None, dac_in_out_None, dac_in_in_None, dac_out_out_weight, dac_out_in_weight, 
                            dac_in_out_weight, dac_in_in_weight, length_largest_cc)
                            values(%d, %s, %d, %d, %d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %d)
                        """ % (hs, time, d, dic[d][0], dic[d][1], dic[d][2], dic[d][3], dic[d][4], dic[d][5], dic[d][6], dic[d][7], dic[d][8],
                               dic[d][9], dic[d][10], dic[d][11], dic[d][12], dic[d][13], dic[d][14], dic[d][15]['dacoutoutNone'],
                               dic[d][15]['dacoutinNone'], dic[d][15]['dacinoutNone'], dic[d][15]['dacininNone'], dic[d][15]['dacoutoutweight'],
                               dic[d][15]['dacoutinweight'], dic[d][15]['dacinoutweight'], dic[d][15]['dacininweight'], len(dic[d][35])))
                        conn.commit()
                    except Exception:
                        pass

                    for i in range(50):
                        if i in dic[d][35]:
                            in_lcc = 1
                        else:
                            in_lcc = 0

                        try:
                            cur.execute("""
                                     insert into UN_multiple_indicator(hs, time, region, binomial_cutdown, degree, in_degree, out_degree, 
                                     clustering, clustering_weight, core_number, degree_centrality, in_degree_centrality, out_degree_centrality, 
                                     eigenvector_centrality, eigenvector_centrality_wight, second_order_centrality, betweenness_centrality, 
                                     betweenness_centrality_distance, closeness_centrality, closeness_centrality_distance, degree_weight_Di, degree_weight, pagerank, 
                                     in_largest_cc, d_unit, d_diff_in, d_diff_out, d_diff_both, shs_constraint, shs_constraint_weight, shs_effective_size, shs_effective_size_weight, 
                                     hits_0, hits_1, d_an_o_o_None, d_an_o_i_None, d_an_o_io_None, d_an_i_o_None, d_an_i_i_None, d_an_i_io_None, d_an_io_o_None, 
                                     d_an_io_i_None, d_an_io_io_None, d_an_o_o_weight, d_an_o_i_weight, d_an_o_io_weight, d_an_i_o_weight, d_an_i_i_weight, 
                                     d_an_i_io_weight, d_an_io_o_weight, d_an_io_i_weight, d_an_io_io_weight, closeness_vitality, 
                                     closeness_vitality_weight, community_louvain_communities, community_greedy_modularity_communities, 
                                     community_girvan_newman, c_flu_2, c_flu_3, c_flu_4, c_flu_5, c_flu_6, c_flu_7, c_flu_8, c_flu_9, 
                                     community_kernighan_lin_bisection, community_asyn_lpa_communities, community_label_propagation_communities)
                                     values(%d, %s, \'%s\', %d, %d, %d, %d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, 
                                     %f, \'%s\', %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, 
                                     %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f)
                                """ % (hs, time, regions[i], d, dic[d][16][i], dic[d][17][i], dic[d][18][i],
                                       dic[d][19][i], dic[d][20][i], dic[d][21][i], dic[d][22][i], dic[d][23][i], dic[d][24][i], dic[d][25][i],
                                       dic[d][26][i], dic[d][27][i], dic[d][28][i], dic[d][29][i], dic[d][30][i], dic[d][31][i], dic[d][32][i],
                                       dic[d][33][i], dic[d][34][i], in_lcc, dic[d][36][i][1], dic[d][37][i][1], dic[d][37][i][2], dic[d][37][i][3], dic[d][38][i], dic[d][39][i],
                                       dic[d][40][i], dic[d][41][i], dic[d][42][0][i], dic[d][42][1][i], dic[d][43]['o_o_None'][i], dic[d][43]['o_i_None'][i],
                                       dic[d][43]['o_io_None'][i], dic[d][43]['i_o_None'][i], dic[d][43]['i_i_None'][i], dic[d][43]['i_io_None'][i],
                                       dic[d][43]['io_o_None'][i], dic[d][43]['io_i_None'][i], dic[d][43]['io_io_None'][i], dic[d][43]['o_o_weight'][i],
                                       dic[d][43]['o_i_weight'][i], dic[d][43]['o_io_weight'][i], dic[d][43]['i_o_weight'][i], dic[d][43]['i_i_weight'][i],
                                       dic[d][43]['i_io_weight'][i], dic[d][43]['io_o_weight'][i], dic[d][43]['io_i_weight'][i], dic[d][43]['io_io_weight'][i],
                                       dic[d][44][i], dic[d][45][i], dic[d][46][i], dic[d][47][i], dic[d][51][i], dic[d][48]['c_flu_2'][i],
                                       dic[d][48]['c_flu_3'][i], dic[d][48]['c_flu_4'][i], dic[d][48]['c_flu_5'][i], dic[d][48]['c_flu_6'][i], dic[d][48]['c_flu_7'][i],
                                       dic[d][48]['c_flu_8'][i], dic[d][48]['c_flu_9'][i], dic[d][49][i], dic[d][50][i], dic[d][52][i]))
                            conn.commit()
                        except Exception:
                            pass
    except Exception:
        print(f"error {hs}  {time}")

    conn.close()
    cur.close()
    print(f"hs {start}-{end-1} finished")

def process_data():
    with ProcessPoolExecutor(max_workers=8) as executor:
        executor.map(cal, range(10))


if __name__ == '__main__':
    # create_table()
    # cal()
    process_data()