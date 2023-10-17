import numpy as np
import networkx as nx
import pandas as pd
from networkx.algorithms import community


def Grape(df, binomial_cutdown=10):
    df = np.array(df)
    n = df.shape[0]

    # 有权图
    DG = nx.from_numpy_array(df, create_using=nx.DiGraph)
    for node in DG.nodes:
        for nbr in DG.neighbors(node):
            weight = DG[node][nbr]["weight"]
            DG[node][nbr]["distance"] = 1 / weight

    # 依据binomial_cutdown截止的0-1（binomial）无权图
    BG = nx.DiGraph()
    BG.add_nodes_from(range(n))
    limit = np.mean(df) / binomial_cutdown
    for i in range(n):
        for j in range(n):
            if df[i][j] > limit:
                BG.add_edge(i, j)
    for node in BG.nodes:
        for nbr in BG.neighbors(node):
            BG[node][nbr]["distance"] = 1

    return BG, DG


def single_indicator(G):
    # 无向图
    g = nx.from_numpy_array(nx.to_numpy_array(G), create_using=nx.Graph)

    # ～～～～1个数值的指标
    # 节点数
    n = len(G.nodes)

    # 关系数
    m = len(G.edges)

    # 平均度
    d = G.degree
    d_avg = sum(dict(d).values()) / n

    # 平均入度 出度
    d_in = G.in_degree
    d_out = G.out_degree
    d_avg_in = sum(dict(d_in).values()) / n
    d_avg_out = sum(dict(d_out).values()) / n

    # 平均局部效率（局部效率：某节点的邻居引起的子图的平均全局效率）
    le = nx.local_efficiency(g)

    # 全局效率
    ge = nx.global_efficiency(g)

    # 密度: m/[n（n-1）]
    density = nx.density(G)

    # 直径diameter(max距离）
    try:
        diameter = nx.diameter(g)
    except Exception:
        diameter = 0

    # 平均路径长度(距离）
    try:
        aspl = nx.average_shortest_path_length(g)
    except Exception:
        aspl = n

    # 加距离平均路径长度(距离）(weight代表距离！！）
    try:
        aspl_d = nx.average_shortest_path_length(g, weight='distance')
    except Exception:
        aspl_d = n

    # 网络同配性(度度相关性）
    dac = dict()
    for w in (None, 'weight'):
        for x in ['out', 'in']:
            for y in ['out', 'in']:
                dac['dac' + str(x) + str(y) + str(w)] = \
                    nx.degree_assortativity_coefficient(G, x=x, y=y, weight=w)


    # 聚类系数：某一个点的邻居节点的相连程度
    # 平均集聚系数
    ac = nx.average_clustering(g)
    # 加权平均集聚系数
    # (weight代表链接强度！！）
    ac_w = nx.average_clustering(g, weight='weight')
    # 全局集聚系数
    trans = nx.transitivity(g)

    # 维纳索引
    wiener = nx.wiener_index(G)
    wiener = 1E8 if wiener == float('inf') else wiener
    # 小世界系数(太慢）
    # sigma omega

    return n, m, d_avg, d_avg_in, d_avg_out, le, ge, density, \
           diameter, aspl, aspl_d, ac, ac_w, trans, wiener, \
           dac


def multiple_indicator(G):
    # 无向图
    g = nx.from_numpy_array(nx.to_numpy_array(G), create_using=nx.Graph)
    # 节点数
    n = len(G.nodes)

    # ～～～～长度51（节点个数)的有序数据
    # 度
    d = G.degree
    # 入度 出度
    d_in = G.in_degree
    d_out = G.out_degree
    # 邻域平均度
    parameter_ls = ['out', 'in', 'in+out']
    name_ls = ['o_', 'i_', 'io_']
    d_an = dict()
    for w in (None, 'weight'):
        for i in range(3):
            for j in range(3):
                source = parameter_ls[i]
                target = parameter_ls[j]
                d_an[name_ls[i] + name_ls[j] + str(w)] = \
                    nx.average_neighbor_degree(G, source=source, target=target, weight=w)

    # 集聚系数（点的邻接点之间相互连接的程度）
    cluster = nx.clustering(g)
    # 加权集聚系数（点的邻接点之间相互连接的程度）
    # (weight代表链接强度！！）
    cluster_w = nx.clustering(g, weight='weight')

    # 核度(剥洋葱）
    ks = nx.core_number(g)

    # 度中心性 Degree Centrality（邻居多少）
    dc = nx.degree_centrality(G)
    dc_in = nx.in_degree_centrality(G)
    dc_out = nx.out_degree_centrality(G)

    # 特征向量中心性（邻居多少，及邻居的重要性）
    ec = nx.eigenvector_centrality(g)
    # 加权特征向量中心性（邻居多少，及邻居的重要性）
    # (weight代表链接强度！！）
    try:
        ec_w = nx.eigenvector_centrality(g, weight='weight', max_iter=6000)
    except Exception:
        ec_w = -1

    # 二阶中心性（二阶中心度值越低，则表明中心度越高。）
    try:
        soc = nx.second_order_centrality(g)
    except Exception:
        soc = [0] * n

    # 中介中心性 Betweeness centrality（多少最短路经过了它）
    bc = nx.betweenness_centrality(G)
    # 加距离中介中心性 Betweeness centrality（多少最短路经过了它）
    # (weight代表距离！！）
    bc_d = nx.betweenness_centrality(G, weight='distance')

    # 紧密中心性 Closeness centrality （更几何）
    cc = nx.closeness_centrality(G)
    # 加距离紧密中心性 Closeness centrality （更几何）
    # (distance代表距离！！）
    cc_d = nx.closeness_centrality(G, distance='distance')

    # 点权(有向与无向）（加权度，连边权重和）
    d_weigh_Di = nx.degree(G, weight='weight')
    d_weigh = nx.degree(g, weight='weight')

    # pagerank（需要scipy的版本）
    try:
        pr = nx.pagerank(G)
    except:
        pr = [n] * n

    # hits（需要scipy的版本）
    try:
        hits = nx.hits(G)
    except:
        hits = {0: [n] * n, 1: [n] * n}


    # 单位权： A点单位权 = A点权/A点度
    d_unit = np.zeros((n, 2))
    for i in range(n):
        d_unit[i][0] = i
        if d[i] == 0:
            d_unit[i][1] = None
        else:
            d_unit[i][1] = d_weigh[i] / d[i]

    # 权重分布差异性 就是与某个点相连的各个边的权重的标准差
    # 有in out both 三个指标
    d_diff = np.zeros((n, 4))
    df = nx.to_numpy_array(G)
    for i in range(n):
        df_out = df[i, ...]
        df_in = df[..., i]
        df_both = np.vstack((df_in, df_out))

        d_diff[i][0] = i
        d_diff[i][1] = np.std(df_in)
        d_diff[i][2] = np.std(df_out)
        d_diff[i][3] = np.std(df_both)

    # 鲁棒性 connected_components
    largest_cc = max(nx.connected_components(g), key=len)

    # 结构洞
    #print('结构洞,比较慢')
    # 约束条件
    #print('结构洞约束条件')
    constraint = nx.constraint(G)
    for cons in constraint:
        if constraint[cons] != constraint[cons]:
            constraint[cons] = -100
        elif constraint[cons] == float("inf"):
            constraint[cons] = 1E8
    #print('结构洞加权约束条件')
    constraint_w = nx.constraint(G, weight='weight')
    for cons in constraint_w:
        if constraint_w[cons] != constraint_w[cons]:
            constraint_w[cons] = -100
        elif constraint_w[cons] == float('inf'):
            constraint_w[cons] = 1E8
    # 有效尺寸
    #print('结构洞有效尺寸')
    effective_size = nx.effective_size(G)
    for esize in effective_size:
        if effective_size[esize] != effective_size[esize]:
            effective_size[esize] = -100
        elif effective_size[esize] == float('inf'):
            effective_size[esize] = 1E8
    #print('结构洞加权有效尺寸')
    effective_size_w = nx.effective_size(G, weight='weight')
    for esize in effective_size_w:
        if effective_size_w[esize] != effective_size_w[esize]:
            effective_size_w[esize] = -100
        elif effective_size_w[esize] == float('inf'):
            effective_size_w[esize] = 1E8

    # 和某点之间的距离
    # nx.voronoi_cells(G,node)
    cv = nx.closeness_vitality(G)
    for c in cv:
        if cv[c] != cv[c]:
            cv[c] = -100
        elif cv[c] == float('inf'):
            cv[c] = 1E8
    cv_w = nx.closeness_vitality(G,weight='weight')
    for c in cv_w:
        if cv_w[c] != cv_w[c]:
            cv_w[c] = -100
        elif cv_w[c] == float('inf'):
            cv_w[c] = 1E8
    # 紧密性生命力? 排除该节点时所有节点对之间距离之和的更改


    return d, d_in, d_out, cluster, cluster_w, ks, dc, dc_in, dc_out, \
           ec, ec_w, soc, bc, bc_d, cc, cc_d, d_weigh_Di, d_weigh, pr, \
           largest_cc, d_unit, d_diff, constraint, constraint_w, \
           effective_size, effective_size_w, hits, d_an, cv, cv_w


def get_community(G):
    # 无向图
    g = nx.from_numpy_array(nx.to_numpy_array(G), create_using=nx.Graph)
    # 节点数
    n = len(G.nodes)

    # 社区划分
    #print('louvain_communities')
    c_lc = community.louvain_communities(g)
    C_LC = []
    for i in range(n):
        for j, s in enumerate(c_lc):
            if i in s:
                C_LC.append(j + 1)
                break

    # Modularity-based communities，模块度最大值算法
    #print('greedy_modularity_communities')
    c_gmc = community.greedy_modularity_communities(G, weight='weight')
    C_GMC = []
    for i in range(n):
        for j, s in enumerate(c_gmc):
            if i in s:
                C_GMC.append(j + 1)
                break

    # GN算法
    #print('girvan_newman')
    communities_generator = community.girvan_newman(G)
    GN = next(communities_generator)
    c_gn = sorted(map(sorted, GN))
    c_GN = []
    for i in range(n):
        for j, s in enumerate(c_gn):
            if i in s:
                c_GN.append(j + 1)
                break

    # KL算法，返回两个社区，元组里边是节点集合
    #print('kernighan_lin_bisection')
    c_kl = community.kernighan_lin_bisection(g)
    c_KL = []
    for i in range(n):
        for j, s in enumerate(c_kl):
            if i in s:
                c_KL.append(j + 1)
                break

    # lpa 标签传播算法 asyn根据标签频率进行传播，使用最新label
    #print('asyn_lpa_communities')
    lpa = community.asyn_lpa_communities(G, weight='weight')
    c_lpa = sorted(map(sorted, lpa))
    C_LPA = []
    for i in range(n):
        for j, s in enumerate(c_lpa):
            if i in s:
                C_LPA.append(j + 1)
                break

    # 半同步标签传播方法，结合了同步和异步模型的优点，只用于无向图
    #print('label_propagation_communities')
    label = community.label_propagation_communities(g)
    c_label = sorted(map(sorted, label))
    C_LABEL = []
    for i in range(n):
        for j, s in enumerate(c_label):
            if i in s:
                C_LABEL.append(j + 1)
                break

    # Asynchronous Fluid Communities algorithm 异步流体社区算法
    # 需要设置分区数、不支持加权图、必须是连通图
    #print('asyn_fluidc')
    c_flu_dict = dict()
    names = locals()
    for i in range(2, 10):
        try:
            c_flu = sorted(map(sorted, community.asyn_fluidc(g, i)))
            C_FLU = []
            for k in range(n):
                for j, s in enumerate(c_flu):
                    if k in s:
                        C_FLU.append(j + 1)
                        break
            c_flu_dict['c_flu_' + str(i)] = C_FLU
        except:
            c_flu_dict['c_flu_' + str(i)] = [None]*51


    return C_LC, C_GMC, c_flu_dict, c_KL, C_LPA, c_GN, C_LABEL

def main(df):

    BG, DG = Grape(df, binomial_cutdown=2)
    # binomial_cutdown 越小，阈值越高
    # g = nx.from_numpy_array(nx.to_numpy_array(G), create_using=nx.Graph)
    ls = list()
    ls.append(DG)
    ls.append(BG)

    # 试截断。。。。
    for i in [3, 4, 5, 10, 15, 20]:
        BG = Grape(df, binomial_cutdown=i)[0]
        ls.append(BG)

    dic = {}
    binomial_cutdown = [0, 2, 3, 4, 5, 10, 15, 20]
    for i, G in enumerate(ls):
        #print('开始第', i, '个图的单指标')
        n, m, d_avg, d_avg_in, d_avg_out, le, ge, density, \
        diameter, aspl, aspl_d, ac, ac_w, trans, wiener, \
        dac \
            = single_indicator(G)

        #print('开始第', i, '个图的多指标')
        d, d_in, d_out, cluster, cluster_w, ks, dc, dc_in, dc_out, \
        ec, ec_w, soc, bc, bc_d, cc, cc_d, d_weigh_Di, d_weigh, pr, \
        largest_cc, d_unit, d_diff, constraint, constraint_w, \
        effective_size, effective_size_w, hits, d_an, cv, cv_w \
            = multiple_indicator(G)

        #print('开始第', i, '个图的社区')
        C_LC, C_GMC, c_flu_dict, c_KL, c_lpa, c_GN, c_label \
            = get_community(G)

        dic[binomial_cutdown[i]] = [
            n, m, d_avg, d_avg_in, d_avg_out, le, ge, density, diameter, aspl, aspl_d, ac, ac_w, trans, wiener, dac,
            d, d_in, d_out, cluster, cluster_w, ks, dc, dc_in, dc_out, ec, ec_w, soc, bc, bc_d, cc, cc_d, d_weigh_Di, d_weigh, pr,
           largest_cc, d_unit, d_diff, constraint, constraint_w, effective_size, effective_size_w, hits, d_an, cv, cv_w,
           C_LC, C_GMC, c_flu_dict, c_KL, c_lpa, c_GN, c_label]

        #print('结束第', i, '个图')

    # print('down')

    return dic


if __name__ == '__main__':
    df = pd.read_csv("/Users/murphy/学业/python/学习-复杂网络/数据/hs92_201001.csv", index_col=0)

    BG, DG = Grape(df, binomial_cutdown=2)
    # binomial_cutdown 越小，阈值越高
    # g = nx.from_numpy_array(nx.to_numpy_array(G), create_using=nx.Graph)
    ls = list()
    ls.append(DG)
    ls.append(BG)

    #试截断。。。。
    for i in (3, 4, 5, 10, 15, 20):
        BG = Grape(df, binomial_cutdown=i)[0]
        ls.append(BG)


    for i, G in enumerate(ls):

        #print('开始第', i, '个图的单指标')
        n, m, d_avg, d_avg_in, d_avg_out, le, ge, density, \
        diameter, aspl, aspl_d, ac, ac_w, trans, wiener, \
        dac \
            = single_indicator(G)

        #print('开始第', i, '个图的多指标')
        d, d_in, d_out, cluster, cluster_w, ks, dc, dc_in, dc_out, \
        ec, ec_w, soc, bc, bc_d, cc, cc_d, d_weigh_Di, d_weigh, pr, \
        largest_cc, d_unit, d_diff, constraint, constraint_w, \
        effective_size, effective_size_w, hits, d_an, cv, cv_w \
            = multiple_indicator(G)

        #print('开始第', i, '个图的社区')
        C_LC, C_GMC, c_flu_dict, c_KL, c_lpa, c_GN, c_label \
            = get_community(G)

        #print('结束第', i, '个图')

    #print('down')
