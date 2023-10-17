# Trade-Network-Resilience-Reseach-Code

1. 爬虫 \
    使用crawler中的new-crawler.py文件，在main函数中修改需要获取的HS编号，在regions数组中修改需要获取的国家地区。\
    注：目前regions数组为GDP前50国家地区；代码运行时需要和同文件夹的json文件放在一起。

2. 构建邻接矩阵 \
    使用1-UN或2-UN代码解析爬虫获得的数据，并录入数据库。1-UN忽略world数据，2-UN包含world数据。\
    注：需要添加数据库相关信息，已在代码中标注。

3. 指标计算 \
    使用3-UN或4-UN代码，两者计算指标相同，只有单进程和多进程的区别。\
    注：同样需要添加数据库信息；需要和Indicator_new.py在同一文件夹内，Indicator_new.py无需改动。
   
