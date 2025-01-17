import pandas as pd
from pyecharts.charts import Bar, Boxplot, Line, HeatMap, Funnel
from pyecharts import options as opts
from pyecharts.render import make_snapshot
from snapshot_selenium import snapshot

# 读取CSV文件
pandas_df = pd.read_csv('extracted_data.csv')

print("File read successfully")
print(pandas_df.head())

# 定义城市和地区映射
region_mapping = {
    "北京": "华北",
    "天津": "华北",
    "石家庄": "华北",
    "太原": "华北",
    "济南": "华北",
    "郑州": "华北",
    "沈阳": "东北",
    "长春": "东北",
    "哈尔滨": "东北",
    "西安": "西北",
    "兰州": "西北",
    "西宁": "西北",
    "乌鲁木齐": "西北",
    "成都": "西南",
    "贵阳": "西南",
    "昆明": "西南",
    "重庆": "西南",
    "广州": "华南",
    "海口": "华南",
    "南宁": "华南",
    "拉萨": "西南",
    "南京": "华东",
    "杭州": "华东",
    "合肥": "华东",
    "上海": "华东",
    "南昌": "华东",
    "福州": "东南",
    "武汉": "华中",
    "长沙": "华中",
    "银川": "西北",
    "呼和浩特": "华北",
}

# 将城市和地区映射添加到DataFrame
pandas_df['地区'] = pandas_df['城市'].map(region_mapping).fillna('其他')

# 打印地区分布情况
print("\n地区分布情况:")
print(pandas_df['地区'].value_counts())

# 计算每个地区的平均月租
average_price_pd = pandas_df.groupby("地区")["月租"].mean().reset_index()
average_price_pd.columns = ["地区", "平均月租"]

# 计算每个地区的中位月租
median_price_pd = pandas_df.groupby("地区")["月租"].median().reset_index()
median_price_pd.columns = ["地区", "中位月租"]

# 计算每个地区的平均面积
average_area_pd = pandas_df.groupby("地区")["面积"].mean().reset_index()
average_area_pd.columns = ["地区", "平均面积"]

# 计算每个地区的月租标准差
stddev_price_pd = pandas_df.groupby("地区")["月租"].std().reset_index()
stddev_price_pd.columns = ["地区", "月租标准差"]

# 修正楼层类型映射
floor_type_mapping = {1: "低层", 2: "中层", 3: "高层"}
pandas_df['楼层类型'] = pandas_df['楼层类型'].map(floor_type_mapping).fillna("高层")

# 检查楼层类型列的唯一值
distinct_floor_types = pandas_df['楼层类型'].unique()
print("\nDistinct floor types:", distinct_floor_types)

# 计算每个楼层类型的平均月租
average_floor_price_pd = pandas_df.groupby("楼层类型")["月租"].mean().reset_index()
average_floor_price_pd.columns = ["楼层类型", "平均月租"]

# 计算各地区1、2、3楼层房数量及其占比
floor_count_pd = pandas_df[pandas_df['楼层类型'].isin(["低层", "中层", "高层"])].groupby(["地区", "楼层类型"]).size().unstack(fill_value=0)
total_count_pd = floor_count_pd.sum(axis=1)
floor_count_percentage_pd = floor_count_pd.div(total_count_pd, axis=0).fillna(0)

# 对数值进行取整
average_price_pd["平均月租"] = average_price_pd["平均月租"].round().astype(int)
median_price_pd["中位月租"] = median_price_pd["中位月租"].round().astype(int)
average_area_pd["平均面积"] = average_area_pd["平均面积"].round().astype(int)
stddev_price_pd["月租标准差"] = stddev_price_pd["月租标准差"].round().astype(int)
average_floor_price_pd["平均月租"] = average_floor_price_pd["平均月租"].round().astype(int)

# 合并中位数、平均数和标准差的数据
combined_stats_pd = average_price_pd.merge(median_price_pd, on="地区").merge(stddev_price_pd, on="地区")

# 打印各个分析结果
print("\n各地区平均月租:")
print(average_price_pd)
print("\n各地区中位月租:")
print(median_price_pd)
print("\n各地区平均面积:")
print(average_area_pd)
print("\n各地区月租标准差:")
print(stddev_price_pd)
print("\n各楼层平均月租:")
print(average_floor_price_pd)
print("\n各地区1、2、3楼层房数量及占比:")
print(floor_count_percentage_pd)
print("\n各地区综合统计:")
print(combined_stats_pd)

# 各地区房价平均月租柱状图
bar_avg_price = (
    Bar(init_opts=opts.InitOpts(theme='light'))
    .add_xaxis(average_price_pd["地区"].tolist())
    .add_yaxis("平均月租", average_price_pd["平均月租"].tolist(), 
               label_opts=opts.LabelOpts(is_show=True, formatter="{c}"),
               itemstyle_opts=opts.ItemStyleOpts(color="#FF9999"))  # 设置颜色
    .set_global_opts(
        title_opts=opts.TitleOpts(title="各地区房价平均月租"),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
        yaxis_opts=opts.AxisOpts(name="月租"),
    )
)

# 各地区平均面积柱状图
bar_avg_area = (
    Bar(init_opts=opts.InitOpts(theme='light'))
    .add_xaxis(average_area_pd["地区"].tolist())
    .add_yaxis("平均面积", average_area_pd["平均面积"].tolist(), 
               label_opts=opts.LabelOpts(is_show=True, formatter="{c}"),
               itemstyle_opts=opts.ItemStyleOpts(color="#99CCFF"))  # 设置颜色
    .set_global_opts(
        title_opts=opts.TitleOpts(title="各地区平均面积"),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
        yaxis_opts=opts.AxisOpts(name="面积"),
    )
)

# 各楼层类型的平均月租线条图
line_avg_floor_price = (
    Line(init_opts=opts.InitOpts(theme='light'))
    .add_xaxis(average_floor_price_pd["楼层类型"].tolist())
    .add_yaxis("平均月租", average_floor_price_pd["平均月租"].tolist(), 
               label_opts=opts.LabelOpts(is_show=True, formatter="{c}"),
               linestyle_opts=opts.LineStyleOpts(color="#66FF66"),  # 设置颜色
               itemstyle_opts=opts.ItemStyleOpts(color="#66FF66"))  # 设置颜色
    .set_global_opts(
        title_opts=opts.TitleOpts(title="各楼层类型的平均月租"),
        xaxis_opts=opts.AxisOpts(type_="category", name="楼层类型"),
        yaxis_opts=opts.AxisOpts(name="月租"),
    )
)

# 各地区房价中位月租盒须图
boxplot_median_price = Boxplot(init_opts=opts.InitOpts(theme='light'))
citiestats = boxplot_median_price.prepare_data([median_price_pd["中位月租"].tolist()])
boxplot_median_price.add_xaxis(["月租"])
boxplot_median_price.add_yaxis("", citiestats, label_opts=opts.LabelOpts(is_show=False))
boxplot_median_price.set_global_opts(
    title_opts=opts.TitleOpts(title="全部地区月租Box")
)

# 各地区1、2、3楼层房数量的占比热力图
regions = floor_count_percentage_pd.index.tolist()
floor_counts = floor_count_percentage_pd.values.tolist()

heatmap_chart = (
    HeatMap(init_opts=opts.InitOpts(theme='light'))
    .add_xaxis(regions)
    .add_yaxis("楼层类型", ["低层", "中层", "高层"], [[i, j, value] for i, values in enumerate(floor_counts) for j, value in enumerate(values)])
    .set_global_opts(
        title_opts=opts.TitleOpts(title="各地区1、2、3楼层房数量的占比"),
        visualmap_opts=opts.VisualMapOpts(min_=0, max_=1, orient="horizontal", pos_bottom="10%")
    )
)

# 各地区平均月租的漏斗图
funnel_chart = (
    Funnel(init_opts=opts.InitOpts(theme='light'))
    .add(
        series_name="平均月租",
        data_pair=[(region, price) for region, price in zip(average_price_pd["地区"], average_price_pd["平均月租"])],
        sort_="descending",
        label_opts=opts.LabelOpts(position="inside"),
        tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c}")
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="各地区平均月租漏斗图")
    )
)

# 各地区月租中位数、平均月租和月租方差的复合柱状图
composite_bar_chart = (
    Bar(init_opts=opts.InitOpts(theme='light'))
    .add_xaxis(combined_stats_pd["地区"].tolist())
    .add_yaxis("平均月租", combined_stats_pd["平均月租"].tolist(),
               label_opts=opts.LabelOpts(is_show=True, formatter="{c}"),
               itemstyle_opts=opts.ItemStyleOpts(color="#FF9999"))
    .add_yaxis("中位月租", combined_stats_pd["中位月租"].tolist(),
               label_opts=opts.LabelOpts(is_show=True, formatter="{c}"),
               itemstyle_opts=opts.ItemStyleOpts(color="#99CCFF"))
    .add_yaxis("月租标准差", combined_stats_pd["月租标准差"].tolist(),
               label_opts=opts.LabelOpts(is_show=True, formatter="{c}"),
               itemstyle_opts=opts.ItemStyleOpts(color="#66FF66"))
    .set_global_opts(
        title_opts=opts.TitleOpts(title="各地区月租中位数、平均月租和月租标准差"),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
        yaxis_opts=opts.AxisOpts(name="月租"),
        legend_opts=opts.LegendOpts(pos_top="10%"),
    )
)

# 渲染图表到PNG文件
make_snapshot(snapshot, bar_avg_price.render(), "region_average_price.png")
make_snapshot(snapshot, boxplot_median_price.render(), "region_median_price_boxplot.png")
make_snapshot(snapshot, bar_avg_area.render(), "region_average_area.png")
make_snapshot(snapshot, line_avg_floor_price.render(), "floor_average_price_line.png")
make_snapshot(snapshot, heatmap_chart.render(), "floor_count_heatmap.png")
make_snapshot(snapshot, funnel_chart.render(), "average_rental_funnel.png")
make_snapshot(snapshot, composite_bar_chart.render(), "composite_rental_stats.png")



