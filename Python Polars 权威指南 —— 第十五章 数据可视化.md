

前面的章节为你提供了将原始数据转换为精美DataFrame所需的所有工具。但如何将这样的DataFrame转变为有意义的洞察呢？

一种方法是通过数据可视化，而Python为此提供了大量的包。可用的包包括hvPlot（用于快速可视化）、Matplotlib（用于底层绘图）、Bokeh（用于交互式图表）和Plotnine（用于在Python中利用图形语法）。

这既是个好事也是个坏事，因为虽然很可能有适合你需求的包，但选择合适的包却具有挑战性。此外，每个包都有其独特的功能、假设和陷阱。

![图15-1展示了Python复杂的数据可视化生态系统](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728837523430-2c9dffea-d077-4401-9110-f41173b2e235.png)



数据可视化不仅仅是制作漂亮的图形；它是数据科学的一个基本部分。通过将DataFrame转换为图形形式，你可以更好地理解趋势、发现异常值，并讲述能够影响决策的故事。有效的数据可视化能够阐明模糊的概念，简化复杂的问题，使数据更易于访问。

在本章中，你将学习如何：

+ 使用Polars的内置绘图功能快速创建柱状图、散点图、密度图和直方图
+ 组合和分层多个图表
+ 自定义图表
+ 创建交互式可视化
+ 在地图上绘制数百万个点
+ 使用Plotnine等替代可视化包
+ 使用Great Tables包创建美观的带nan图的表格

到本章结束时，你将对每个包的功能有一个好的了解，知道何时使用哪个包，以及如何将它们与Polars结合使用。但首先，我们需要谈谈我们将使用的数据集。

<h2 id="xHUTk">NYC自行车骑行</h2>
在本章中，我们将使用同一数据，即纽约市[Citi Bike](https://citibikenyc.com/system-data:)租赁自行车的骑行数据。以下是DataFrame的样子。

```python
import polars as pl

trips = pl.read_parquet("data/biketrips/*.parquet")

print(trips[:,:4])
print(trips[:,4:7])
print(trips[:,7:11])
print(trips[:,11:])
```

```plain
shape: (2_735_398, 4)
┌───────────┬────────────┬─────────────────────┬─────────────────────┐
│ bike_type │ rider_type │ datetime_start      │ datetime_end        │
│ ---       │ ---        │ ---                 │ ---                 │
│ cat       │ cat        │ datetime[μs]        │ datetime[μs]        │
╞═══════════╪════════════╪═════════════════════╪═════════════════════╡
│ electric  │ member     │ 2024-03-01 00:00:02 │ 2024-03-01 00:27:39 │
│ electric  │ member     │ 2024-03-01 00:00:04 │ 2024-03-01 00:09:29 │
│ …         │ …          │ …                   │ …                   │
│ electric  │ casual     │ 2024-03-31 23:59:57 │ 2024-04-01 00:15:39 │
│ classic   │ casual     │ 2024-03-31 23:59:58 │ 2024-04-01 00:01:38 │
└───────────┴────────────┴─────────────────────┴─────────────────────┘
shape: (2_735_398, 3)
┌──────────────┬──────────────────────────────┬─────────────────────────┐
│ duration     │ station_start                │ station_end             │
│ ---          │ ---                          │ ---                     │
│ duration[μs] │ str                          │ str                     │
╞══════════════╪══════════════════════════════╪═════════════════════════╡
│ 27m 37s      │ W 30 St & 8 Ave              │ Maiden Ln & Pearl St    │
│ 9m 25s       │ Longwood Ave & Southern Blvd │ Lincoln Ave & E 138 St  │
│ …            │ …                            │ …                       │
│ 15m 42s      │ Hart St & Wyckoff Ave        │ Monroe St & Bedford Ave │
│ 1m 40s       │ 5 Ave & E 30 St              │ 5 Ave & E 30 St         │
└──────────────┴──────────────────────────────┴─────────────────────────┘
shape: (2_735_398, 4)
┌────────────────────┬────────────────────┬───────────────┬─────────────┐
│ neighborhood_start │ neighborhood_end   │ borough_start │ borough_end │
│ ---                │ ---                │ ---           │ ---         │
│ str                │ str                │ str           │ str         │
╞════════════════════╪════════════════════╪═══════════════╪═════════════╡
│ Chelsea            │ Financial District │ Manhattan     │ Manhattan   │
│ Longwood           │ Mott Haven         │ Bronx         │ Bronx       │
│ …                  │ …                  │ …             │ …           │
│ Bushwick           │ Bedford-Stuyvesant │ Brooklyn      │ Brooklyn    │
│ Midtown            │ Midtown            │ Manhattan     │ Manhattan   │
└────────────────────┴────────────────────┴───────────────┴─────────────┘
shape: (2_735_398, 5)
┌───────────┬────────────┬───────────┬────────────┬──────────┐
│ lat_start │ lon_start  │ lat_end   │ lon_end    │ distance │
│ ---       │ ---        │ ---       │ ---        │ ---      │
│ f64       │ f64        │ f64       │ f64        │ f64      │
╞═══════════╪════════════╪═══════════╪════════════╪══════════╡
│ 40.749614 │ -73.995071 │ 40.707065 │ -74.007319 │ 4.83703  │
│ 40.816459 │ -73.896576 │ 40.810893 │ -73.927311 │ 2.665806 │
│ …         │ …          │ …         │ …          │ …        │
│ 40.704865 │ -73.919904 │ 40.685144 │ -73.953809 │ 3.606664 │
│ 40.745985 │ -73.986295 │ 40.745985 │ -73.986295 │ 0.0      │
└───────────┴────────────┴───────────┴────────────┴──────────┘
```



在2024年3月，纽约市四个区的Citi Bike骑行超过270万次：布朗克斯区、布鲁克林区、曼哈顿区和皇后区（第五个区史坦顿岛没有任何Citi Bike站）。每个区都有许多社区。

+ `bike_type`要么是“电动”，要么是“经典”；
+ `rider_type`要么是“会员”，要么是“临时”；
+ `duration`是`datetime_start`和`datetime_end`之间的差值
+ `lat_start`、`lon_start`、`lat_end`和`lon_end`这四个列分别是起始和结束的GPS坐标
+ `distance`是这两个坐标之间的直线距离（千米），而不是实际行驶的距离。

我们目前只进行了一些数据清理，即删除所有在同一自行车站点开始和结束，并且持续时间少于五分钟的骑行，因为那些实际上不是骑行：

```python
trips = trips.filter(
    ~((pl.col("station_start") == pl.col("station_end")) &
      (pl.col("duration").dt.total_seconds() < 5*60))
)
trips.height
```

```plain
2672594
```



例如，最后一次骑行“5 Ave & E 30 St”开始并结束，持续1分40秒，因此会被删除。

一旦完成，DataFrame `trips`仍然有近270万行，包含时间戳、类别、名称和坐标等多种列。这将使我们能够生成许多有趣的数据可视化。

好了，让我们来看看人们何时骑车、骑行距离多远以及哪些车站最受欢迎，通过一些数据可视化。

<h2 id="ABrFN">使用hvPlot的内置绘图</h2>
将DataFrame快速转化为数据可视化的最简单方法是使用Polars提供的内置方法。这些方法通过`df.plot`命名空间访问，例如：`df.plot.scatter()`和`df.plot.bar()`。在后台，这些方法被转发到另一个包hvPlot。

hvPlot不同于大多数数据可视化包，因为它本身不执行任何可视化。相反，它提供了一个统一的接口，连接多个其他数据可视化包，而不会将用户锁定在某一个包中。图15-2展示了hvPlot的架构概述。让我们逐步了解这一架构。

![图15-2. hvPlot 为Bokeh, Matplotlib, and Plotly提供了统一接口](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728837762941-2b944d17-53bb-495a-8691-0047f4bbec49.png)



首先，hvPlot的绘图方法接受Polars、Pandas、Ibis DataFrame以及PyData生态系统中的许多其他数据结构。其次，hvPlot使用HoloViews包构造一个中间的、包无关的可视化表示。可以把这个表示看作是如何创建图表的描述。

它在需要时可以选择使用Datashader包，例如在地图上绘制数百万个点时。第三，hvPlot将中间表示转换为Bokeh、Matplotlib或Plotly的规格。这时，用户可以使用特定包的语法自定义图表。最后，输出包呈现图表，即将原始数据转化为像素。

<h3 id="XeMtT">第一个图表</h3>
让我们从一个散点图开始，这是可视化两个连续值之间关系的好方法。由于我们的骑行数据集相当大，我们将仅保留在“W 21 St & 6 Ave”车站开始的骑行数据，这恰好是所有车站中最繁忙的：

```python
trips_speed = (
    trips
    .filter(pl.col("station_start") == "W 21 St & 6 Ave")
    .select(  ①
        pl.col("distance"),
        pl.col("duration").dt.total_seconds() / 3600,  ②
        pl.col("bike_type")
    )
)
trips_speed
```

```plain
shape: (10_981, 3)
┌──────────┬──────────┬───────────┐
│ distance │ duration │ bike_type │
│ ---      │ ---      │ ---       │
│ f64      │ f64      │ cat       │
╞══════════╪══════════╪═══════════╡
│ 0.452909 │ 0.026944 │ electric  │
│ 0.993271 │ 0.089444 │ electric  │
│ …        │ …        │ …         │
│ 0.992056 │ 0.059167 │ electric  │
│ 3.690942 │ 0.326389 │ electric  │
└──────────┴──────────┴───────────┘
```

① 我们仅保留可视化所需的列。这并不是必需的，但有助于展示我们使用的数据。

②`duration`列的单位是小时。或者，可以使用`Expr.dt.total_hours()`方法，但这只返回整数小时。



下面的代码片段使用`df.plot.scatter`构建散点图：

```python
trips_speed.plot.scatter(x="distance", y="duration", color="bike_type",  ①
                         xlabel="distance (km)", ylabel="duration (h)",  ②
                         ylim=(0, 2))  ③
```

① 这三个参数是最重要的，因为它们确定每个点的位置和颜色所使用的列。

② 添加或更改标签不是必需的，但可以使轴代表的内容更加清晰。

③ 我们手动限制y轴的范围，因为有些更长的骑行会影响可视化，使较小值更难以看清。你也可以通过对DataFrame应用过滤来解决这类问题。

![图15-3 每种自行车的骑行距离和时长的关系](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728837900667-d177492b-6226-4a01-bac8-9f91db9546c4.png)

图15-3显示，通常来说，电动自行车比经典自行车速度更快，行驶距离更远。

<h3 id="yoNNh">绘图命名空间中的方法</h3>
`df.plot.scatter()`是`df.plot`命名空间中众多可用方法之一。要查看哪些方法可用，可以使用标签补全（即在`df.plot.`后按TAB键）：

`trips.plot.<TAB>`  
可用的方法包括：

+ `df.plot.area()`: 绘制一个区域图，类似于线图，但填充曲线下方的区域，并可选择堆叠。
+ `df.plot.bar()`: 绘制一个柱状图，可以堆叠或分组。
+ `df.plot.bivariate()`: 绘制一组点的二维密度。
+ `df.plot.box()`: 绘制箱线图，比较一个或多个变量的分布。
+ `df.plot.density()`: 绘制一个或多个变量的核密度估计。
+ `df.plot.heatmap()`: 绘制热图以可视化变量在两个独立维度上的分布。
+ `df.plot.hexbins()`: 绘制六边形箱图。
+ `df.plot.hist()`: 绘制一个或多个变量的分布直方图。
+ `df.plot.line()`: 绘制线图（如时间序列）。
+ `df.plot.scatter()`: 绘制散点图，比较两个变量。
+ `df.plot.violin()`: 绘制小提琴图，使用核密度估计比较一个或多个变量的分布。



<h3 id="u2cTz">获取方法的帮助</h3>
通常，要获取某个方法的帮助，你可以输入`help(<method>)`或`?<method>`。但我们不推荐对`df.plot`命名空间中的方法使用这种方式。例如，`df.plot.scatter()`方法的文档包含334行文本，信息量非常大，并且许多参数可能并不总是相关。

幸运的是，hvPlot 提供了自己的`help()`函数，可以让你禁用某些部分：

```python
import hvplot
hvplot.help('scatter', generic=False, style=False)
```

```plain
scatter图将你的点在2D空间中作为标记可视化。你还可以通过使用颜色来可视化更多的维度。
scatter图是用来绘制具有非连续轴数据的好方法。
参考链接: https://hvplot.holoviz.org/reference/tabular/scatter.html
参数
● x：字符串，可选
从字段名中获取x轴位置。如果未指定，则使用索引。可以是连续和分类数据。
● y：字符串或列表，可选
从字段名中获取y轴位置。如果未指定，使用所有数值字段。
● marker：字符串，可选
标记形状，可以是matplotlib支持的任何形状，参见 https://matplotlib.org/stable/api/markers_api.html。
● c：字符串，可选
```



[hvPlot 的网站](https://hvplot.holoviz.org/)还提供了很棒的文档，包括许多示例。不过请记住，很多页面和示例是基于Pandas的。这可以理解，因为Pandas比Polars早了十年。一些示例假定你的DataFrame具有Index，甚至是MultiIndex，而Polars的DataFrame并没有。在下节中，我们提供了遇到这种情况时的建议。

<h3 id="PIWTa">Pandas作为备选方案</h3>
hvPlot的底层首先将Polars DataFrame转换为Pandas DataFrame。它只复制绘图所需的列。大多数情况下，这种方法运作良好，但并非总是如此。

这是一个示例，我们想创建一个热图。hvPlot的文档提到特殊的`.hour`和`.day`修饰符，分别用于提取DateTime的小时和天。然而，不幸的是，这还不支持Polars DataFrame，因此以下代码会产生错误：

```python
trips_per_day_hour = (
    trips
    .sort("datetime_start")
    .group_by_dynamic("datetime_start", every="1h")
    .agg(pl.len())
)

trips_per_day_hour.plot.heatmap(x='datetime_start.hour',
                                y='datetime_start.day',
                                C='len', cmap='reds')
```

```plain
ValueError: 'datetime_start.hour' is not in list
```



我们得到一个错误，因为hvPlot尝试复制列`datetime_start.day`，但这列不存在于我们的DataFrame中。别担心；我们可以使用`df.to_pandas()`方法切换回Pandas：

```python
import hvplot.pandas
trips_per_day_hour.to_pandas().hvplot.heatmap(x='datetime_start.hour',
                                              y='datetime_start.day',
                                              C='len', cmap='reds')
```

![图15-4. 在Polars尚未支持的情况下，Pandas是一个很好的备选方案](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728838296927-71dc0b23-f0f0-451f-98f8-8875788ef9e9.png)

<h3 id="wcqLD">手动转换</h3>
让我们尝试另一个绘图示例——柱状图。柱状图特别适合展示不同组的计数。hvPlot期望你提供的DataFrame包含要使用的实际值；它不会为你进行任何转换。

我们对每种自行车类型和骑行者类型的骑行次数感兴趣。由于这些计数在我们的DataFrame中没有明确显示，我们需要手动计算：

```python
trips_type_counts = trips.group_by("rider_type", "bike_type").len()
trips_type_counts
```

```plain
shape: (4, 3)
┌────────────┬───────────┬─────────┐
│ rider_type │ bike_type │ len     │
│ ---        │ ---       │ ---     │
│ cat        │ cat       │ u32     │
╞════════════╪═══════════╪═════════╡
│ casual     │ electric  │ 295530  │
│ member     │ electric  │ 1420012 │
│ casual     │ classic   │ 120888  │
│ member     │ classic   │ 836164  │
└────────────┴───────────┴─────────┘
```



一旦我们有了这些数据，就可以创建一个堆叠柱状图，如下所示：

```python
trips_type_counts.plot.bar(x="rider_type", y="len", by="bike_type",
                           ylabel="count", stacked=True,
                           color=["orange", "green"])
```

![图15-5. 显示按自行车类型和骑行者类型划分的出行次数的堆叠柱状图](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728838378537-28d17629-5af2-4037-ae1b-ca67c6b9a6fe.png)

图15-5展示了每种自行车类型和骑行者类型的骑行次数堆叠柱状图。我们可以看到，大多数骑行由Citi Bike的会员完成，并且大多数会员使用电动自行车。

<h3 id="ldr6H">更改绘图后端</h3>
hvPlot的默认绘图后端是Bokeh。在大多数情况下，这已经足够了，但在某些情况下，切换到Plotly或Matplotlib更为有用。例如，Matplotlib在不需要交互式可视化时非常有用。或者，当你需要与报告中的其他可视化风格匹配时，也可以选择Matplotlib。

可以通过使用`hvplot.extension()`方法并传递“matplotlib”或“plotly”来更改后端。为此，你首先需要显式导入hvPlot包：

```python
import hvplot
hvplot.extension("matplotlib")
```



让我们使用Matplotlib后端创建前一节中的同一个柱状图：

```python
trips_type_counts.plot.bar(x="rider_type", y="len", by="bike_type",
                           ylabel="count", stacked=True,
                           color=["orange", "green"])
```

![图15-6. 与之前相同的柱状图，但现在由Matplotlib后端生成](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728838433037-f7f1e704-6f89-4e27-9c3a-56794fc1ad34.png)

图15-6展示了与之前相同的柱状图，只不过现在由Matplotlib后端生成。除了少量的视觉差异，代码未做任何改变。

让我们将绘图后端重置为Bokeh，以便继续本章内容：

```python
hvplot.extension("bokeh")
```

<h3 id="gdLTH">在地图上绘制点</h3>
我们尚未真正使用DataFrame中的坐标。让我们通过创建一个地图来改变这一点。你可能会像以前一样创建一个散点图：

```python
trips.plot.scatter(x='lon_start', y='lat_start', color='borough_start',
                   width=600, height=600)
```

![图15-7. 简单的散点图不适合地理数据。](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728838501610-123e2ae7-7ac3-47a4-a5dc-ce484a42379f.png)

然而，这是一个不太好的图。它缺乏上下文，坐标没有正确投影，并且由于数据量过大，生成该图可能需要一分钟。

但数据可视化本质上是一个迭代的过程。可视化坐标的更好方法是使用`df.plot.points()`方法，并将`geo`参数设置为`True`。这确保坐标正确投影到适当的地图上：

```python
trips.plot.points(x="lon_start", y="lat_start",
                  datashade=True, geo=True,
                  tiles="CartoLight",
                  width=800, height=600)
```

![图15-8. 互动地理图。](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728838550106-1ad93526-01b5-435d-99cb-efcdc0eee898.png)

很难在书中展示，但图15-8中的静态地图实际上是一个交互式可视化。你可以平移和缩放。由于我们设置了`datashade`参数为`True`，因此只使用必要的数据，从而最大限度地提高了效率。

<h3 id="g1bIz">组合图表</h3>
有时，单个图表是不够的。使用hvPlot，你可以将多个图表组合为一个图表。组合有两种类型：堆叠和分层。

首先，我们准备一些允许我们绘制折线图的数据：

```python
trips_hour_num_speed = (
    trips
    .sort("datetime_start")
    .group_by_dynamic("datetime_start", every="1h")
    .agg(num_trips=pl.len(),
         speed=(pl.col("distance") / (pl.col("duration").dt.total_seconds() / 3600)).median())
    .filter(pl.col("datetime_start") > pl.date(2024, 3 , 26))
)

trips_hour_num_speed
```

```plain
shape: (143, 3)
┌─────────────────────┬───────────┬───────────┐
│ datetime_start      │ num_trips │ speed     │
│ ---                 │ ---       │ ---       │
│ datetime[μs]        │ u32       │ f64       │
╞═════════════════════╪═══════════╪═══════════╡
│ 2024-03-26 01:00:00 │ 298       │ 13.797211 │
│ 2024-03-26 02:00:00 │ 182       │ 14.312177 │
│ 2024-03-26 03:00:00 │ 124       │ 12.851984 │
│ 2024-03-26 04:00:00 │ 235       │ 13.787607 │
│ 2024-03-26 05:00:00 │ 888       │ 13.920884 │
│ …                   │ …         │ …         │
│ 2024-03-31 19:00:00 │ 5216      │ 10.696787 │
│ 2024-03-31 20:00:00 │ 3687      │ 11.058714 │
│ 2024-03-31 21:00:00 │ 2878      │ 11.445669 │
│ 2024-03-31 22:00:00 │ 2354      │ 11.422508 │
│ 2024-03-31 23:00:00 │ 1603      │ 11.884897 │
└─────────────────────┴───────────┴───────────┘
```



在第一个代码片段中，我们使用`+`运算符组合了两个图表，这会将两个图表并排放置：

```python
(
    trips_hour_num_speed.plot.line(x="datetime_start", y="num_trips") +
    trips_hour_num_speed.plot.line(x="datetime_start", y="speed")
).cols(1) ①
```

① 通过`.cols()`方法，我们确保两个图表垂直排列在一起。

![图15-9. 两个图表可以并排放置](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728838635011-6092d36f-0d4d-4adf-b463-a48900399a4c.png)

在第二个代码片段中，我们组合了两个图表：

```python
(
    trips_hour_num_speed.plot.line(x="datetime_start", y="num_trips") *
    trips_hour_num_speed
    .filter(pl.col("num_trips") > 9000)
    .plot.scatter(x="datetime_start", y="num_trips", c="red", s=50)
)
```

![图15-10. 两个图表可以重叠放置](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728838877439-b3e5ad90-a224-4151-91dc-b758cfaf41a8.png)

请注意，我们使用了两个不同的DataFrame（第二个是第一个的子集）和两种不同的图表类型（折线图和散点图）。

<h3 id="vxxCX">添加交互式小部件</h3>
Bokeh 后端已经提供了交互功能：你可以放大、缩小、平移以查看图表的不同部分，并悬停在元素上以获取更多信息。

通过使用`groupby`关键字参数，你可以添加一个或多个小部件。传递给此参数的列名会将数据分为多个子集。借助这些小部件，你可以选择用于绘图的数据子集。

这是一个按日期分组的示例。小部件的类型基于列的类型。正如图15-11所示，选择日期的小部件是一个滑块。

```python
trips_per_hour = (
    trips
    .sort("datetime_start")
    .group_by_dynamic("datetime_start", group_by="borough_start", every="1h")
    .agg(pl.len())
    .with_columns(date=pl.col("datetime_start").dt.date())
)
trips_per_hour
```

```plain
shape: (2_972, 4)
┌───────────────┬─────────────────────┬─────┬────────────┐
│ borough_start │ datetime_start      │ len │ date       │
│ ---           │ ---                 │ --- │ ---        │
│ str           │ datetime[μs]        │ u32 │ date       │
╞═══════════════╪═════════════════════╪═════╪════════════╡
│ Manhattan     │ 2024-03-01 00:00:00 │ 480 │ 2024-03-01 │
│ Manhattan     │ 2024-03-01 01:00:00 │ 294 │ 2024-03-01 │
│ Manhattan     │ 2024-03-01 02:00:00 │ 187 │ 2024-03-01 │
│ Manhattan     │ 2024-03-01 03:00:00 │ 100 │ 2024-03-01 │
│ Manhattan     │ 2024-03-01 04:00:00 │ 126 │ 2024-03-01 │
│ …             │ …                   │ …   │ …          │
│ Queens        │ 2024-03-31 19:00:00 │ 366 │ 2024-03-31 │
│ Queens        │ 2024-03-31 20:00:00 │ 336 │ 2024-03-31 │
│ Queens        │ 2024-03-31 21:00:00 │ 211 │ 2024-03-31 │
│ Queens        │ 2024-03-31 22:00:00 │ 176 │ 2024-03-31 │
│ Queens        │ 2024-03-31 23:00:00 │ 144 │ 2024-03-31 │
└───────────────┴─────────────────────┴─────┴────────────┘
```

```python
trips_per_hour.plot.line(x="datetime_start", by="borough_start",
                         groupby="date", widget_location='left_top')
```

![图15-11. 通过groupby关键字参数轻松添加交互式小部件](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728838955771-cd81d014-666c-4900-8415-c60b3e4004c4.png)**<font style="color:rgb(61, 59, 73);">  
</font>**

<font style="color:rgb(61, 59, 73);">你还可以将列名列表传递给</font>`<font style="color:rgb(61, 59, 73);">groupby</font>`<font style="color:rgb(61, 59, 73);">关键字参数，以创建多个小部件。</font>

<h3 id="wMgEB">常见的自定义选项</h3>
hvPlot提供了许多通过附加关键字参数自定义图表的方式。这些关键字参数在所有图表类型中是通用的。我们已经使用了一些，例如`ylim`、`width`和`height`。

我们最常用的关键字参数来提高图表可读性，列在表15-1中。



_表15-1. hvPlot的常见参数_

| 参数 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">cmap</font>**` | 设置色彩映射。默认值为`None`。常见的有`Category10`、`viridis`和`fire`。详见Holoviews用户指南中的色彩映射部分。 |
| `**<font style="color:#01B2BC;">fontscale</font>**` | 按相同比例缩放所有字体的大小。例如，`fontscale=1.5`将所有字体（标题、xticks、标签等）放大50%。默认值为1。 |
| `**<font style="color:#01B2BC;">grid</font>**` | 是否显示网格。默认值为`False`。 |
| `**<font style="color:#01B2BC;">logx</font>**`**<font style="color:#01B2BC;">, </font>**`**<font style="color:#01B2BC;">logy</font>**` | 分别启用对x轴和y轴的对数刻度。默认值均为`False`。 |
| `**<font style="color:#01B2BC;">rot</font>**` | 将x轴上的刻度旋转指定角度。默认值为0。 |
| `**<font style="color:#01B2BC;">title</font>**` | 图表标题。默认值为`""`。 |
| `**<font style="color:#01B2BC;">width</font>**`**<font style="color:#01B2BC;">, </font>**`**<font style="color:#01B2BC;">height</font>**` | 图表的宽度和高度，以像素为单位。默认值分别为700和300。 |
| `**<font style="color:#01B2BC;">xlabel</font>**`**<font style="color:#01B2BC;">, </font>**`**<font style="color:#01B2BC;">ylabel</font>**`**<font style="color:#01B2BC;">, </font>**`**<font style="color:#01B2BC;">clabel</font>**` | 分别为x轴、y轴和颜色条设置轴标签。默认值为`None`，在这种情况下，列名将用作标签。 |
| `**<font style="color:#01B2BC;">xlim</font>**`**<font style="color:#01B2BC;">, </font>**`**<font style="color:#01B2BC;">ylim</font>**` | 分别为x轴和y轴设置图表范围。默认值为`None`，可使用包含两个数值的元组或列表。 |


> 详见[Holoviews用户指南](https://holoviews.org/user_guide/Colormaps.html)中的色彩映射部分。
>



这是一个展示这些关键字参数的示例。由于进行了大量自定义，图15-12可能是本书中最丑的图表。它还包括一些失误：颜色的重复使用、y轴未从零开始、无意义的对数刻度。但为了演示如何自定义图表，这是我们必须付出的代价。（确实存在过度自定义的问题）。让我们尝试一下：

```python
busiest_stations = (
    trips
    .group_by(station="station_start").agg(
        num_trips=pl.len(),
    )
    .sort("num_trips", descending=True)
    .head(20)
)

busiest_stations
```

```plain
shape: (20, 2)
┌───────────────────────┬───────────┐
│ station               │ num_trips │
│ ---                   │ ---       │
│ str                   │ u32       │
╞═══════════════════════╪═══════════╡
│ W 21 St & 6 Ave       │ 10981     │
│ Forsyth St & Broome … │ 9988      │
│ Broadway & W 58 St    │ 9771      │
│ 8 Ave & W 31 St       │ 8977      │
│ Delancey St & Eldrid… │ 8947      │
│ …                     │ …         │
│ Ave A & E 14 St       │ 7194      │
│ West St & Chambers S… │ 6709      │
│ W 30 St & 10 Ave      │ 6505      │
│ Amsterdam Ave & W 73… │ 6484      │
│ W 43 St & 10 Ave      │ 6451      │
└───────────────────────┴───────────┘
```

```python
fig = busiest_stations.plot.bar(x="station", y="num_trips", color="num_trips",
                                cmap="viridis",
                                fontscale=1.2,
                                grid=True,
                                logx=False, logy=True,
                                rot=45,
                                title="Busiest Citi Bike Stations",
                                width=800, height=400,
                                xlabel="", ylabel="Number of trips",
                                xlim=None, ylim=(4000, None))

fig
```

![图15-12. 过度自定义也是一种存在的情况](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728839126572-28d623b1-ecab-4a0a-97d6-bb779330889c.png)



可以使用`hv.plot.help(kind="...")`查看所有可用的关键字参数。部分参数用于通用自定义，部分属于特定类型的图表。

Holoviews用户指南提供了如何使用`.opts()`方法自定义底层Holoviews表示的信息。在该方法中，使用`hooks`参数可以指定由后端（Bokeh、Matplotlib 和 Plotly）处理的进一步自定义。为了扩展上述图（称为`fig`），下面是一个使用这两种自定义方法的代码片段，生成图15-13：

```python
def bokeh_hook(plot, element):
    plot.handles["yaxis"].major_label_text_color = "blue"
    plot.handles["plot"].title.align = "right"

fig.opts(invert_axes=True, invert_yaxis=True, hooks=[bokeh_hook])
```

![图15-13. 就在你以为无法再进一步自定义时！](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728839187613-5deb86d0-e56c-49ac-81a5-c65b7bcdf58c.png)

<h2 id="WBrzi">替代包</h2>
hvPlot并不是唯一可以用来可视化Polars DataFrame的包。在本节中，我们将介绍两个替代方案：Plotnine和Great Tables。

<h3 id="Pa3MT">Plotnine</h3>
[Plotnine](https://plotnine.org/:)是基于图形语法的分层数据可视化包，由Hassan Kibirige创建。它的API类似于R语言中广受欢迎的ggplot2包，该包由Hadley Wickham等人创建。

这种基础的图形语法伴随着一致的API，可以让你快速迭代地创建不同类型的漂亮数据可视化，几乎无需查阅文档。

Plotnine包可以从PyPI安装：

```bash
pip install plotnine
```

并可以通过以下方式导入：

```python
from plotnine import *
```



---

**<font style="color:#01B2BC;">导入所有内容</font>**

虽然通常不建议将所有内容导入全局命名空间，但我们认为在像笔记本这样的临时环境中这样做没问题，因为这使得使用Plotnine的众多函数更加方便。

如果你不想在全局命名空间中引入混乱，我们建议你使用`import plotnine as p9`并在每个函数前加上`p9`前缀。

---



我们将使用Plotnine创建带有些许变化的散点图。首先，我们将添加一个额外的层次——毕竟它基于图形语法的分层结构。然后，我们将把散点图转换为具有四个面板的图表。

以下代码片段准备了一个DataFrame，再次展示了距离和持续时间之间的关系。不过这次我们将在自行车站的层级上操作，使用每个站的距离和持续时间中位数。我们想回答的问题是，距离和持续时间在多大程度上相关。我们只考虑在同一区域内的自行车骑行：

```python
trips_speed = (
    trips.group_by("neighborhood_start", "neighborhood_end").agg(
        pl.col("duration").dt.total_seconds().median() / 3600,
        pl.col("distance").median(),
        pl.col("borough_start").first(),
        pl.col("borough_end").first(),
        pl.len(),
    ).filter(
        (pl.col("len") > 30) &
        (pl.col("distance") > 0.2) &
        (pl.col("neighborhood_start") != pl.col("neighborhood_end")),
    ).with_columns(
        speed=pl.col("distance") / pl.col("duration")
    ).sort("borough_start")
)
trips_speed
```

```plain
shape: (2_971, 8)
┌───────────────┬───────────────┬──────────┬───┬─────────────┬─────┬───────────┐
│ neighborhood_ │ neighborhood_ │ duration │ … │ borough_end │ len │ speed     │
│ start         │ end           │ ---      │   │ ---         │ --- │ ---       │
│ ---           │ ---           │ f64      │   │ str         │ u32 │ f64       │
│ str           │ str           │          │   │             │     │           │
╞═══════════════╪═══════════════╪══════════╪═══╪═════════════╪═════╪═══════════╡
│ Morris        │ East          │ 0.252778 │ … │ Bronx       │ 38  │ 12.14096  │
│ Heights       │ Morrisania    │          │   │             │     │           │
│ Tremont       │ West Farms    │ 0.080833 │ … │ Bronx       │ 121 │ 12.197868 │
│ …             │ …             │ …        │ … │ …           │ …   │ …         │
│ Long Island   │ Clinton Hill  │ 0.469861 │ … │ Brooklyn    │ 58  │ 13.09114  │
│ City          │               │          │   │             │     │           │
│ Long Island   │ West Village  │ 0.539444 │ … │ Manhattan   │ 61  │ 10.26295  │
│ City          │               │          │   │             │     │           │
└───────────────┴───────────────┴──────────┴───┴─────────────┴─────┴───────────┘
```

以下是创建第一个散点图所需的Plotnine代码。每行（在图15-14中每个点）是同一区域内起始站点和终点站点的配对。

```python
(
    ggplot(data=trips_speed
           .filter(pl.col("borough_start") == pl.col("borough_end")),
           mapping=aes(x="distance", y="duration", color="borough_end")) +
    geom_point(size=0.25, alpha=0.5) +
    geom_smooth(method="lowess", size=2, se=False, alpha=0.8) +
    scale_color_brewer(type="qualitative", palette="Set1") +
    labs(title="Trip distance and duration within each borough",
         x="Distance (km)", y="Duration (m)", color="Borough") +
    theme_linedraw() +
    theme(figure_size=(8, 6))
)
```

![图15-14. 各区内骑行的距离和持续时间](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728839342657-397fb99a-2d7c-4adc-94b2-3b6c79fd9858.png)

<h4 id="OUzXd"></h4>
通过否定该过滤器，我们可以研究跨区骑行的距离和持续时间之间的关系。由于我们有四个不同的起始区，因此创建四个散点图是有意义的。我们使用`facet_wrap()`函数来创建四个面板，每个区一个。

```python
(
    ggplot(data=trips_speed
           .filter(pl.col("borough_start") != pl.col("borough_end"))
           .with_columns(
               ("From " + pl.col("borough_start")).alias("borough_start")),
           mapping=aes(x="distance", y="duration", color="borough_end")) +
    geom_point(size=0.25, alpha=0.5) +
    geom_smooth(method="lowess", size=2, se=False, alpha=0.8) +
    scale_color_brewer(type="qualitative", palette="Set1") +
    facet_wrap("borough_start") +
    labs(title="Trip distance and duration cross borough",
         x="Distance (km)", y="Duration (m)", color="To Borough") +
    theme_linedraw() +
    theme(figure_size=(8, 6))
)
```

![图15-15. 跨区骑行的距离和持续时间](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728839377122-49e8ef17-5a51-43ff-abae-ed78895a0e55.png)

<h4 id="deUwT"></h4>
如果你比较上面两个代码片段，你会发现它们共享了大量代码。我们只更改了`ggplot()`函数的数据参数，在第二个代码片段中添加了`facet_wrap`以创建四个面板，并更新了一些标签。这得益于Plotnine的可组合API。它通过链式调用方法来创建图表，而不是向一个方法添加关键字参数。

有关Plotnine的更多信息，请参考其[网站](https://plotnine.org/)或[Jeroen](https://jeroenjanssens.com/plotnine/)的博客文章。



<h3 id="oIfui">Great Tables</h3>
到目前为止，我们已经从DataFrame走向了数据可视化，将原始数字转变为色彩斑斓的像素。然而，还有一种介于这两者之间的第三种方式——表格。Rich Iannone 和 Michael Chow 开发的 Great Tables 包能够帮助你创建优秀的表格。

当表格能够以清晰和结构化的方式呈现信息时，它就是一个出色的表格。这可能包括：

+ 可读的列名
+ 具有适当格式的数值
+ 行分组
+ 用样式突出显示重要值
+ 标题、标签和脚注等注释

Great Tables 的核心理念基于一组紧密结合的表格组件（见图15-16）。从一个DataFrame开始，你可以通过迭代链式调用方法来添加元素和应用格式。

![图15-16. Great Table的组件（经Great Tables作者许可重现）](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728839548818-203b1468-f94a-4278-8a24-37644c73021c.png)



Great Tables 包可以通过PyPI安装：

```bash
$ pip install great_tables
```

让我们为一个精美的表格准备一个DataFrame。下面的代码片段计算了每个区最繁忙的三个车站：

```python
busiest_stations = (
    trips
    .group_by(   ①
                 station=pl.col("station_start"),
                 date=pl.col("datetime_start").dt.date()
             )
    .agg(
        borough=pl.col("borough_start").first(),
        neighborhood=pl.col("neighborhood_start").first(),
        num_rides=pl.len(),
        percent_member=(pl.col("rider_type") == "member").mean(),
        percent_electric=(pl.col("bike_type") == "electric").mean()
    )
    .sort("date")
    .group_by("station")
    .agg(
        pl.col(pl.String).first(),
        pl.col(pl.NUMERIC_DTYPES).mean(),
        pl.col("num_rides").alias("rides_per_day")  ②
    )
    .sort("num_rides", descending=True)
    .group_by("borough", maintain_order=True).head(3)
)
busiest_stations
```

```plain
shape: (12, 7)
┌───────────┬───────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ borough   │ station   │ neighbor │ num_ride │ percent_ │ percent_ │ rides_pe │
│ ---       │ ---       │ hood     │ s        │ member   │ electric │ r_day    │
│ str       │ str       │ ---      │ ---      │ ---      │ ---      │ ---      │
│           │           │ str      │ f64      │ f64      │ f64      │ list[u32 │
│           │           │          │          │          │          │ ]        │
╞═══════════╪═══════════╪══════════╪══════════╪══════════╪══════════╪══════════╡
│ Manhattan │ W 21 St & │ Chelsea  │ 354.2258 │ 0.913584 │ 0.583709 │ [325,    │
│           │ 6 Ave     │          │ 06       │          │          │ 88, …    │
│           │           │          │          │          │          │ 308]     │
│ …         │ …         │ …        │ …        │ …        │ …        │ …        │
│ Bronx     │ Plaza Dr  │ Mount    │ 31.70967 │ 0.837427 │ 0.948925 │ [30, 19, │
│           │ & W 170   │ Eden     │ 7        │          │          │ … 33]    │
│           │ St        │          │          │          │          │          │
└───────────┴───────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
```

① 首次聚合是必要的，因为我们希望使用nanoplot展示每天每个车站的出行次数（稍后会介绍它们）。

② 这一列的值将帮助我们在创建nanoplot时发挥作用。



以下代码使用Great Tables生成一个表格：

```python
import polars.selectors as cs
from great_tables import GT, style, md

(
    GT(busiest_stations, rowname_col="station", groupname_col="borough") ①
    .cols_label(       ②
        neighborhood="Neighborhood",
        num_rides="Mean Daily Rides",
        percent_member="Members",
        percent_electric="E-Bikes",
        rides_per_day="Rides Per Day",
    )
    .tab_header(
        title="Busiest Bike Stations in NYC",
        subtitle="In March 2024, Per Borough"
    )
    .tab_stubhead(label="Station")
    .fmt_number(columns="num_rides", decimals=1)
    .fmt_percent(columns=cs.starts_with("percent_"), decimals=0)  ③
    .fmt_nanoplot(columns="rides_per_day", reference_line="mean")
    .data_color(columns="num_rides", palette="Blues")
    .tab_options(row_group_font_weight="bold")
    .tab_source_note(source_note=md(
        "Source: [NYC Citi Bike](https://citibikenyc.com/system-data)"
    ))
)
```

① 车站按区分组以增加结构化。

② 我们可以为表格列赋予适当的名称，而不需要更改底层DataFrame。

③Great Tables接受列选择器，使我们的代码更紧凑。



![图15-17. 显示每个区三个最繁忙车站信息的表格](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728839670433-821f05ac-ae12-4775-bdf2-e8da0ea74833.png)

输出表格如图15-17所示。最右侧列中的折线图称为nanoplot，它们可视化了每个车站每日的骑行次数。

拥有这些方法作为构建块的一个好处是，你可以快速创建一个初始表格，然后逐步对其进行改进。

<h2 id="qFUk4">本章小结</h2>
在本章中，我们探讨了将DataFrame转换为图表和表格的多种方式。关键要点如下：

+ 数据可视化包有很多
+ Polars 内置了基于 hvPlot 的绘图功能
+ hvPlot 使用 Bokeh、Matplotlib 或 Plotly 来生成图表
+ hvPlot 可以将多个图表并排或叠加展示
+ hvPlot 可以创建交互式小部件，以便选择并绘制数据的子集
+ hvPlot 可以生成地理可视化
+ 如果某个数据可视化包尚未完全支持Polars，你可以随时使用Pandas
+ 如果你喜欢图形语法，Plotnine 是一个很棒的数据可视化包
+ 表格可以成为图表的有价值的替代方案，Great Tables 帮助你生成看起来很棒的表格

在下一章中，我们将探讨如何扩展Polars。

