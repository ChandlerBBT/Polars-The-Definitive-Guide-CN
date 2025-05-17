

在大多数分析中，你最终都会希望汇总或聚合数据，以回答某个问题或获得见解。除了 `sum`、`mean`、`min` 和 `max` 这样的基本聚合函数，它们可以帮助你了解整个数据集的情况，Polars 还提供了许多函数，用于分析数据的子集。这些函数是 `group_by` 上下文的一部分，它允许你基于一个或多个列或表达式对数据进行分组，然后对每个组分别应用特定的计算或转换。这是一种强大的方式，可以帮助你分析大型数据集并获得有价值的见解。

本章将教你关于 `group_by` 上下文及其可用方法的知识，并向你展示如何使用它们来分析数据。我们还会介绍如何使用 `group_by_dynamic`、`rolling` 和 `over` 基于时间值对数据进行分组。此外，我们还会展示一些可用于提高性能的优化方法。

<h2 id="HKe6L">根据上下文聚合</h2>
可以将 Polars 中的“group by”操作看作是在一个大型派对上要求客人根据共同点（例如他们的出生月份）分组。每个组（或“group by”类别）代表一个月份，所有出生在该月的客人都加入该组。一旦每个人都分好了组，你可以做一些事情，比如统计每组中有多少客人，找出每组中最高的人，或者计算每组客人的平均身高。这类似于 Polars 中的“group by”操作：它基于一个或多个列或表达式对数据进行分组，然后允许你对每个组分别应用特定的计算或转换。你可以使用 Polars 的 `group_by` 函数将数据组织成这些组。

在对数据进行分组后，你可以使用聚合函数来汇总每个组内的数据并获得有价值的见解。例如，你可以通过按客户 ID 分组来计算每个客户类别的平均购买金额。类似地，你可以通过按位置分组来找到每个位置的传感器读数总数。`group_by` 函数在分析大型数据集时是一个游戏规则的改变者，它允许你：

+ 基于特定属性组织和分类数据。
+ 应用聚合函数，从每个组中提取有意义的见解。
+ 通过关注特定方面，深入理解你的数据。

**表 12-1** 列出了你可以直接用于分析数据的方法。



_表 12-1：Group by 上下文中的可用方法_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">pl.GroupBy.__iter__()</font>**` | 允许迭代 `group by` 操作中的分组。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.agg(…)</font>**` | 计算 `group by` 操作中每个组的聚合。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.all()</font>**` | 将各组聚合成 `Series`。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.apply(…)</font>**` | 对各组应用自定义/用户定义函数（UDF）作为子 DataFrame。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.first()</font>**` | 聚合组中的第一个值。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.head(…)</font>**` | 获取每个组的前 `n` 行。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.last()</font>**` | 聚合组中的最后一个值。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.len()</font>**` | 返回每个组的行数。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.map_groups(…)</font>**` | 对各组应用自定义/用户定义函数（UDF）作为子 DataFrame。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.max()</font>**` | 将各组简化为最大值。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.mean()</font>**` | 将各组简化为平均值。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.median()</font>**` | 返回每组的中位数。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.min()</font>**` | 将各组简化为最小值。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.n_unique()</font>**` | 统计每组中的唯一值数量。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.quantile(…)</font>**` | 计算每组的分位数。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.sum()</font>**` | 将各组简化为总和。 |
| `**<font style="color:#01B2BC;">pl.GroupBy.tail(…)</font>**` | 获取每个组的最后 `n` 行。 |




为了展示 `group_by` 函数的用法，让我们从加载 2023 年荷兰的 Top2000 热门歌曲榜单数据集开始。该数据集包含荷兰人在 2023 年选出的所有时间最受欢迎的前 2000 首歌曲的信息，包括它们在 Top2000 中的排名、艺术家、歌曲名称和发行年份。

```python
import polars as pl

top2000 = pl.read_excel(
    "data/top2000-2023.xlsx",
    read_options={"skip_rows": 1},
    engine="calamine"
).set_sorted("positie")
```

---

`set_sorted` 启用快速路径算法

在这里我们使用 `set_sorted` 告诉 Polars 列 `"positie"` 已经排序。这是我们知道的信息，但 Polars 并不知情。通过告知 Polars 它可以利用一些仅在数据已排序时可用的快速路径优化。快速路径优化是一种通过利用关于数据的特殊知识来加速程序运行的方法。

---

你可以通过将分组转换为列表来显示 `group_by` 聚合函数应用到的值，你可以运行以下代码来实现：

```python
(
    top2000
    .group_by("jaar")
    .agg(  ①
        (
            pl.concat_str(
                pl.col("artiest"),
                pl.lit(" - "),
                pl.col("titel")
            )  ②
        ).alias("songs"),
    )
    .sort("jaar", descending=True)
)
```

```plain
shape: (67, 2)
┌──────┬─────────────────────────────────────────────────────┐
│ jaar │ songs                                               │
│ ---  │ ---                                                 │
│ i64  │ list[str]                                           │
╞══════╪═════════════════════════════════════════════════════╡
│ 2022 │ ["Son Mieux - Multicolor", "Bankzitters - Je Blik … │
│ 2021 │ ["Goldband - Noodgeval", "Bankzitters - Stapelgek"… │
│ 2020 │ ["DI-RECT - Soldier On", "Miss Montreal - Door De … │
│ 2019 │ ["Danny Vera - Roller Coaster", "Floor Jansen & He… │
│ 2018 │ ["Lady Gaga & Bradley Cooper - Shallow", "White Li… │
│ …    │ …                                                   │
│ 1960 │ ["Etta James - At Last", "Shadows - Apache"]        │
│ 1959 │ ["Jacques Brel - Ne Me Quitte Pas", "Elvis Presley… │
│ 1958 │ ["Chuck Berry - Johnny B. Goode", "Ella Fitzgerald… │
│ 1957 │ ["Johnny Cash - I Walk The Line", "Elvis Presley -… │
│ 1956 │ ["Elvis Presley - Love Me Tender", "Elvis Presley … │
└──────┴─────────────────────────────────────────────────────┘
```

① `.agg()` 方法允许你列出想要应用于分组数据的聚合操作。我们将在介绍标准的 `group_by` 聚合后回到这一点。

② 你通过连接艺术家的字符串、一个分隔符横线和标题来创建歌曲标题列表。

<h2 id="Nr0w5">描述统计</h2>
<h3 id="DKkQz">`head()`方法</h3>
在以下示例中，你将使用 `head()` 方法获取最近三年发布的前 3 首歌曲。

```python
(
    top2000
    .group_by("jaar", maintain_order=True)  ①
    .head(3)  ②
    .sort("jaar", descending=True)
    .head(9)  ③
)
```

```plain
shape: (9, 4)
┌──────┬─────────┬───────────────────────┬─────────────────────┐
│ jaar │ positie │ titel                 │ artiest             │
│ ---  │ ---     │ ---                   │ ---                 │
│ i64  │ i64     │ str                   │ str                 │
╞══════╪═════════╪═══════════════════════╪═════════════════════╡
│ 2022 │ 179     │ Multicolor            │ Son Mieux           │
│ 2022 │ 370     │ Je Blik Richting Mij  │ Bankzitters         │
│ 2022 │ 395     │ L'enfer               │ Stromae             │
│ 2021 │ 55      │ Noodgeval             │ Goldband            │
│ 2021 │ 149     │ Stapelgek             │ Bankzitters         │
│ 2021 │ 210     │ Dat Heb Jij Gedaan    │ Meau                │
│ 2020 │ 19      │ Soldier On            │ DI-RECT             │
│ 2020 │ 38      │ Door De Wind          │ Miss Montreal       │
│ 2020 │ 77      │ Impossible (Orchestr… │ Nothing But Thieves │
└──────┴─────────┴───────────────────────┴─────────────────────┘
```

①Top2000 数据集按排名排序，由于你使用了这个排序顺序，你想要保持它。通过将 `maintain_order` 参数设置为 `True`，可以确保保留该顺序。如果设置为 `False`（默认值），由于分组的并行处理，可能会丢失此顺序。

② 你希望按发行年份获取前 3 首歌曲，因此使用 `head` 方法。`head(3)` 方法应用于 `group_by` 上下文，这意味着它将返回每个组的前三首歌曲。

③ 你再次使用 `head(9)` 方法，但这次是为了获取最近三年发布的前 3 首歌曲。



<h3 id="DoufM">`tail()`方法</h3>
同样地，你可以使用 `tail()` 方法获取最近三年发布的最低排名的 3 首歌曲。

```python
(
    top2000
    .group_by("jaar", maintain_order=True)
    .tail(3)
    .sort("jaar", descending=True)
    .head(9)
)
```

```plain
shape: (9, 4)
┌──────┬─────────┬─────────────────┬───────────────────────┐
│ jaar │ positie │ titel           │ artiest               │
│ ---  │ ---     │ ---             │ ---                   │
│ i64  │ i64     │ str             │ str                   │
╞══════╪═════════╪═════════════════╪═══════════════════════╡
│ 2022 │ 1391    │ De Diepte       │ S10                   │
│ 2022 │ 1688    │ Zeit            │ Rammstein             │
│ 2022 │ 1716    │ THE LONELIEST   │ Måneskin              │
│ 2021 │ 1865    │ Bon Gepakt      │ Donnie & Rene Froger  │
│ 2021 │ 1978    │ Hold On         │ Armin van Buuren ft.… │
│ 2021 │ 2000    │ Drivers License │ Olivia Rodrigo        │
│ 2020 │ 1824    │ Smoorverliefd   │ Snelle                │
│ 2020 │ 1879    │ The Business    │ Tiësto                │
│ 2020 │ 1902    │ Levitating      │ Dua Lipa ft. DaBaby   │
└──────┴─────────┴─────────────────┴───────────────────────┘
```

---

`**<font style="color:#01B2BC;">head</font>**`**<font style="color:#01B2BC;"> 和 </font>**`**<font style="color:#01B2BC;">tail</font>**`**<font style="color:#01B2BC;"> 的别名</font>**

`first()` 方法与 `head(1)` 方法相同，但更直观且易于阅读。`last()` 方法也与 `tail(1)` 方法相同。

---



<h3 id="k86VY">`len()`方法</h3>
现在，假设你想知道根据在 Top2000 中的歌曲数量，艺术家的前 10 名。你可以通过按艺术家分组，然后使用 `len()` 方法获取组的长度来实现这一目标。

```python
(
    top2000
    .group_by("artiest")
    .len()
    .sort("len", descending=True)
    .head(10)
)
```

```plain
shape: (10, 2)
┌────────────────────┬─────┐
│ artiest            │ len │
│ ---                │ --- │
│ str                │ u32 │
╞════════════════════╪═════╡
│ Queen              │ 34  │
│ The Beatles        │ 31  │
│ ABBA               │ 25  │
│ The Rolling Stones │ 22  │
│ Bruce Springsteen  │ 22  │
│ Michael Jackson    │ 20  │
│ Fleetwood Mac      │ 20  │
│ Coldplay           │ 20  │
│ David Bowie        │ 18  │
│ U2                 │ 18  │
└────────────────────┴─────┘
```

看来荷兰人真的很喜欢皇后乐队、披头士和 ABBA！

接下来的方法更适合用另一个数据集来解释。该数据集包含销售数据，可以展示各种分析。

```python
df = pl.read_csv("data/sales_data.csv")
df.columns
```

```plain
['Date',
 'Age_Group',
 'Country',
 'Product_Category',
 'Sub_Category',
 'Product',
 'Order_Quantity',
 'Unit_Cost',
 'Unit_Price',
 'Profit',
 'Cost',
 'Revenue']
```



<h3 id="BhcGe">`min()`和 `max()`方法</h3>
让我们从演示 `min` 和 `max` 方法开始。假设你想知道最昂贵的类别和子类别。你可以通过按产品类别和子类别分组，然后使用 `max()` 方法获取最大单价来实现这一目标。

```python
(
    df
    .select("Product_Category", "Sub_Category", "Unit_Price")  ①
    .group_by("Product_Category", "Sub_Category")  ②
    .max()
    .sort("Unit_Price", descending=True)  ③
    .head(10)
)
```

```plain
shape: (10, 3)
┌──────────────────┬─────────────────┬────────────┐
│ Product_Category │ Sub_Category    │ Unit_Price │
│ ---              │ ---             │ ---        │
│ str              │ str             │ i64        │
╞══════════════════╪═════════════════╪════════════╡
│ Bikes            │ Road Bikes      │ 3578       │
│ Bikes            │ Mountain Bikes  │ 3400       │
│ Clothing         │ Vests           │ 2384       │
│ Bikes            │ Touring Bikes   │ 2384       │
│ Accessories      │ Bike Stands     │ 159        │
│ Accessories      │ Bike Racks      │ 120        │
│ Clothing         │ Socks           │ 70         │
│ Clothing         │ Shorts          │ 70         │
│ Accessories      │ Hydration Packs │ 55         │
│ Clothing         │ Jerseys         │ 54         │
└──────────────────┴─────────────────┴────────────┘
```

① 你选择了相关列，以便专注于所需的数据。

② 你按产品类别和子类别对数据进行分组。与之前的示例不同，你按两列分组。这意味着 `max()` 方法将返回每个产品类别和子类别组合的最高单价。

③ 你按单价降序对数据进行排序，并获取最昂贵的前 10 个子类别。



<h3 id="KEGjR">`sum()`方法</h3>
现在假设你想知道每个国家的总利润。你可以通过按国家分组，然后使用 `sum()` 方法获取利润总和来实现这一目标。

```python
(
    df
    .select("Country", "Profit")
    .group_by("Country")
    .sum()
    .sort("Profit", descending=True)
)
```

```plain
shape: (6, 2)
┌────────────────┬──────────┐
│ Country        │ Profit   │
│ ---            │ ---      │
│ str            │ i64      │
╞════════════════╪══════════╡
│ United States  │ 11073644 │
│ Australia      │ 6776030  │
│ United Kingdom │ 4413853  │
│ Canada         │ 3717296  │
│ Germany        │ 3359995  │
│ France         │ 2880282  │
└────────────────┴──────────┘
```



<h3 id="dzyJx">`n_unique()`方法</h3>
那子类别中拥有最多唯一产品的子类别呢？你可以通过按子类别分组，然后使用 `n_unique()` 方法获取唯一产品的数量来实现这一目标。

```python
(
    df
    .select("Sub_Category", "Product")
    .group_by("Sub_Category")
    .n_unique()
    .sort("Product", descending=True)
    .head(10)
)
```

```plain
shape: (10, 2)
┌───────────────────┬─────────┐
│ Sub_Category      │ Product │
│ ---               │ ---     │
│ str               │ u32     │
╞═══════════════════╪═════════╡
│ Road Bikes        │ 38      │
│ Mountain Bikes    │ 28      │
│ Touring Bikes     │ 22      │
│ Tires and Tubes   │ 11      │
│ Jerseys           │ 8       │
│ Vests             │ 4       │
│ Gloves            │ 4       │
│ Socks             │ 3       │
│ Bottles and Cages │ 3       │
│ Helmets           │ 3       │
└───────────────────┴─────────┘
```



<h3 id="t97ox">`mean()`方法</h3>
假设你想知道每个年龄组的平均订单数量。你可以通过按年龄组分组，然后使用 `mean()` 方法获取订单数量的平均值来实现这一目标。

```python
(
    df
    .select("Age_Group", "Order_Quantity")
    .group_by("Age_Group")
    .mean()
    .sort("Order_Quantity", descending=True)
)
```

```plain
shape: (4, 2)
┌──────────────────────┬────────────────┐
│ Age_Group            │ Order_Quantity │
│ ---                  │ ---            │
│ str                  │ f64            │
╞══════════════════════╪════════════════╡
│ Seniors (64+)        │ 13.530137      │
│ Youth (<25)          │ 12.124018      │
│ Adults (35-64)       │ 12.045303      │
│ Young Adults (25-34) │ 11.560899      │
└──────────────────────┴────────────────┘
```



<h3 id="V5jaX">`quantile()`方法</h3>
此外，你可以使用 `quantile` 方法获取每个年龄组订单数量的 25%、50% 和 75% 分位数。

```python
(
    df
    .select("Age_Group", "Revenue")
    .group_by("Age_Group")
    .quantile(0.9)
    .sort("Revenue", descending=True)
)
```

```plain
shape: (4, 2)
┌──────────────────────┬─────────┐
│ Age_Group            │ Revenue │
│ ---                  │ ---     │
│ str                  │ f64     │
╞══════════════════════╪═════════╡
│ Young Adults (25-34) │ 2227.0  │
│ Adults (35-64)       │ 2217.0  │
│ Youth (<25)          │ 1997.0  │
│ Seniors (64+)        │ 943.0   │
└──────────────────────┴─────────┘
```



看起来“都市年轻专业人士”又一次站上了风口！在荷兰，"Yuppies"（都市年轻专业人士）通常与高收入和高消费联系在一起。看起来这些 Yuppies 在订单上的花费很多，他们的订单数量在第 90 个分位数竟然达到了 2,227！

另一个方法是 `median` 方法，它是 `quantile(0.5)` 的别名。

现在我们已经看到了 `group_by` 上下文中可用的基本聚合操作，是时候进入一些高级的操作了。



<h2 id="EPjBI">高级聚合</h2>
到目前为止我们讨论的所有方法都非常适合进行简单的聚合操作。然而，有时你可能希望进行更复杂的聚合操作、同时执行多个聚合操作，甚至应用你自己的自定义聚合函数。这时，多功能的 `agg` 方法就派上用场了。

`agg` 方法允许你：

+ 将列元素聚合成每组的一个列表。
+ 控制生成列的名称。
+ 使用表达式同时对多个列应用多个聚合函数。
+ 应用你自己的自定义聚合函数。

让我们逐一进行解释。



首先，`agg` 允许你将列元素按组聚合成列表。这是通过将列选择器传递给 `agg` 方法来实现的。让我们通过按国家聚合利润和收入来看看如何做到这一点：

```python
(
    df
    .select("Country", "Profit", "Revenue")
    .group_by("Country")
    .agg(
        pl.col("Profit"),
        pl.col("Revenue"),
    )
)
```

```plain
shape: (6, 3)
┌────────────────┬─────────────────────┬──────────────────────┐
│ Country        │ Profit              │ Revenue              │
│ ---            │ ---                 │ ---                  │
│ str            │ list[i64]           │ list[i64]            │
╞════════════════╪═════════════════════╪══════════════════════╡
│ Canada         │ [590, 590, … 630]   │ [950, 950, … 1014]   │
│ Germany        │ [160, 53, … 746]    │ [295, 98, … 1250]    │
│ United Kingdom │ [1053, 1053, … 112] │ [1728, 1728, … 184]  │
│ Australia      │ [1366, 1188, … 655] │ [2401, 2088, … 1183] │
│ United States  │ [524, 407, … 542]   │ [929, 722, … 878]    │
│ France         │ [427, 427, … 655]   │ [787, 787, … 1207]   │
└────────────────┴─────────────────────┴──────────────────────┘
```

像这样收集结果是聚合过程的第一步，通常会接着对这些值应用一个函数。



`agg` 方法的第二个功能是为生成的列命名。这可以通过对聚合表达式使用 `alias` 方法或 `name` 命名空间来实现。有关 `name` 命名空间的更多信息，你可以参考第 6 章的表 6-2。

```python
(
    df
    .select("Country", "Profit", "Revenue")
    .group_by("Country")
    .agg(
        pl.col("Profit").alias("All Profits Per Transactions"),
        pl.col("Revenue").name.prefix("All "),
    )
)
```

```plain
shape: (6, 3)
┌────────────────┬───────────────────────┬──────────────────────┐
│ Country        │ All Profits Per Tran… │ All Revenue          │
│ ---            │ ---                   │ ---                  │
│ str            │ list[i64]             │ list[i64]            │
╞════════════════╪═══════════════════════╪══════════════════════╡
│ United States  │ [524, 407, … 542]     │ [929, 722, … 878]    │
│ Germany        │ [160, 53, … 746]      │ [295, 98, … 1250]    │
│ Canada         │ [590, 590, … 630]     │ [950, 950, … 1014]   │
│ United Kingdom │ [1053, 1053, … 112]   │ [1728, 1728, … 184]  │
│ France         │ [427, 427, … 655]     │ [787, 787, … 1207]   │
│ Australia      │ [1366, 1188, … 655]   │ [2401, 2088, … 1183] │
└────────────────┴───────────────────────┴──────────────────────┘
```



第三，`agg` 允许你同时对多个列应用多个聚合函数。你可以通过将表达式列表传递给 `agg` 方法来实现这一点。

```python
(
    df
    .select("Country", "Profit", "Revenue")
    .group_by("Country")
    .agg(
        pl.col("Profit").sum().alias("Total Profit"),
        pl.col("Profit").mean().alias("Average Profit per Transaction"),
        pl.col("Revenue").sum().alias("Total Revenue"),
        pl.col("Revenue").mean().alias("Average Revenue per Transaction"),
    )
)
```

```plain
shape: (6, 5)
┌───────────────┬──────────────┬───────────────┬───────────────┬───────────────┐
│ Country       │ Total Profit │ Average       │ Total Revenue │ Average       │
│ ---           │ ---          │ Profit per T… │ ---           │ Revenue per … │
│ str           │ i64          │ ---           │ i64           │ ---           │
│               │              │ f64           │               │ f64           │
╞═══════════════╪══════════════╪═══════════════╪═══════════════╪═══════════════╡
│ Germany       │ 3359995      │ 302.756803    │ 8978596       │ 809.028293    │
│ Canada        │ 3717296      │ 262.187615    │ 7935738       │ 559.721964    │
│ United States │ 11073644     │ 282.447687    │ 27975547      │ 713.552696    │
│ United        │ 4413853      │ 324.071439    │ 10646196      │ 781.659031    │
│ Kingdom       │              │               │               │               │
│ Australia     │ 6776030      │ 283.089489    │ 21302059      │ 889.959016    │
│ France        │ 2880282      │ 261.891435    │ 8432872       │ 766.764139    │
└───────────────┴──────────────┴───────────────┴───────────────┴───────────────┘
```



或者，你也可以将列选择器与这些表达式结合起来，在单个表达式中同时对多个列应用聚合函数。

```python
(
    df
    .select("Country", "Profit", "Revenue")
    .group_by("Country")
    .agg(
        pl.all().sum().name.prefix("Total "),
        pl.all().mean().name.prefix("Average "),
    )
)
```

```plain
shape: (6, 5)
┌────────────────┬──────────────┬───────────────┬────────────┬────────────┐
│ Country        │ Total Profit │ Total Revenue │ Average    │ Average    │
│ ---            │ ---          │ ---           │ Profit     │ Revenue    │
│ str            │ i64          │ i64           │ ---        │ ---        │
│                │              │               │ f64        │ f64        │
╞════════════════╪══════════════╪═══════════════╪════════════╪════════════╡
│ Australia      │ 6776030      │ 21302059      │ 283.089489 │ 889.959016 │
│ France         │ 2880282      │ 8432872       │ 261.891435 │ 766.764139 │
│ United Kingdom │ 4413853      │ 10646196      │ 324.071439 │ 781.659031 │
│ Germany        │ 3359995      │ 8978596       │ 302.756803 │ 809.028293 │
│ Canada         │ 3717296      │ 7935738       │ 262.187615 │ 559.721964 │
│ United States  │ 11073644     │ 27975547      │ 282.447687 │ 713.552696 │
└────────────────┴──────────────┴───────────────┴────────────┴────────────┘
```



由于你使用的是表达式，因此甚至可以处理条件。条件返回一个布尔掩码，而 `sum` 方法将对 `True` 值进行求和。布尔掩码是一种类似数组的结构，包含布尔值，用于根据特定条件过滤行或列。

例如，我们可以通过下面的示例找出按国家分组的高利润交易数。为了展示表达式的布尔掩码是什么样子的，你可以只使用表达式进行聚合。这会创建一个布尔值列表。由于这是一个包含 0 和 1 的列表，你可以对它们求和以获取 `True` 值的数量！

```python
(
    df
    .select("Country", "Profit")
    .group_by("Country")
    .agg(
        (pl.col("Profit") > 1000)
        .alias("Profit > 1000"),
        (pl.col("Profit") > 1000)
        .sum()
        .alias("Number of Transactions with Profit > 1000"),
    )
)
```

```plain
shape: (6, 3)
┌────────────────┬───────────────────────┬───────────────────────┐
│ Country        │ Profit > 1000         │ Number of Transactio… │
│ ---            │ ---                   │ ---                   │
│ str            │ list[bool]            │ u32                   │
╞════════════════╪═══════════════════════╪═══════════════════════╡
│ Canada         │ [false, false, … fal… │ 868                   │
│ Australia      │ [true, true, … false… │ 1233                  │
│ United States  │ [false, false, … fal… │ 2623                  │
│ Germany        │ [false, false, … fal… │ 659                   │
│ United Kingdom │ [true, true, … false… │ 788                   │
│ France         │ [false, false, … fal… │ 482                   │
└────────────────┴───────────────────────┴───────────────────────┘
```



由于你可以在 `agg` 函数中使用表达式，因此你还可以插入返回表达式的 Python 函数。虽然通常只有在绝对必要时才应该将 Polars 和 Python 结合使用，但这是一个例外。这是因为 Python 函数在 Polars 之前运行，并返回一个 Polars 表达式，Polars 随后在 Rust 中运行该表达式。

```python
def custom_agg(column: str) -> pl.Expr:
    return (column > 1000).alias("Profit > 1000"), (column > 1000).sum().alias("Number of Transactions with Profit > 1000")

(
    df
    .select("Country", "Profit")
    .group_by("Country")
    .agg(
        custom_agg(pl.col("Profit"))
    )
)
```

```plain
shape: (6, 3)
┌────────────────┬───────────────────────┬───────────────────────┐
│ Country        │ Profit > 1000         │ Number of Transactio… │
│ ---            │ ---                   │ ---                   │
│ str            │ list[bool]            │ u32                   │
╞════════════════╪═══════════════════════╪═══════════════════════╡
│ Germany        │ [false, false, … fal… │ 659                   │
│ United Kingdom │ [true, true, … false… │ 788                   │
│ France         │ [false, false, … fal… │ 482                   │
│ United States  │ [false, false, … fal… │ 2623                  │
│ Canada         │ [false, false, … fal… │ 868                   │
│ Australia      │ [true, true, … false… │ 1233                  │
└────────────────┴───────────────────────┴───────────────────────┘
```



允许插入像这样的表达式展示了 `agg` 方法的灵活性。然而，如果你想对数据应用 Python 函数该怎么办？

<h2 id="fSxZW">用户定义的函数（UDF）</h2>
Polars 提供了一套广泛的表达式，允许你执行各种操作。然而，有时你需要执行某些可用表达式没有涵盖的操作，或者需要使用外部包来执行操作。为此，Polars 允许使用用户定义的函数（UDFs）。Polars 提供以下函数让你可以实现这一点：

+ `map_elements`：对列的每个元素应用一个 Python 函数。
+ `map_batches`：对 `Series` 或 `Series` 序列应用一个 Python 函数。
+ `map_groups`：在 `GroupBy` 上下文中对每个组应用一个 Python 函数。

让我们深入了解如何使用这些函数对数据应用你自己的自定义 Python 函数。



<h3 id="EgLk8">`map_elements`函数</h3>
`map_elements` 函数允许你在不需要了解列中其他元素的情况下，对列中的每个元素应用一个 Python 函数。

以下示例展示了对包含评论的 DataFrame 文本进行情感分析：

```python
from textblob import TextBlob

def analyze_sentiment(review):
    return TextBlob(review).sentiment.polarity

df = pl.DataFrame({
    "reviews": [
        "This product is great!",
        "Terrible service.",
        "Okay, but not what I expected.",
        "Excellent! I love it."
    ]
})

df = df.with_columns(
    pl.col("reviews")
    .map_elements(
        analyze_sentiment,
        return_dtype=pl.Float64
    )
    .alias("sentiment_score")
)
df
```

```plain
shape: (4, 2)
┌────────────────────────────────┬─────────────────┐
│ reviews                        │ sentiment_score │
│ ---                            │ ---             │
│ str                            │ f64             │
╞════════════════════════════════╪═════════════════╡
│ This product is great!         │ 1.0             │
│ Terrible service.              │ -1.0            │
│ Okay, but not what I expected. │ 0.2             │
│ Excellent! I love it.          │ 0.75            │
└────────────────────────────────┴─────────────────┘
```



在这个示例中，我们使用 `map_elements` 函数将 `analyze_sentiment` 函数应用于“reviews”列中的每个元素。结果值的范围从 -1.0（非常负面）到 1.0（非常正面），0.0 表示中性。



---

**<font style="color:#C99103;">对低效映射的警告</font>**

当你在 Polars 中使用 Python 函数时，重要的是要知道它的速度不会像原生 Polars 函数那么快。Polars 通常在 Rust 中运行其操作，但当它必须应用自定义 Python 函数时，会发生以下几件事情：

+ 该函数执行较慢的 Python 字节码，而不是更快的 Rust 字节码。
+ Python 函数受限于全局解释器锁（GIL），这意味着它不能并行运行。这在 `group_by` 操作中特别影响速度，因为通常会对每个组并行调用聚合函数。

将 Python 的 lambda 函数或自定义函数映射到 Polars 数据时应作为最后的手段。当 Polars 抛出 `PolarsInefficientMapWarning` 时，这表明很可能存在使用原生 Polars 表达式的替代方式。只有当你查阅了 Polars 文档并发现没有原生表达式或表达式组合能够实现你想要的功能时，才应该考虑使用 Python 函数。

在以下示例中，你会看到通过将一个简单的函数映射到列时出现的 `PolarsInefficientMapWarning`。

```python
df = pl.DataFrame({
    "x": [1,2,3,4]
})

def add_one(x):
    return x + 1

df.with_columns(
    pl.col('x')
    .map_elements(
        add_one,
        return_dtype=pl.Int64,
    )
    .alias("x + 1")
)
```

```plain
PolarsInefficientMapWarning:
Expr.map_elements is significantly slower than the native expressions API.
Only use if you absolutely CANNOT implement your logic otherwise.
Replace this expression...
  - pl.col("x").map_elements(add_one)
with this one instead:
  + pl.col("x") + 1
shape: (4, 2)
┌─────┬───────┐
│ x   │ x + 1 │
│ --- │ ---   │
│ i64 │ i64   │
╞═════╪═══════╡
│ 1   │ 2     │
│ 2   │ 3     │
│ 3   │ 4     │
│ 4   │ 5     │
└─────┴───────┘
```

这些建议通常是寻找更高效实现的良好起点！



---

`@lru_cache`

Python 的 `functools` 模块中的 `@lru_cache` 装饰器是优化计算密集型函数的一个有用工具。通过缓存函数调用的结果，它可以显著减少执行时间，尤其是在函数多次使用相同参数调用时。这在你将函数映射到包含重复值的 DataFrame 列时特别有用。`@lru_cache` 会存储你的函数调用结果。当函数再次用相同参数调用时，它将从缓存中检索结果，而不是重新计算。

你可以为 `@lru_cache` 装饰器指定 `maxsize` 参数，决定缓存结果的数量。默认情况下，这设置为 128 个缓存条目，但你可以根据数据量将其设置得更高，以防止缓存未命中。`@lru_cache` 会在缓存填满时丢弃最少使用的条目。如果你想存储所有结果而不介意高内存使用，可以将 `maxsize` 设置为 `None`。当缓存不再需要时，可以使用 `cache_clear()` 方法清除缓存。让我们将其应用到先前使用 `map_elements` 进行余弦相似度计算的示例中：

```python
from functools import lru_cache


df = pl.DataFrame({
    "x": [1,1,3,3]
})

@lru_cache(maxsize=None)
def add_one(x):
    return x + 1

df.with_columns(
    pl.col('x')
    .map_elements(
        add_one,
        return_dtype=pl.Int64,
    )
    .alias("x + 1")
)
```

```plain
shape: (4, 2)
┌─────┬───────┐
│ x   │ x + 1 │
│ --- │ ---   │
│ i64 │ i64   │
╞═════╪═══════╡
│ 1   │ 2     │
│ 1   │ 2     │
│ 3   │ 4     │
│ 3   │ 4     │
└─────┴───────┘
```

---

<h3 id="m0ura">`map_batches` 函数</h3>
`map_batches` 函数允许你将 Python 函数应用于 `Series` 或 `Series` 序列。这在你需要了解列中其他元素的信息，或需要同时对多个列应用函数时非常有用。`map_batches` 具有以下参数：

+ `function`：要应用于 `Series` 的函数。
+ `return_dtype`：该函数返回的 `Series` 的数据类型。
+ `is_elementwise`：该函数是否逐元素操作。如果是，它可以在流引擎中运行，但可能返回不正确的 `group_by` 结果。
+ `agg_list`：在 `group_by` 上下文中，在应用函数之前将表达式的值聚合为列表。这样函数只对组的列表调用一次，而不是对每个组调用一次。

在以下示例中，我们将通过对“feature1”和“feature2”列应用 `softmax` 归一化函数来演示 `map_batches` 函数。`softmax` 归一化函数将一组数字转换为加起来为 100% 的概率。

```python
import polars.selectors as cs
import numpy as np
from scipy.special import softmax

df = pl.DataFrame({
    "feature1": [0.3, 0.2, 0.4, 0.1, 0.2, 0.3, 0.5],
    "feature2": [32, 50, 70, 65, 0, 10, 15],
    "label": [1, 0, 1, 0, 1, 0, 0]
})

result = df.select(
    "label",
    cs.starts_with("feature").map_batches(
        lambda x: softmax(x.to_numpy()),
    )
)
result
```

```plain
shape: (7, 3)
┌───────┬──────────┬────────────┐
│ label │ feature1 │ feature2   │
│ ---   │ ---      │ ---        │
│ i64   │ f64      │ f64        │
╞═══════╪══════════╪════════════╡
│ 1     │ 0.143782 │ 3.1181e-17 │
│ 0     │ 0.130099 │ 2.0474e-9  │
│ 1     │ 0.158904 │ 0.993307   │
│ 0     │ 0.117719 │ 0.006693   │
│ 1     │ 0.130099 │ 3.9488e-31 │
│ 0     │ 0.143782 │ 8.6979e-27 │
│ 0     │ 0.175616 │ 1.2909e-24 │
└───────┴──────────┴────────────┘
```



<h3 id="BMdmX">`map_groups` 函数</h3>
`map_groups` 函数允许你在 `GroupBy` 上下文中对每个组应用 Python 函数。

假设你有一个包含不同地点测量温度的 DataFrame，其中美国地点的温度以华氏度表示，欧洲地点的温度以摄氏度表示。如果在你的分析中只关心温度的变化，那么你可以对每个组的特征进行缩放，以使它们具有可比性：

```python
from sklearn.preprocessing import StandardScaler

def scale_temperature(group):
    scaler = StandardScaler()
    scaled_values = scaler.fit_transform(group[['temperature']].to_numpy())
    return group.with_columns(pl.Series(values=scaled_values.flatten(), name="scaled_feature"))

df = pl.DataFrame({
    "group": ["USA", "USA", "USA", "USA", "NL", "NL", "NL"],
    "temperature": [32, 50, 70, 65, 0, 10, 15]
})

result = df.group_by("group").map_groups(scale_temperature)
result
```

```plain
shape: (7, 3)
┌───────┬─────────────┬────────────────┐
│ group │ temperature │ scaled_feature │
│ ---   │ ---         │ ---            │
│ str   │ i64         │ f64            │
╞═══════╪═════════════╪════════════════╡
│ USA   │ 32          │ -1.502872      │
│ USA   │ 50          │ -0.287066      │
│ USA   │ 70          │ 1.063831       │
│ USA   │ 65          │ 0.726107       │
│ NL    │ 0           │ -1.336306      │
│ NL    │ 10          │ 0.267261       │
│ NL    │ 15          │ 1.069045       │
└───────┴─────────────┴────────────────┘
```



最后，如果你需要对 `GroupBy` 上下文中的各个组进行精细控制，你也可以对它们进行迭代。这在你需要为每个组应用不同的自定义函数或想要单独检查每个组时非常有用。对组进行迭代会返回一个包含组标识符（如果只有一个标识符，则为单个标识符）和该组的 DataFrame 的元组。

```python
df = pl.DataFrame({
    "group": ["USA", "USA", "USA", "USA", "NL", "NL", "NL"],
    "temperature": [32, 50, 70, 65, 0, 10, 15]
})

for group in df.group_by(["group"]):
    print(group)
```

```plain
(('NL',), shape: (3, 2)
┌───────┬─────────────┐
│ group │ temperature │
│ ---   │ ---         │
│ str   │ i64         │
╞═══════╪═════════════╡
│ NL    │ 0           │
│ NL    │ 10          │
│ NL    │ 15          │
└───────┴─────────────┘)
(('USA',), shape: (4, 2)
┌───────┬─────────────┐
│ group │ temperature │
│ ---   │ ---         │
│ str   │ i64         │
╞═══════╪═════════════╡
│ USA   │ 32          │
│ USA   │ 50          │
│ USA   │ 70          │
│ USA   │ 65          │
└───────┴─────────────┘)
```



总之， Polars 提供了 `map_elements`、`map_batches` 和 `map_groups` 等函数，允许你将自定义 Python 函数应用于数据。虽然这些用户定义的函数提供了广泛的自定义选项，但与原生 Polars 表达式相比，它们在性能上存在缺陷。如果你仍然需要使用 Python 函数，但输入经常相同，那么 `@lru_cache` 装饰器可以帮助优化重复计算。通过理解并利用这些工具，你可以根据特定需求定制数据转换，同时保持最佳性能。



<h2 id="UB19N">行内聚合（Row-wise Aggregations）使用 `reduce` 和 `fold`</h2>
Polars 提供了很多现成的水平聚合表达式，见 **表 8-5**。在 Polars API 中，`reduce` 和 `fold` 方法允许你构建更复杂的水平聚合。这些方法同时在整个列上操作，通常以矢量化方式执行，保持了良好的性能。

以下是 `reduce` 和 `fold` 的工作原理：首先，它们会创建一个名为累加器的新列。这个累加器是一列新的初始值，用于应用聚合操作。另一个输入是从正在聚合的表达式中生成的值。累加器会随着一个函数的结果而更新，该函数的输入是累加器和这个值。

`reduce` 和 `fold` 都接受以下参数：

+ `**function**`：要应用于累加器和折叠值的函数。
+ `**exprs**`：用于聚合的表达式。

此外，`reduce` 使用遇到的第一个值作为累加器，而 `fold` 方法允许你通过以下参数设置累加器的初始值：

+ `**acc**`：累加器的初始值。

让我们通过一个简单的例子来理解 `fold` 是如何工作的：

```python
df = pl.DataFrame({
    "col1": [2],
    "col2": [3],
    "col3": [4]
})

df.with_columns(
    pl.fold(
        acc=pl.lit(0),  ①
        function=lambda acc, x: acc + x,  ②
        exprs=pl.col("*")  ③
    ).alias("sum")
)
```

```plain
shape: (1, 4)
┌──────┬──────┬──────┬─────┐
│ col1 │ col2 │ col3 │ sum │
│ ---  │ ---  │ ---  │ --- │
│ i64  │ i64  │ i64  │ i64 │
╞══════╪══════╪══════╪═════╡
│ 2    │ 3    │ 4    │ 9   │
└──────┴──────┴──────┴─────┘
```

① 因为你正在对列的值进行求和，所以你将累加器的初始值设置为 0。如果使用 `reduce` 方法，则累加器会设置为列中的第一个值。

② 这是一个简单的求和函数。累加器列中的值加上你正在聚合的下一个列的值。

③ 由于 `pl.col("*")` 作为通配符代表任何列，你正在对 DataFrame 中的所有列进行聚合而不进行任何修改。

执行过程如下：

![图12-1：fold函数的执行逻辑](https://cdn.nlark.com/yuque/0/2024/jpeg/1310472/1728832281721-40a25668-3f17-4446-aa14-1e3673a83d82.jpeg)

```python
df = pl.DataFrame({
    "col1": [2],
    "col2": [3],
    "col3": [4]
})

df.with_columns(
    pl.fold(
        acc=pl.lit(0),  ①
        function=lambda acc, x: acc + x,  ②
        exprs=pl.col("*")  ③
    ).alias("sum")
)
```

```plain
shape: (1, 4)
┌──────┬──────┬──────┬─────┐
│ col1 │ col2 │ col3 │ sum │
│ ---  │ ---  │ ---  │ --- │
│ i64  │ i64  │ i64  │ i64 │
╞══════╪══════╪══════╪═════╡
│ 2    │ 3    │ 4    │ 9   │
└──────┴──────┴──────┴─────┘
```

`fold` 的一个可能使用场景是当你想按列进行加权求和时。例如，你有一个包含不同产品销售数据的 DataFrame，并希望计算销售的加权总和：

```python
df = pl.DataFrame({
    "product_A": [10, 20, 30],
    "product_B": [20, 30, 40],
    "product_C": [30, 40, 50]
})


weights = {  ①
    "product_A": 0.5,
    "product_B": 1.5,
    "product_C": 2.0
}

weighted_exprs = [  ②
    (pl.col(product) * weight).alias(product)
    for product, weight in weights.items()
]

df_with_weighted_sum = df.with_columns(
    pl.fold(  ③
        acc=pl.lit(0),  ④
        function=lambda acc, x: acc + x,  ⑤
        exprs=weighted_exprs  ⑥
    ).alias("weighted_sum")
)

df_with_weighted_sum
```

```plain
shape: (3, 4)
┌───────────┬───────────┬───────────┬──────────────┐
│ product_A │ product_B │ product_C │ weighted_sum │
│ ---       │ ---       │ ---       │ ---          │
│ i64       │ i64       │ i64       │ f64          │
╞═══════════╪═══════════╪═══════════╪══════════════╡
│ 10        │ 20        │ 30        │ 95.0         │
│ 20        │ 30        │ 40        │ 135.0        │
│ 30        │ 40        │ 50        │ 175.0        │
└───────────┴───────────┴───────────┴──────────────┘
```

① 为每个产品定义权重。

② 创建一个 Polars 表达式，将每列乘以各自的权重。

③ 应用 `fold` 函数计算加权总和。

④ 将累加器的初始值设置为 0。

⑤ 再次使用求和函数。

⑥ 将加权表达式应用到 `fold` 函数中。



<h2 id="xHPKG">`over()` 表达式在选择上下文中的应用</h2>
有时，你并不想将数据聚合成组，而是希望将信息添加到 DataFrame 中。这时，`over()` 函数就派上用场了。`over()` 函数允许你在选择上下文中对分组进行聚合！此外，它允许你将结果映射回原始 DataFrame，保持其原始维度。当你需要保留单个行的上下文，并希望用来自组的信息来丰富它时，这非常实用。`over()` 函数有以下参数：

+ `**expr**` 和 `***more_exprs**`：用于分组的列。接受表达式和字符串，字符串将被解析为列名。
+ `**mapping_strategy**`：
    - `group_to_rows`（默认）：将结果映射回它们源自的行。结果的大小与原始 DataFrame 相同。
    - `join`：将结果聚合为一个列表，并将其连接回原始 DataFrame。
    - `explode`：为结果列表中的每个元素创建一个新行。这会改变 DataFrame 的大小。

让我们回到本章开头的 Top2000 数据集。如果你想向 DataFrame 中添加信息，而不是为分析聚合结果，你可以使用 `over()` 函数。例如，我们可以尝试计算某首歌曲在其发行年份中的排名。

```python
(top2000
 .select(
     "jaar",
     "artiest",
     "titel",
     "positie",
     pl.col("positie")
     .rank()
     .over("jaar")
     .alias("year_rank")
 )
 .sample(10, seed=42)
)
```

```plain
shape: (10, 5)
┌──────┬───────────────────────┬───────────────────────┬─────────┬───────────┐
│ jaar │ artiest               │ titel                 │ positie │ year_rank │
│ ---  │ ---                   │ ---                   │ ---     │ ---       │
│ i64  │ str                   │ str                   │ i64     │ f64       │
╞══════╪═══════════════════════╪═══════════════════════╪═════════╪═══════════╡
│ 2013 │ Stromae               │ Papaoutai             │ 318     │ 6.0       │
│ 1969 │ John Denver           │ Leaving On A Jet Pla… │ 607     │ 16.0      │
│ 1971 │ Led Zeppelin          │ Immigrant Song        │ 590     │ 19.0      │
│ 2009 │ Anouk                 │ For Bitter Or Worse   │ 1453    │ 23.0      │
│ 2015 │ Snollebollekes        │ Links Rechts          │ 1076    │ 14.0      │
│ 1984 │ Alphaville            │ Forever Young         │ 302     │ 11.0      │
│ 1977 │ ABBA                  │ Take A Chance On Me   │ 636     │ 23.0      │
│ 1975 │ Rod Stewart           │ Sailing               │ 918     │ 20.0      │
│ 1986 │ Metallica             │ Master Of Puppets     │ 29      │ 1.0       │
│ 2005 │ Alderliefste & Ramse… │ Laat Me/Vivre         │ 463     │ 5.0       │
└──────┴───────────────────────┴───────────────────────┴─────────┴───────────┘
```

我们可以看到，Stromae 的《Papaoutai》在 2013 年根据 Top2000 投票者的投票被评为第 6 名，但在总排名中则排在第 318 位。



<h2 id="uvghF">使用 `group_by_dynamic` 进行动态分组</h2>
当你处理时间数据时，基于时间窗口创建分组可能是实用的。这时，`group_by_dynamic` 函数派上用场。`group_by_dynamic` 计算一个固定大小和宽度的时间窗口，将 DataFrame 中的行分配到这些窗口中。与常规分组不同，某些行可以出现在多个时间窗口中，具体取决于窗口大小和时间列。这对于计算年度或季度销售数据非常有用，在这些情况下你希望将数据划分为特定的时间段。

这些时间窗口可以通过以下参数定义：

+ `**every**`：窗口开始的间隔。
+ `**period**`：时间窗口的长度。如果未指定，它将匹配 `every`，从而形成相邻但不重叠的组。然而，如果你想创建重叠的窗口，可以将 `period` 设置为比 `every` 更大的值。
+ `**offset**`：用于偏移窗口的开始。例如，如果你想在每天上午 9 点开始你的时间窗口，以对齐工作时间，你可以将 `every=1d` 和 `offset=9h` 设置。
+ `**start_by**`：设置确定第一个窗口开始的策略，允许你对齐到最早的数据点，特定的星期几，或根据最早的时间戳调整后再应用你指定的 `every` 间隔。

这些参数可以使用以下字符串指定：



_表 12-2：持续时间字符串及其含义_

| 持续时间字符串 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">1ns</font>**` | 1 纳秒 |
| `**<font style="color:#01B2BC;">1us</font>**` | 1 微秒 |
| `**<font style="color:#01B2BC;">1ms</font>**` | 1 毫秒 |
| `**<font style="color:#01B2BC;">1s</font>**` | 1 秒 |
| `**<font style="color:#01B2BC;">1m</font>**` | 1 分钟 |
| `**<font style="color:#01B2BC;">1h</font>**` | 1 小时 |
| `**<font style="color:#01B2BC;">1d</font>**` | 1 日历天 |
| `**<font style="color:#01B2BC;">1w</font>**` | 1 日历周 |
| `**<font style="color:#01B2BC;">1mo</font>**` | 1 日历月 |
| `**<font style="color:#01B2BC;">1q</font>**` | 1 日历季度 |
| `**<font style="color:#01B2BC;">1y</font>**` | 1 日历年 |
| `**<font style="color:#01B2BC;">1i</font>**` | 1 索引计数 |


这些字符串也可以组合使用。例如：“1y6m1w5d” 代表 1 年、6 个月、1 周和 5 天。通过这些设置，你可以创建常规时间窗口并将数据分组到其中。你可以创建三种类型的窗口配置，如 **图 12-2** 所示。

![图12-2 不同类型的时间窗口](https://cdn.nlark.com/yuque/0/2024/jpeg/1310472/1728832892134-b93b1c75-32a2-4aa2-a386-0935cfa8bab4.jpeg)

另外，`closed` 参数决定了精确位于下界或上界的值是否被包含或排除。可选项如 **表 12-3** 所示。



_表 12-3：闭区间选项_

| 参数 | 描述 | 区间 | 包含 a | 包含 b |
| --- | --- | --- | --- | --- |
| `**<font style="color:#01B2BC;">left</font>**` | 下界为闭合，上界为开区间 | [a, b) | ✓ | ✗ |
| `**<font style="color:#01B2BC;">right</font>**` | 下界为开区间，上界为闭合 | (a, b] | ✗ | ✓ |
| `**<font style="color:#01B2BC;">both</font>**` | 下界和上界均为闭合区间 | [a, b] | ✓ | ✓ |
| `**<font style="color:#01B2BC;">none</font>**` | 下界和上界均为开区间 | (a, b) | ✗ | ✗ |


---

**<font style="color:#01B2BC;">让 Polars 知道数据已排序可提升性能</font>**

如果你的索引列已经按升序排序，可以将 `check_sorted` 设置为 `False` 以加速分组过程。否则，Polars 将检查索引是否已排序，这在 `GroupBy` 上下文中是一项开销较大的操作，因为它无法使用 DataFrame 中通常可用的排序标志。

---

<h2 id="rUCuH">使用 `rolling` 进行滚动聚合</h2>
`group_by_dynamic` 创建的是固定大小和宽度的时间窗口，而 `rolling` 则根据 DataFrame 中的值为中心创建窗口。当你想计算滚动聚合（如移动平均或累积求和）时，这很有用。`rolling` 函数具有以下参数：

+ `index_column`：包含用于作为窗口锚点的值的列。
+ `period`：窗口的大小。
+ `offset`：将窗口向前或向后移动。
+ `closed`：定义如何处理边界值，工作方式与前面在 `group_by_dynamic` 中解释的一样。
+ `group_by`：在应用滚动聚合之前，按指定列对数据进行分组。

对于带有时间戳的数据集，滚动操作会为每个时间戳创建一个窗口，该窗口向后扩展指定的时间段。如果设置了 `offset`，它会将整个窗口向前或向后移动，从而调整分析的重点。图 12-3 展示了 `rolling` 方法确定时间窗口的方式。

![图12-3](https://cdn.nlark.com/yuque/0/2024/jpeg/1310472/1728833246062-6dc41636-2160-4532-ab6c-70aa467eb1de.jpeg)

`group_by` 参数允许你在数据组内执行滚动聚合。

假设你正在分析一个来自连锁零售店的数据集。你有多个地点的销售数据，并希望了解每个店铺在 7 天期间的滚动平均销售额。这将帮助我们识别趋势，例如哪些店铺表现稳定，哪些店铺可能经历了销售下降或波动。

让我们创建一个包含简单销售数据的小型 DataFrame。该 DataFrame 将包含两个星期的销售数据，这两个店铺只在工作日营业。你将计算每个店铺过去 7 天的销售总和：

```python
from datetime import date

dates = pl.date_range(  ①
    start=date(2024, 4, 1),
    end=date(2024, 4, 14),
    interval='1d',
    eager=True,  ②
)
dates  = dates.filter(dates.dt.weekday() < 6)  ③
dates_repeated = pl.concat([dates, dates]).sort()  ④

df = pl.DataFrame({
    "date": dates_repeated,
    "store": ["Store A", "Store B"] * dates.len(),
    "sales": [
        200, 150, 220, 160, 250, 180, 270, 190, 280, 210,
        210, 170, 220, 180, 240, 190, 250, 200, 260, 210,
    ]
}).set_sorted("date", "store")  ⑤
```

① 创建从 4 月 1 日到 4 月 14 日的日期范围。

② 将 `eager` 参数设置为 `True` 以立即创建日期范围。

③ 过滤掉周末。

④ 为两个星期重复这些日期并进行排序。

⑤ 指明日期列和店铺列已排序。



现在你有了一个不错的数据集，你可以计算每个店铺过去 7 天的滚动销售总和：

```python
result = (
    df.rolling(  ①
        index_column="date",
        period="7d",
        group_by="store",
        check_sorted=False,  ②
    ).agg(  ③
        pl.sum("sales").alias("sum_of_last_7_days_sales")
    )
)

final_df = df.join(result, on=["date", "store"])  ④

final_df
```

```plain
shape: (20, 4)
┌────────────┬─────────┬───────┬───────────────────────┐
│ date       │ store   │ sales │ sum_of_last_7_days_s… │
│ ---        │ ---     │ ---   │ ---                   │
│ date       │ str     │ i64   │ i64                   │
╞════════════╪═════════╪═══════╪═══════════════════════╡
│ 2024-04-01 │ Store A │ 200   │ 200                   │
│ 2024-04-02 │ Store A │ 220   │ 420                   │
│ 2024-04-03 │ Store A │ 250   │ 670                   │
│ 2024-04-04 │ Store A │ 270   │ 940                   │
│ 2024-04-05 │ Store A │ 280   │ 1220                  │
│ 2024-04-08 │ Store A │ 210   │ 1230                  │
│ 2024-04-09 │ Store A │ 220   │ 1230                  │
│ 2024-04-10 │ Store A │ 240   │ 1220                  │
│ 2024-04-11 │ Store A │ 250   │ 1200                  │
│ 2024-04-12 │ Store A │ 260   │ 1180                  │
│ 2024-04-01 │ Store B │ 150   │ 150                   │
│ 2024-04-02 │ Store B │ 160   │ 310                   │
│ 2024-04-03 │ Store B │ 180   │ 490                   │
│ 2024-04-04 │ Store B │ 190   │ 680                   │
│ 2024-04-05 │ Store B │ 210   │ 890                   │
│ 2024-04-08 │ Store B │ 170   │ 910                   │
│ 2024-04-09 │ Store B │ 180   │ 930                   │
│ 2024-04-10 │ Store B │ 190   │ 940                   │
│ 2024-04-11 │ Store B │ 200   │ 950                   │
│ 2024-04-12 │ Store B │ 210   │ 950                   │
└────────────┴─────────┴───────┴───────────────────────┘
```

①`rolling` 函数创建包含当前行及之前 7 天内的行的窗口。

② 将 `check_sorted` 参数设置为 `False` 以加速处理，因为你知道数据已经排序。翻译如下：

③ 使用 `rolling` 函数计算创建的时间窗口的总和。

④ 将滚动计算的结果与原始 DataFrame 进行合并。

在这里，你可以看到每个店铺过去 7 天的滚动总和已经计算出来。前 7 天的滚动总和只包含数据集中可用的日子，因为窗口中没有更多的天数可以包括。这种滚动聚合可以让你查看每个店铺随时间的销售变化情况。

<h2 id="yGSNF">本章小结</h2>
在本章中，你学习了如何对数据进行聚合。你学习了：

+ `group_by` 上下文中可用的基本聚合操作，如 `sum`、`mean`、`quantile` 和 `median`。
+ `agg` 方法中的高级聚合操作，允许你将列元素按组聚合成列表、控制生成列的名称、同时应用多个聚合函数，并应用你自己的自定义聚合函数。
+ 用户定义的函数（UDF），允许你使用 `map_elements`、`map_batches` 和 `map_groups` 将自定义 Python 函数应用到数据中。
+ 使用 `group_by_dynamic` 函数基于时间窗口创建分组。
+ 使用 `rolling` 函数围绕 DataFrame 中的值创建滚动聚合。
+ 使用 `over()` 表达式在 `select` 上下文中对分组进行聚合。
+ 你可以应用于 Python 函数的一些优化措施，如 `set_sorted` 和 `@lru_cache`，以加速处理过程。

在下一章中，你将学习如何通过 `joins` 和 `unions` 组合多个 DataFrame。

