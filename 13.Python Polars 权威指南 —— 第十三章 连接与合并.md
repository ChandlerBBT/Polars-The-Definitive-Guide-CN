

数据通常来自多个来源，你需要以有意义的方式将它们连接和合并。本章将介绍多种合并DataFrame的方法。

<h2 id="K4fLs">连接（Joining）</h2>
要合并不同的DataFrame，Polars提供了`join()`方法。它接受以下参数：

+ `**other**`: 要合并的DataFrame。
+ `**on**`: 当左侧和右侧DataFrame中列名相同时，用于连接的列。
+ `**left_on**` 和 `**right_on**`: 当左侧和右侧DataFrame中的列名不同，用于连接的列。
+ `**how**`: 使用的连接策略。
+ `**suffix**`: 在两个DataFrame中都出现的列名后附加的后缀。
+ `**validate**`: 验证连接的类型。
+ `**join_nulls**`: 是否连接空值（null）。默认情况下，空值不参与连接。

<h3 id="pMXx8">连接策略</h3>
连接可以根据不同的策略进行。根据你的需求，需要以不同方式合并两个数据集。Polars支持的连接策略如下：

+ `**inner**`**（默认）**: 只保留在两个DataFrame中都有匹配的行。
+ `**outer**`: 保留两个DataFrame中的所有行。
+ `**left**`: 保留左侧DataFrame中的所有行，以及右侧DataFrame中与之匹配的行。
+ `**cross**`: 创建两个DataFrame的笛卡尔积。笛卡尔积源自集合论，表示两个集合中所有元素的组合。稍后你将看到这个策略的示例。
+ `**semi**`: 保留左侧DataFrame中与右侧DataFrame匹配的所有行。
+ `**anti**`: 保留左侧DataFrame中未与右侧DataFrame匹配的所有行。

在本节中，你将使用以下DataFrame来演示不同的连接策略：

```python
import polars as pl

df_left = pl.DataFrame({
    "key": ["A", "B", "C", "D"],
    "value": [1, 2, 3, 4]
})

df_right = pl.DataFrame({
    "key": ["B", "C", "D", "E"],
    "value": [5, 6, 7, 8]
})
```

<h4 id="vvTBx">inner（内部连接）</h4>
Polars的默认连接策略是内部连接。该策略只保留在两个DataFrame中都有匹配的行，丢弃没有匹配的行。你会在下面的示例中看到，左侧DataFrame中键为A的行和右侧DataFrame中键为E的行没有出现在结果DataFrame中。其他行都在结果中，因为它们在两个DataFrame中都有匹配。

```python
df_left.join(df_right, on="key", how="inner")
```

```plain
shape: (3, 3)
┌─────┬───────┬─────────────┐
│ key │ value │ value_right │
│ --- │ ---   │ ---         │
│ str │ i64   │ i64         │
╞═════╪═══════╪═════════════╡
│ B   │ 2     │ 5           │
│ C   │ 3     │ 6           │
│ D   │ 4     │ 7           │
└─────┴───────┴─────────────┘
```

<h4 id="AVKkq">outer（外部连接）</h4>
外部连接策略保留两个DataFrame中的所有行，缺失的值用null填充。此外，你可以更改默认的后缀，以便处理右侧DataFrame中重复列的情况。下面的示例中，我们将后缀更改为`_other`。

```python
df_left.join(df_right, on="key", how="outer", suffix="_other")
```

```plain
shape: (5, 4)
┌──────┬───────┬───────────┬─────────────┐
│ key  │ value │ key_other │ value_other │
│ ---  │ ---   │ ---       │ ---         │
│ str  │ i64   │ str       │ i64         │
╞══════╪═══════╪═══════════╪═════════════╡
│ B    │ 2     │ B         │ 5           │
│ C    │ 3     │ C         │ 6           │
│ D    │ 4     │ D         │ 7           │
│ null │ null  │ E         │ 8           │
│ A    │ 1     │ null      │ null        │
└──────┴───────┴───────────┴─────────────┘
```

<h4 id="RexIQ">left（左连接）</h4>
左连接策略保留左侧DataFrame中的所有行，以及右侧DataFrame中与之匹配的行，缺失的值用null填充。注意，Polars没有右连接，但你可以通过在连接操作中交换DataFrame来实现右连接。

```python
df_left.join(df_right, on="key", how="left")
```

```plain
shape: (4, 3)
┌─────┬───────┬─────────────┐
│ key │ value │ value_right │
│ --- │ ---   │ ---         │
│ str │ i64   │ i64         │
╞═════╪═══════╪═════════════╡
│ A   │ 1     │ null        │
│ B   │ 2     │ 5           │
│ C   │ 3     │ 6           │
│ D   │ 4     │ 7           │
└─────┴───────┴─────────────┘
```

<h4 id="pjmdN">cross（交叉连接）</h4>
交叉连接策略创建两个DataFrame的笛卡尔积。这意味着结果DataFrame的长度将等于左侧DataFrame的长度乘以右侧DataFrame的长度，可能会产生非常大的DataFrame！该连接不需要`on`参数，因为所有行都会彼此连接。

```python
df_left.join(df_right, how="cross")
```

```plain
shape: (16, 4)
┌─────┬───────┬───────────┬─────────────┐
│ key │ value │ key_right │ value_right │
│ --- │ ---   │ ---       │ ---         │
│ str │ i64   │ str       │ i64         │
╞═════╪═══════╪═══════════╪═════════════╡
│ A   │ 1     │ B         │ 5           │
│ A   │ 1     │ C         │ 6           │
│ A   │ 1     │ D         │ 7           │
│ A   │ 1     │ E         │ 8           │
│ B   │ 2     │ B         │ 5           │
│ …   │ …     │ …         │ …           │
│ C   │ 3     │ E         │ 8           │
│ D   │ 4     │ B         │ 5           │
│ D   │ 4     │ C         │ 6           │
│ D   │ 4     │ D         │ 7           │
│ D   │ 4     │ E         │ 8           │
└─────┴───────┴───────────┴─────────────┘
```

<h4 id="QyDiV">semi（半连接）</h4>
半连接是一种特殊的连接，它不会将右侧DataFrame中的数据添加到结果DataFrame中。相反，它只保留左侧DataFrame中与右侧DataFrame匹配的行。这使得半连接成为过滤左侧DataFrame的额外方式之一。

```python
df_left.join(df_right, on="key", how="semi")
```

```plain
shape: (3, 2)
┌─────┬───────┐
│ key │ value │
│ --- │ ---   │
│ str │ i64   │
╞═════╪═══════╡
│ B   │ 2     │
│ C   │ 3     │
│ D   │ 4     │
└─────┴───────┘
```

<h4 id="d3igG">anti（反连接）</h4>
反连接策略是半连接的相反操作。它只保留左侧DataFrame中未与右侧DataFrame匹配的行。

```python
df_left.join(df_right, on="key", how="anti")
```

```plain
shape: (1, 2)
┌─────┬───────┐
│ key │ value │
│ --- │ ---   │
│ str │ i64   │
╞═════╪═══════╡
│ A   │ 1     │
└─────┴───────┘
```



<h3 id="aPt8y">在多个列上进行连接</h3>
你可以通过将列名列表传递给`on`参数，在多个列上连接DataFrame。这将基于列表中的所有列连接DataFrame。

要尝试这个，你需要两个包含更多列的DataFrame。对于本示例，你将使用以下示例DataFrame。在这些DataFrame中，你将基于`name`和`city`列进行连接。

```python
df_left = pl.DataFrame({
    "name": ["Alice", "Bob", "Charlie", "Dave"],
    "city": ["NY", "LA", "NY", "SF"],
    "age": [25, 30, 35, 40]
})

df_right = pl.DataFrame({
    "name": ["Alice", "Bob", "Charlie", "Dave"],
    "city": ["NY", "LA", "NY", "Chicago"],
    "department": ["Finance", "Marketing", "Engineering", "Operations"]
})

df_left.join(df_right, on=["name", "city"], how="inner")
```

```plain
shape: (3, 4)
┌─────────┬──────┬─────┬─────────────┐
│ name    │ city │ age │ department  │
│ ---     │ ---  │ --- │ ---         │
│ str     │ str  │ i64 │ str         │
╞═════════╪══════╪═════╪═════════════╡
│ Alice   │ NY   │ 25  │ Finance     │
│ Bob     │ LA   │ 30  │ Marketing   │
│ Charlie │ NY   │ 35  │ Engineering │
└─────────┴──────┴─────┴─────────────┘
```

<h3 id="RURtp">验证</h3>
在连接数据后，你可以验证连接是否具有特定的基数。这涉及检查连接表之间的关系性质，以确保它们按照预期的关系进行了连接。可以验证以下关系：

+ **多对多（**`**m:m**`**）**  
多对多连接是指左侧DataFrame中的多行与右侧DataFrame中的多行匹配。一个例子是将员工表连接到项目表。每个员工可以参与多个项目，而项目通常有多个员工在工作。在Polars中，这是默认选项，不会进行检查。
+ **一对多（**`**1:m**`**）**  
一对多连接是指左侧DataFrame中的单行与右侧DataFrame中的多行匹配。一个例子是将部门列表与员工列表连接。每个部门有多个员工，但每个员工仅属于一个部门。Polars会验证左侧DataFrame中的连接键是否唯一。
+ **多对一（**`**m:1**`**）**  
多对一连接是指左侧DataFrame中的多行与右侧DataFrame中的单行匹配。一个例子是将员工表与他们所住城市的表连接。每个员工只能住在一个城市，但一个城市可以包含多个员工。Polars会验证右侧DataFrame中的连接键是否唯一。
+ **一对一（**`**1:1**`**）**  
一对一连接是指左侧DataFrame中的单行与右侧DataFrame中的单行匹配。一个例子是将员工表与员工ID表连接。Polars会验证两个DataFrame中的连接键是否唯一。

要验证连接，可以将`validate`参数传递给`join`方法。该参数接受一个字符串，指示你想要验证的关系。

在下面的示例中，你将创建两个DataFrame，包含一组员工和一组部门，并以多对一的方式进行连接。每个员工仅属于一个部门，但每个部门可以有多个员工：

```python
df_employees = pl.DataFrame({
    "employee_id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "Dave"],
    "department_id": [10, 10, 30, 10],
})

df_departments = pl.DataFrame({
    "department_id": [10, 20, 30],
    "department_name": ["Information Technology", "Finance", "Human Resources"],
})

df_employees.join(
    df_departments,
    on="department_id",
    how="left",
    validate="m:1"
)
```

```plain
shape: (4, 4)
┌─────────────┬─────────┬───────────────┬───────────────────────┐
│ employee_id │ name    │ department_id │ department_name       │
│ ---         │ ---     │ ---           │ ---                   │
│ i64         │ str     │ i64           │ str                   │
╞═════════════╪═════════╪═══════════════╪═══════════════════════╡
│ 1           │ Alice   │ 10            │ Information Technolo… │
│ 2           │ Bob     │ 10            │ Information Technolo… │
│ 3           │ Charlie │ 30            │ Human Resources       │
│ 4           │ Dave    │ 10            │ Information Technolo… │
└─────────────┴─────────┴───────────────┴───────────────────────┘
```



如果多个部门共享相同的ID，验证将失败。

```python
df_departments = pl.DataFrame({
    "department_id": [10, 20, 10],
    "department_name": ["Information Technology", "Finance", "Human Resources"],
})

df_employees.join(
    df_departments,
    on="department_id",
    how="left",
    validate="m:1"
)
```

```plain
ComputeError: the join keys did not fulfil m:1 validation
```



<h2 id="tiOBP">不精确连接（Inexact Joining）</h2>
在连接DataFrame时，你可能希望根据彼此接近但不完全相同的值来连接两个表。例如，从不同来源的销售数据集中，一个系统会在将销售记录写入数据库时进行时间戳记，而另一个系统则在支付时进行时间戳记。这会导致时间戳不一致的差异，可以通过基于最近值来连接DataFrame来解决。你可以通过Polars的`join_asof`函数实现这一点。

`join_asof`接受以下参数：

+ `**other**`: 要连接的DataFrame。
+ `**on**`: 当左侧和右侧DataFrame中的列名相同时，用于连接的列。
+ `**left_on**` 和 `**right_on**`: 当左侧和右侧DataFrame中的列名不同时，用于连接的列。
+ `**by**`: 在执行`join_asof`之前用于连接的列。
+ `**by_left**` 和 `**by_right**`: 在执行`join_asof`之前用于连接的列，如果左右两侧DataFrame的列名不同，则使用这些参数。
+ `**strategy**`: 连接时使用的策略。
+ `**suffix**`: 用于命名在两个DataFrame中都出现的相同列的后缀。
+ `**tolerance**`: 认为匹配的最大值差异。
+ `**allow_parallel**`: 允许Polars并行计算DataFrame以进行连接，默认值为`True`。
+ `**force_parallel**`: 强制在连接之前进行并行帧计算，默认值为`False`。

在深入一些示例之前，你需要了解`join_asof`仅在两个DataFrame在要连接的列上排序的情况下才能工作。如果DataFrame没有排序，会报错。

```python
df_left = pl.DataFrame({
    "int_id": [5, 10],
    "value": ["1", "2"]
})

df_right = pl.DataFrame({
    "int_id": [4, 7, 12],
    "value": [1, 2, 3]
})

df_left.join_asof(df_right, on="int_id", tolerance=3)
```

```plain
InvalidOperationError: argument in operation 'asof_join' is not explicitly sorte
d

- If your data is ALREADY sorted, set the sorted flag with: '.set_sorted()'.
- If your data is NOT sorted, sort the 'expr/series/column' first.
```



在这个示例中，DataFrame已经排序，我们可以通过调用`set_sorted("int_id")`方法来告诉Polars这一点。`set_sorted()`是一个无代价操作，因为你只是向Polars提供了列已经排序的信息。在其他情况下，你可以通过调用`sort("int_id")`方法对DataFrame进行排序。即使数据已经排序，这也会花费一些时间来检查是否排序，以及在未排序的情况下进行排序。

```python
df_left = df_left.set_sorted("int_id")
df_right = df_right.set_sorted("int_id")

df_left.join_asof(df_right, on="int_id")
```

```plain
shape: (2, 3)
┌────────┬───────┬─────────────┐
│ int_id │ value │ value_right │
│ ---    │ ---   │ ---         │
│ i64    │ str   │ i64         │
╞════════╪═══════╪═════════════╡
│ 5      │ 1     │ 1           │
│ 10     │ 2     │ 2           │
└────────┴───────┴─────────────┘
```



注意，如果左右两个DataFrame中要连接的列名相同，连接时会丢弃右侧DataFrame的连接列。如果这不是你想要的结果，你可以在之前重命名该列，并使用`on_left`和`on_right`参数。

```python
df_right = df_right.rename({"int_id": "int_id_right"})

df_left.join_asof(
    df_right,
    left_on="int_id",
    right_on="int_id_right",
)
```

```plain
shape: (2, 4)
┌────────┬───────┬──────────────┬─────────────┐
│ int_id │ value │ int_id_right │ value_right │
│ ---    │ ---   │ ---          │ ---         │
│ i64    │ str   │ i64          │ i64         │
╞════════╪═══════╪══════════════╪═════════════╡
│ 5      │ 1     │ 4            │ 1           │
│ 10     │ 2     │ 7            │ 2           │
└────────┴───────┴──────────────┴─────────────┘
```

<h3 id="feYJ7">`join_asof`策略</h3>
`join_asof`函数提供了三种策略来连接DataFrame：

+ `***backward**`**（默认）**: 使用最后一个具有相等或较小值的行进行连接。
+ `***forward**`: 使用第一个具有相等或较大值的行进行连接。
+ `***nearest**`: 使用具有最接近值的行进行连接。

默认策略是`backward`策略。此策略将行连接到其他DataFrame的连接列中的第一个值，该值等于或小于左侧DataFrame中的值，同时仍在`tolerance`定义的范围内。

```python
df_left.join_asof(
    df_right,
    left_on="int_id",
    right_on="int_id_right",
    tolerance=3,
    strategy="backward"
)
```

```plain
shape: (2, 4)
┌────────┬───────┬──────────────┬─────────────┐
│ int_id │ value │ int_id_right │ value_right │
│ ---    │ ---   │ ---          │ ---         │
│ i64    │ str   │ i64          │ i64         │
╞════════╪═══════╪══════════════╪═════════════╡
│ 5      │ 1     │ 4            │ 1           │
│ 10     │ 2     │ 7            │ 2           │
└────────┴───────┴──────────────┴─────────────┘
```



`forward`策略会查找其他DataFrame连接列中的第一个值，该值等于或大于左侧DataFrame中的值。

```python
df_left.join_asof(
    df_right,
    left_on="int_id",
    right_on="int_id_right",
    tolerance=3,
    strategy="forward"
)
```

```plain
shape: (2, 4)
┌────────┬───────┬──────────────┬─────────────┐
│ int_id │ value │ int_id_right │ value_right │
│ ---    │ ---   │ ---          │ ---         │
│ i64    │ str   │ i64          │ i64         │
╞════════╪═══════╪══════════════╪═════════════╡
│ 5      │ 1     │ 7            │ 2           │
│ 10     │ 2     │ 12           │ 3           │
└────────┴───────┴──────────────┴─────────────┘
```



最后，`nearest`策略将行连接到最接近的值。

```python
df_left.join_asof(
    df_right,
    left_on="int_id",
    right_on="int_id_right",
    tolerance=3,
    strategy="nearest"
)
```

```plain
shape: (2, 4)
┌────────┬───────┬──────────────┬─────────────┐
│ int_id │ value │ int_id_right │ value_right │
│ ---    │ ---   │ ---          │ ---         │
│ i64    │ str   │ i64          │ i64         │
╞════════╪═══════╪══════════════╪═════════════╡
│ 5      │ 1     │ 4            │ 1           │
│ 10     │ 2     │ 12           │ 3           │
└────────┴───────┴──────────────┴─────────────┘
```



<h3 id="gOEjV">使用`tolerance`和`by`进行额外的微调</h3>
当你设置`tolerance`参数时，只有当最近匹配的值落在某个范围内时，行才会被连接。你可以为数值和时间类型的数据设置`tolerance`。对于时间类型数据，使用`datetime.timedelta`或之前在表12-2中提到的持续时间字符串，例如"7d12h30m"。

如果你想确保行只与在指定列中具有相同值的另一DataFrame中的行连接，而不是仅与任何最近的匹配项连接，你可以使用`by`关键字。如果列的名称不匹配，使用`by_left`和`by_right`的组合。

现在让我们在一个实际用例中运用这些参数。

<h3 id="GVJqh">用户案例：营销活动归因</h3>
假设：你需要找出公司营销活动的效果。你收集了两个数据集：一个包含销售数据，另一个包含过去一年的营销活动数据。

```python
marketing_lf = pl.scan_csv("data/marketing use case/marketing_campaigns.csv")
marketing_lf.fetch(1)
```

```plain
shape: (1, 3)
┌───────────────┬─────────────────────┬──────────────┐
│ Campaign Name │ Campaign Date       │ Product Type │
│ ---           │ ---                 │ ---          │
│ str           │ str                 │ str          │
╞═══════════════╪═════════════════════╪══════════════╡
│ Launch        │ 2023-01-01 20:00:00 │ Electronics  │
└───────────────┴─────────────────────┴──────────────┘
```

```python
marketing_lf.select(pl.col("Product Type").unique()).collect()
```

```plain
shape: (4, 1)
┌──────────────┐
│ Product Type │
│ ---          │
│ str          │
╞══════════════╡
│ Books        │
│ Furniture    │
│ Electronics  │
│ Clothing     │
└──────────────┘
```

```python
sales_lf = pl.scan_csv("data/marketing use case/sales_data.csv")
sales_lf.fetch(1)
```

```plain
shape: (1, 3)
┌───────────────────────┬──────────────┬──────────┐
│ Sale Date             │ Product Type │ Quantity │
│ ---                   │ ---          │ ---      │
│ str                   │ str          │ i64      │
╞═══════════════════════╪══════════════╪══════════╡
│ 2023-01-01 02:00:00.… │ Books        │ 7        │
└───────────────────────┴──────────────┴──────────┘
```



你可以看到时间戳仍然是字符串数据类型。为了处理这些数据，你需要先将其格式化为匹配的时间数据类型。此外，由于日期并不完全匹配，使用`join_asof`函数连接两个DataFrame。另外，通过设置`by`参数，将活动与同类产品的销售数据匹配。最后，活动不会一直有效。假设在这种情况下，一个活动有效期为2个月，因此将`tolerance`设置为2个月。

```python
sales_lf = sales_lf.with_columns(
    pl.col("Sale Date")
    .str.to_datetime("%Y-%m-%d %H:%M:%S%.f")
    .cast(pl.Datetime("us")),
)
marketing_lf = marketing_lf.with_columns(
    pl.col("Campaign Date").str.to_datetime("%Y-%m-%d %H:%M:%S"),
)

sales_with_campaign_df = (
    sales_lf
    .sort("Sale Date")
    .join_asof(
        marketing_lf
        .sort("Campaign Date"),
        left_on="Sale Date",
        right_on="Campaign Date",
        by="Product Type",
        strategy="backward",
        tolerance="60d"
    )
    .collect()
)
sales_with_campaign_df
```

```plain
shape: (20_000, 5)
┌──────────────┬──────────────┬──────────┬───────────────┬───────────────┐
│ Sale Date    │ Product Type │ Quantity │ Campaign Name │ Campaign Date │
│ ---          │ ---          │ ---      │ ---           │ ---           │
│ datetime[μs] │ str          │ i64      │ str           │ datetime[μs]  │
╞══════════════╪══════════════╪══════════╪═══════════════╪═══════════════╡
│ 2023-01-01   │ Electronics  │ 2        │ null          │ null          │
│ 01:26:12.…   │              │          │               │               │
│ 2023-01-01   │ Books        │ 7        │ null          │ null          │
│ 02:00:00     │              │          │               │               │
│ 2023-01-01   │ Toys         │ 9        │ null          │ null          │
│ 06:14:30.…   │              │          │               │               │
│ 2023-01-01   │ Clothing     │ 9        │ null          │ null          │
│ 06:52:25.…   │              │          │               │               │
│ 2023-01-01   │ Books        │ 7        │ null          │ null          │
│ 07:44:50.…   │              │          │               │               │
│ …            │ …            │ …        │ …             │ …             │
│ 2023-12-31   │ Clothing     │ 10       │ null          │ null          │
│ 15:45:29.…   │              │          │               │               │
│ 2023-12-31   │ Toys         │ 4        │ null          │ null          │
│ 18:15:09.…   │              │          │               │               │
│ 2023-12-31   │ Electronics  │ 7        │ null          │ null          │
│ 18:33:47.…   │              │          │               │               │
│ 2023-12-31   │ Books        │ 6        │ null          │ null          │
│ 18:37:54.…   │              │          │               │               │
│ 2023-12-31   │ Furniture    │ 4        │ null          │ null          │
│ 19:41:22.…   │              │          │               │               │
└──────────────┴──────────────┴──────────┴───────────────┴───────────────┘
```



现在，如果你想知道这些活动是否导致了更高的平均销售量，你可以按产品类型和活动名称对数据进行分组。这可以让你比较有活动和没有活动时的产品销量，并计算平均销售量，就像在第十二章讨论的那样。

```python
(
    sales_with_campaign_df
    .group_by("Product Type", "Campaign Name")
    .agg(pl.col("Quantity").mean())
    .sort("Product Type", "Campaign Name")
)
```

```plain
shape: (9, 3)
┌──────────────┬───────────────┬──────────┐
│ Product Type │ Campaign Name │ Quantity │
│ ---          │ ---           │ ---      │
│ str          │ str           │ f64      │
╞══════════════╪═══════════════╪══════════╡
│ Books        │ null          │ 5.527716 │
│ Clothing     │ null          │ 5.433385 │
│ Clothing     │ New Arrivals  │ 8.200581 │
│ Electronics  │ null          │ 5.486832 │
│ Electronics  │ Launch        │ 8.080775 │
│ Electronics  │ Seasonal Sale │ 8.471406 │
│ Furniture    │ null          │ 5.430222 │
│ Furniture    │ Discount      │ 8.191888 │
│ Toys         │ null          │ 5.50318  │
└──────────────┴───────────────┴──────────┘
```



从结果中我们可以看到，营销活动通常会导致更高的销售量，除了图书和玩具类别。玩具类别从未进行过活动，这可以解释这一现象，但图书类别又是怎么回事呢？

```python
marketing_lf.filter(pl.col("Product Type") == "Books").collect()
```

```plain
shape: (1, 3)
┌───────────────┬─────────────────────┬──────────────┐
│ Campaign Name │ Campaign Date       │ Product Type │
│ ---           │ ---                 │ ---          │
│ str           │ datetime[μs]        │ str          │
╞═══════════════╪═════════════════════╪══════════════╡
│ Clearance     │ 2023-12-31 21:00:00 │ Books        │
└───────────────┴─────────────────────┴──────────────┘
```



似乎图书类别只进行了一次活动：跨年清仓特卖。让我们看看在那之后是否还有任何销售：

```python
(
    sales_lf
    .filter(
        (pl.col("Product Type") == "Books") &
        (
            pl.col("Sale Date") >
            pl.lit("2023-12-31 21:00:00").str.to_datetime()
        )
    )
    .collect()
)
```

```plain
shape: (0, 3)
┌──────────────┬──────────────┬──────────┐
│ Sale Date    │ Product Type │ Quantity │
│ ---          │ ---          │ ---      │
│ datetime[μs] │ str          │ i64      │
╞══════════════╪══════════════╪══════════╡
└──────────────┴──────────────┴──────────┘
```



似乎在清仓开始后，没有再销售任何图书。由于我们使用的是`backward`的`join_asof`策略，这个活动没有与任何值匹配，因此在结果中缺失！这意味着它可能导致的销售不在数据集中，导致看起来它没有效果！

<h2 id="hUGvR">垂直和水平合并</h2>
`join`函数基于DataFrame中的值合并DataFrame，但有时你只是想将DataFrame按顺序添加在一起，而不考虑它们的值。通常，DataFrame存储在内存的不同位置。当你想将它们合并时，可以有三种方式：

1. 通过复制到新位置，将数据合并到一个新的DataFrame中。
2. 让新的DataFrame指向数据存储的位置。
3. 将第二个DataFrame的数据复制到第一个DataFrame的数据后面。

第一种方式是将数据复制到新位置。这是`pl.concat(...)`的默认行为。此函数接受DataFrame、LazyFrame或Series的列表，并且可以垂直、水平或对角线连接它们。合并帧后，它会将结果DataFrame重新分块，将数据复制到新位置并存储为单个块。这保证了之后的查询性能最佳。

正如在第三章解释的那样，重新分块会将数据复制到内存中的新位置，使其再次连续。这提高了查询性能，特别是在结果DataFrame多次查询时非常有用。

`concat`具有以下关键字参数：

+ `**items**`: 要连接的DataFrame、LazyFrame或Series的列表。
+ `**how**`: 合并这些项的策略。
+ `**rechunk**`: 是否重新分块结果DataFrame，默认为`True`。
+ `**parallel**`: 确定是否并行计算LazyFrame，默认为`True`。

你可以选择以下连接策略：

+ `**vertical**`**（默认）**: 垂直连接项。应用多个`vstack`操作。如果DataFrame的列不匹配（包括数据类型），操作将失败。
+ `**vertical_relaxed**`: 垂直连接项，并在列类型不匹配时将列强制转换为超级类型。如果DataFrame的列名不匹配，操作将失败，忽略数据类型。
+ `**horizontal**`: 水平连接项。如果长度不匹配，使用 `null` 填充。
+ `**diagonal**`: 通过创建它们列的并集来组合项。使用 `null` 填充缺失的值。
+ `**diagonal_relaxed**`: 与`diagonal`相同，另外在列类型不匹配时将列强制转换为超级类型。
+ `**align**`: 以智能方式水平合并帧。它根据两个DataFrame中都有的列的值对齐行。缺失的值用 `null` 填充。

第一种策略是垂直连接。这是`concat`的默认策略。它垂直组合DataFrame，意味着DataFrame的行将堆叠在一起。工作方式如图13-1所示。

![图13-1 垂直连接](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728835029031-367fd7f4-0722-4b99-be8b-2c4364647fc5.png)

```python
df1 = pl.DataFrame({
    "id": [1, 2, 3],
    "value": ["a", "b", "c"],
})
df2 = pl.DataFrame({
    "id": [4, 5],
    "value": ["d", "e"],
})
pl.concat([df1,df2], how="vertical")
```

```plain
shape: (5, 2)
┌─────┬───────┐
│ id  │ value │
│ --- │ ---   │
│ i64 │ str   │
╞═════╪═══════╡
│ 1   │ a     │
│ 2   │ b     │
│ 3   │ c     │
│ 4   │ d     │
│ 5   │ e     │
└─────┴───────┘
```



第二种策略是“水平（horizontal）”连接。这种策略水平组合DataFrame，这意味着DataFrame的列会并排堆叠。当DataFrame的长度不匹配时，结果DataFrame将用null值填充。列名不能相同，如果相同，则操作会失败。你可以通过在连接之前重命名列来规避这个问题。图13-2展示了其工作方式。

![图13-2 水平连接](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728835295735-65651e98-65e0-4b97-a311-6d378d2f1a7a.png)

```python
df1 = pl.DataFrame({
    "id": [1, 2, 3],
    "value": ["a", "b", "c"],
})
df2 = pl.DataFrame({
    "value2": ["x", "y"],
})
pl.concat([df1,df2], how="horizontal")
```

```plain
shape: (3, 3)
┌─────┬───────┬────────┐
│ id  │ value │ value2 │
│ --- │ ---   │ ---    │
│ i64 │ str   │ str    │
╞═════╪═══════╪════════╡
│ 1   │ a     │ x      │
│ 2   │ b     │ y      │
│ 3   │ c     │ null   │
└─────┴───────┴────────┘
```





第三种策略是“对角线（diagonal）”连接。这种策略通过创建列的并集来组合DataFrame。任何DataFrame中缺失的列值都将用null值填充。图13-3展示了其工作方式。

![图13-3 对角线连接](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728835347105-c5cbabdb-dc9e-46de-9c99-e2b9eaf754b3.png)



```python
df1 = pl.DataFrame({
    "id": [1, 2, 3],
    "value": ["a", "b", "c"],
})
df2 = pl.DataFrame({
    "value": ["d", "e"],
    "value2": ["x", "y"],
})
pl.concat([df1,df2], how="diagonal")
```

```plain
shape: (5, 3)
┌──────┬───────┬────────┐
│ id   │ value │ value2 │
│ ---  │ ---   │ ---    │
│ i64  │ str   │ str    │
╞══════╪═══════╪════════╡
│ 1    │ a     │ null   │
│ 2    │ b     │ null   │
│ 3    │ c     │ null   │
│ null │ d     │ x      │
│ null │ e     │ y      │
└──────┴───────┴────────┘
```



第四种也是最后一种策略是“对齐（align）”连接。这种策略不是简单地将行或列拼接在一起，而是找到两个DataFrame中可用列中的匹配值，并基于这些值对齐行，如图13-4所示。

![图13-4 对齐连接](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728835415519-34987bc1-c4d9-4cac-afbf-e6841ec1a848.png)

```python
df1 = pl.DataFrame({
    "id": [1, 2, 3],
    "value": ["a", "b", "c"],
})
df2 = pl.DataFrame({
    "value": ["a", "c", "d"],
    "value2": ["x", "y", "z"],
})
pl.concat([df1,df2], how="align")
```

```plain
shape: (4, 3)
┌──────┬───────┬────────┐
│ id   │ value │ value2 │
│ ---  │ ---   │ ---    │
│ i64  │ str   │ str    │
╞══════╪═══════╪════════╡
│ 1    │ a     │ x      │
│ 2    │ b     │ null   │
│ 3    │ c     │ y      │
│ null │ d     │ z      │
└──────┴───────┴────────┘
```



此外，垂直、水平和对角线策略各自都有一个“放松版”。这意味着，如果两个DataFrame中具有相同名称的列类型不匹配，列将被强制转换为一个超级类型。例如，包含整数和浮点数的列将被强制转换为浮点数列，包含整数和字符串的列将被强制转换为字符串列。这在你想要连接具有相同列但不同数据类型的DataFrame时非常有用。

下面的示例展示了当你试图连接具有相同列但数据类型不同的两个DataFrame时会发生什么：

```python
df1 = pl.DataFrame({
    "id": [1, 2, 3],
    "value": ["a", "b", "c"],
})
df2 = pl.DataFrame({
    "id": [4.0, 5.0],
    "value": [1, 2],
})
pl.concat([df1,df2], how="vertical")
```

```plain
SchemaError: type Float64 is incompatible with expected type Int64
```



当你使用“垂直放松版”策略时，连接将成功：

```python
pl.concat([df1,df2], how="vertical_relaxed")
```

```plain
shape: (5, 2)
┌─────┬───────┐
│ id  │ value │
│ --- │ ---   │
│ f64 │ str   │
╞═════╪═══════╡
│ 1.0 │ a     │
│ 2.0 │ b     │
│ 3.0 │ c     │
│ 4.0 │ 1     │
│ 5.0 │ 2     │
└─────┴───────┘
```



---

`align_frames`

`align`策略基于`align_frames`函数。该函数允许你选择一列，并根据该列中的值对一组DataFrame的行进行对齐。如果某个DataFrame中缺失某个值，结果DataFrame的该行将填充null值。如果值出现多次，将创建这些行的笛卡尔积。该函数返回相同的DataFrame，但它们的行彼此对齐。

`align_frames`的关键字参数如下：

+ `**frames**`: 你想要彼此对齐的DataFrame。
+ `**on**`: 用于对齐的列。
+ `**how**`: 用于确定结果值的连接策略，默认为外连接（outer）。
+ `**select**`: 选择并排列结果DataFrame的列。
+ `**descending**`: 是否按降序排序结果DataFrame。这也可以是与`on`中列长度匹配的布尔值列表。

```python
df1 = pl.DataFrame({
    "id": [1, 2, 2],
    "value": ["a", "c", "b"],
})
df2 = pl.DataFrame({
    "id": [2, 2],
    "value": ["x", "y"],
})
pl.align_frames(df1,df2, on="id")
```

```plain
[shape: (5, 2)
 ┌─────┬───────┐
 │ id  │ value │
 │ --- │ ---   │
 │ i64 │ str   │
 ╞═════╪═══════╡
 │ 1   │ a     │
 │ 2   │ c     │
 │ 2   │ c     │
 │ 2   │ b     │
 │ 2   │ b     │
 └─────┴───────┘,
 shape: (5, 2)
 ┌─────┬───────┐
 │ id  │ value │
 │ --- │ ---   │
 │ i64 │ str   │
 ╞═════╪═══════╡
 │ 1   │ null  │
 │ 2   │ x     │
 │ 2   │ y     │
 │ 2   │ x     │
 │ 2   │ y     │
 └─────┴───────┘]
```



注意，在第二个DataFrame中，缺失ID为“1”的值时，该值用 `null` 填充。此外，由于两个DataFrame都包含ID为“2”的值两次，结果DataFrame将包含这些行的笛卡尔积。

---



<h3 id="I6Ajc">`vstack`</h3>
`vstack`和`hstack`函数使用第二种方式来组合DataFrame。（一个类似的函数称为`append`，用于Series）。它们将两个DataFrame组合在一起，而不移动内存中的数据。相反，它们创建一个包含多个内存块的新DataFrame或Series，这些块可以位于内存的不同位置。这使得堆叠操作快速，但查询可能较慢，因为需要从内存的多个位置读取数据。`concat`的垂直策略使用`vstack`操作，但它也可以单独调用。`concat`允许你对结果DataFrame进行重新分块，以防止性能下降，而`vstack`则不允许。

这些是当你一次追加多个DataFrame时的首选方法。注意，堆叠操作仅适用于DataFrame，而不适用于LazyFrame，因为它们需要组合现有的内存块。

`vstack`要求DataFrame的宽度、列名和它们的数据类型匹配。

```python
df1 = pl.DataFrame({
    "id": [1, 2],
    "value": ["a", "b"],
})
df2 = pl.DataFrame({
    "id": [3, 4],
    "value": ["c", "d"],
})
df1.vstack(df2)
```

```plain
shape: (4, 2)
┌─────┬───────┐
│ id  │ value │
│ --- │ ---   │
│ i64 │ str   │
╞═════╪═══════╡
│ 1   │ a     │
│ 2   │ b     │
│ 3   │ c     │
│ 4   │ d     │
└─────┴───────┘
```



<h3 id="LwHZT">`hstack`</h3>
与`vstack`完全相同，`hstack`将DataFrame水平组合。此操作要求DataFrame的高度匹配。

```python
df1 = pl.DataFrame({
    "id": [1, 2],
    "value": ["a", "b"],
})
df2 = pl.DataFrame({
    "value2": ["x", "y"],
})
df1.hstack(df2)
```

```plain
shape: (2, 3)
┌─────┬───────┬────────┐
│ id  │ value │ value2 │
│ --- │ ---   │ ---    │
│ i64 │ str   │ str    │
╞═════╪═══════╪════════╡
│ 1   │ a     │ x      │
│ 2   │ b     │ y      │
└─────┴───────┴────────┘
```



<h3 id="XRPqV">`append`</h3>
对于Series，你可以使用`append`。它会保留第一个Series的名称。

```python
s1 = pl.Series("a", [1, 2])
s2 = pl.Series("b", [3, 4])
s1.append(s2)
```

```plain
shape: (4,)
Series: 'a' [i64]
[
	1
	2
	3
	4
]
```



<h3 id="rlM7q">`extend`</h3>
第三种组合DataFrame的方式是`extend`。当原始DataFrame后面有足够的可用内存时，`extend`会将第二个DataFrame的数据复制到第一个DataFrame后面。这消除了将数据复制到新位置的需求，这样可以更快，并且仍然保持数据在内存中的连续性。这在你想将较小的DataFrame添加到较大的DataFrame时效果最佳。注意，`extend`会直接修改DataFrame！它也会返回结果DataFrame，但仅作为一个方便的返回。

```python
df1 = pl.DataFrame({
    "id": [1, 2],
    "value": ["a", "b"],
})
df2 = pl.DataFrame({
    "id": [3, 4],
    "value": ["c", "d"],
})
df1.extend(df2)
```

```plain
shape: (4, 2)
┌─────┬───────┐
│ id  │ value │
│ --- │ ---   │
│ i64 │ str   │
╞═════╪═══════╡
│ 1   │ a     │
│ 2   │ b     │
│ 3   │ c     │
│ 4   │ d     │
└─────┴───────┘
```



<h2 id="Yn1K6">本章小结</h2>
在本章中，你学习了如何组合DataFrame。

你可以使用`join`根据DataFrame中的值以及这里概述的策略来组合DataFrame。你可以通过`tolerance`和`by`参数对连接进行微调。

`join_asof`是一种特殊的连接，它根据另一个DataFrame中最近的值来连接DataFrame。

`concat`不考虑值来组合DataFrame，具有多种策略来组合DataFrame，你可以通过`rechunk`参数优化结果DataFrame的性能。

`vstack`和`hstack`分别垂直和水平堆叠DataFrame，而`append`通过创建包含多个块的新DataFrame来追加Series，这种方式速度快，但查询时性能较差。

`extend`通过将数据复制到原始DataFrame后面，快速地将一个DataFrame添加到另一个DataFrame中，这比将数据复制到新位置更快。

在下一章中，我们将探讨如何重新塑造DataFrame，这在你想要更改数据结构时非常有用。

