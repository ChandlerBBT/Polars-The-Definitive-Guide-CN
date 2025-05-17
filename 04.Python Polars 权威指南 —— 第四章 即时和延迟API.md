

在本章中，我们探讨了Polars应用编程接口（API）的两种不同类型：即时（eager）API和延迟（lazy）API。每种API针对特定的使用场景，并具有独特的性能特征。理解这些API对于有效利用Polars的数据处理和分析功能至关重要。

即时API使用立即执行模型。每个函数按顺序执行，数据操作实时发生。这种模型非常适合数据探索和迭代分析任务，因为它允许每一步直接与数据交互。用户执行函数后会立即得到反馈，这对于后续查询的决策非常有价值。这种执行风格与Pandas类似，非常适合熟悉Pandas工作流程的用户过渡。

相比之下，延迟API推迟数据转换的执行，直到必要时才进行。通过这种延迟执行，Polars能够全面优化查询，尤其在大规模和对性能敏感的场景中提升性能。

理解这些API的细微差别以及它们的优化策略，对于充分发挥Polars在数据分析和操作中的潜力至关重要。在本章结束时，您将掌握选择适合您需求的API的知识，并在数据科学项目中有效使用它们。



<h2 id="KmQX4">即时API：DataFrame</h2>
即时API在Polars中基于立即执行模型运作，每个函数逐行顺序执行数据集上的操作。对于数据探索和迭代分析特别有效，因为它允许每个步骤直接与数据交互。用户执行操作后，立即得到反馈和见解，使得后续查询的决策更明智。此执行风格与Pandas类似，非常适合那些熟悉Pandas或从Pandas过渡的用户。

在这个示例中，我们通过一个实际的应用探索Polars的即时API。我们有一个出租车行程数据集，目标是分析数据，找到按每公里收入计算的前3名供应商。让我们逐步分解该过程，了解即时API如何促进这一分析。请注意，我们使用了`%%time`魔法命令来计时并打印代码执行所需的时间。

```python
import polars as pl

%%time
trips = pl.read_parquet("data/taxi/yellow_tripdata_*.parquet") ①
sum_per_vendor = trips.group_by("VendorID").sum() ②

income_per_distance_per_vendor = sum_per_vendor.select(
    "VendorID",
    income_per_distance=pl.col("total_amount") / pl.col("trip_distance"),
)

top_three = ( ③
    income_per_distance_per_vendor.sort(
        by="income_per_distance",
        descending=True
    )
    .head(3)
)
top_three
```

```plain
CPU times: user 9.45 s, sys: 8.7 s, total: 18.1 s
Wall time: 8.52 s
shape: (3, 2)
┌──────────┬─────────────────────┐
│ VendorID │ income_per_distance │
│ ---      │ ---                 │
│ i64      │ f64                 │
╞══════════╪═════════════════════╡
│ 1        │ 6.434789            │
│ 6        │ 5.296493            │
│ 5        │ 4.731557            │
└──────────┴─────────────────────┘
```

① 这段代码读取所有符合通配模式的Parquet文件。通配模式是一种字符串定义，用于指定通过匹配模式术语的一组文件名。在第5章中，我们将深入探讨如何读取和写入数据。现在，只需知道数据集由多个文件组成，Polars会将这些文件一次性读取到`DataFrame`中。`read_parquet()`函数返回的`DataFrame`使用的是即时API。

② 所有列根据`VendorID`汇总，便于计算总金额。

③ 从这些汇总中，您可以计算每公里的平均收入，按供应商区分每次行程。

在对数据排序后，您可以选择前三名，回答我们之前的问题：“按每公里收入计算，排名前3的供应商是谁？”

执行这种分析时，通常最好将主要问题拆分为较小的部分。这样可以逐步查看数据，帮助您为后续步骤做出更好的选择。



<h2 id="VDnwz">延迟API：LazyFrame</h2>
延迟API推迟执行所有选择、过滤和操作，直到实际需要时才执行。这样查询引擎可以获取更多关于实际需要的数据和转换信息，从而进行大量优化，显著提高性能。我们将在后面讨论这些优化。延迟API的最佳使用场景包括大规模和复杂数据集，以及在速度至关重要的性能敏感型应用中。

接下来我们将讨论查询规划器应用于延迟查询的一些主要优化。这些优化使延迟API成为这些场景的绝佳选择。

---

<h3 id="UzW80">LazyFrame 扫描级别优化</h3>
第一组优化考虑了在扫描阶段加载数据的过程。扫描级别是Polars从数据源读取数据的执行层。这些优化的目标是完全避免读取不会被使用的数据。

**投影下推**（Projection pushdown）意味着通过尽可能早地移动列选择来优化查询。这可以防止未使用的列被读入内存。

在这个例子中，我们将使用与即时API相同的数据集。但这次我们将使用延迟API，试图找出按每公里收入计算排名前3的供应商：

```python
lf = pl.scan_parquet("data/taxi/yellow_tripdata_*.parquet") ①
lf.select(pl.col("trip_distance")).show_graph() ②
```

① `scan_parquet()`不会立即从磁盘读取文件。相反，它返回一个`LazyFrame`，其中只扫描相关的元数据。元数据包含了诸如架构、行数和列数等信息。`LazyFrame`暴露了Polars的延迟API。提供的方法实际上与即时API几乎相同，区别在于它们只会在调用`.collect()`时才执行。

② 这只选择了`trip_distance`列，并使用`show_graph()`打印查询计划。因此，您可以查看查询引擎中的具体操作。您可以看到大约只有1/19的列将被读入内存。

![图 4-1 扫描出租车数据集并选择单列时生成的查询计划](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728624408652-c3ca1fad-4a06-4837-b5f8-92332909093b.png)

**查询计划**的解释如下：

+ 第一个执行的步骤在底部，因此应该从下往上读取图表。
+ 每个框对应查询计划中的一个阶段。
+ `σ`表示选择，并指示任何行过滤条件。
+ `π`表示投影，并指示选择的列子集。

在图4-1中，您可以看到`π`包含从19个可用列中选择的1个。在图4-2中，您可以看到`σ`包含`trip_distance`列上的过滤条件。

接下来我们将讨论下一个优化——谓词下推（predicate pushdown）。它类似于投影下推，但专注于过滤行，而不是选择列。这有助于避免读取不需要的行。

```python
lf.filter(pl.col("trip_distance") > 10).show_graph()
```

![图 4-2. 对 trip_distance 列中的值进行过滤时生成的查询计划。](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728624463515-f4a0fc32-e9eb-4e35-b2a0-0b570de79178.png)



代码对 `trip_distance` 列中的值大于10的行进行过滤。在图 **4-2** 中，你可以看到 `σ` 后面的过滤器。这个过滤器将按行应用。

最后一个优化是 **切片下推(**_**<font style="color:rgb(61, 59, 73);">slice pushdown</font>**_**<font style="color:rgb(61, 59, 73);">)</font>**，它只从扫描级别（即数据读取到内存时）加载所需的数据切片。类似于谓词下推，切片下推可以避免读取不需要的行，但不同的是，它是根据数据块的归属来读取行，而不是根据过滤条件。可以通过以下命令实现：

```python
lf.fetch(n_rows=2)
```

输出结果：

```plain
shape: (2, 19)

┌─────────┬───────────────┬───────────────┬──────┬──────────────┬─────────────┐
│ VendorID │ tpep_pickup   │ tpep_dropoff │ ...  │ total_amount │ congestion  │
│         │ p_datetime[n] │ ff_datetime[n]│ ...  │     f64      │  surcharge  │
├─────────┼───────────────┼───────────────┼──────┼──────────────┼─────────────┤
│ 1       │ 2022-01-01    │ 2022-01-01    │ ...  │    21.95     │     2.5     │
│         │ 00:35:40      │ 00:53:29      │ ...  │              │             │
├─────────┼───────────────┼───────────────┼──────┼──────────────┼─────────────┤
│ 1       │ 2022-01-01    │ 2022-01-01    │ ...  │    13.3      │     0.0     │
│         │ 00:33:43      │ 00:42:07      │ ...  │              │             │
└─────────┴───────────────┴───────────────┴──────┴──────────────┴─────────────┘
```

这个操作只获取了数据的前两行，并在扫描级别收集数据，返回结果为`DataFrame`。

方法 `fetch(10)` 和 `head(10)` 类似但不相同。`fetch(nrows: int)` 方法将在扫描级别加载前 `n_rows` 行，而 `head(nrows: int)` 是在最后应用的。这意味着在使用 `fetch()` 时，查询计划中的任何聚合操作将显示与完全运行时截然不同的结果。

另一方面，`head()` 运行完整的计算，并且只提取最后的结果。最好使用 `fetch()` 来快速测试查询计划是否运行，而 `head()` 可用于过滤出计算得出的前几行结果。

这些下推完全避免了在不必要的数据上执行后续转换操作。



<h3 id="g5zmC">其他优化</h3>
其他优化更注重高效计算。为了演示这一点，我们创建一个小的`LazyFrame`作为示例。

```python
lazy_df = pl.LazyFrame({
    "foo": [1, 2, 3, 4, 5],
    "bar": [6, 7, 8, 9, 10]
})
```

其中一个优化是**公共子计划消除**。子计划（subplan）或子树（subtree）是查询计划中的一组步骤。当某些操作或文件扫描在查询计划的多个子树中使用时，结果将被缓存，以便轻松重复使用。例如：

```python
common_subplan = lazy_df.with_columns(pl.col("foo") * 2)

# 在两个不同的表达式中使用公共子计划
expr1 = common_subplan.filter(pl.col("foo") * 2 > 4)
expr2 = common_subplan.filter(pl.col("foo") * 2 < 8)

result = pl.concat([expr1, expr2])

result.show_graph(optimized=False)
result.show_graph()
```

![图 4-3. 未优化的查询计划](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728624754542-2a84d16d-909b-44e1-9ac4-ed6faf91bd78.png)

![图 4-4. 优化后的查询计划](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728624775425-f8cb704e-af90-4ca3-bf67-5d75ec6e47d8.png)



在这里，你通过`pl.concat()`方法将共享公共子计划的表达式结合起来。图4-3显示了如果没有优化的查询，图4-4则显示了优化后的版本。在优化后的版本中，文件只读取一次，且只选择了`foo`列，而未优化的版本中文件被读取了两次。之后，应用了不同的过滤器。

---

**<font style="color:#1DC0C9;">注意</font>**

在许多情况下，即时API实际上是在底层调用延迟API并立即收集结果。这带来了一个好处，即查询计划器仍然可以在查询中进行优化。此外，这对维护者来说更容易，因为即时API方法是延迟API的一个轻量封装，去除了重复代码。

---

此外，延迟API可以在处理数据前捕获架构错误。查询计划包含了每一步需要进行的操作信息，以及结果的预期样子。

接下来是一个示例。你将创建一个包含三个人名和年龄的`LazyFrame`。如果你将包含`int`数据类型的`age`列当作`str`处理，你会立刻在执行任何计算前遇到`SchemaError`错误。

```python
ldf = pl.LazyFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})

erroneous_query = ldf.with_columns(
    pl.col("age").str.slice(1,3).alias("sliced_age")
)

result_df = erroneous_query.collect()
```

```plain
SchemaError: 无效的序列数据类型：预期为String，但得到的是i64。
```

这种机制允许查询快速失败并提供简短的反馈循环，从而提高编程效率。如果你处理的是大型数据集并运行了耗时的查询，遇到这种错误可能需要花费数小时。这个即时反馈可以为你节省大量时间！



<h2 id="I5xcu">性能差异</h2>
我们建议你尝试使用即时和延迟API执行相同的查询。这是很好的一种方式，可以直观地了解深层次的优化效果。让我们回顾一下早先在出租车行程数据集上的即时查询，该数据集以Parquet格式存储：

```python
%%time
trips = pl.scan_parquet("data/taxi/yellow_tripdata_*.parquet")
sum_per_vendor = trips.group_by("VendorID").sum()
income_per_distance_per_vendor = sum_per_vendor.select(
    "VendorID",
    income_per_distance=pl.col("total_amount") / pl.col("trip_distance")
)
top_three = income_per_distance_per_vendor.sort(
    by="income_per_distance",
    descending=True
).head(3)
top_three.collect()
```

```plain
CPU times: user 2.01 s, sys: 301 ms, total: 2.31 s
Wall time: 592 ms
shape: (3, 2)
┌──────────┬────────────────────┐
│ VendorID │ income_per_distance│
│ ---      │ ---                │
│ i64      │ f64                │
├──────────┼────────────────────┤
│ 1        │ 6.434789           │
│ 6        │ 5.296493           │
│ 5        │ 4.731557           │
└──────────┴────────────────────┘
```

结果返回了相同的`DataFrame`，但延迟API的速度是即时API的10倍左右！这就是我们所谓的惊人的速度。

在Polars中，`LazyFrame`只有在你调用`collect()`方法时才会被评估并转换为`DataFrame`。虽然这种延迟评估带来了效率提升，但必须注意到，后续调用`collect()`将会重新计算`LazyFrame`，这意味着相同的计算将会多次执行，因此需要避免。

我们将创建一个小的`LazyFrame`，包含两列，每列有三行，并将其模拟为一个计算时间较长的大型数据集。

```python
lf = pl.LazyFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
# 一些耗时的计算
print(lf.collect())
print(lf.with_columns(pl.col("col1") + 1).collect())  # 重新计算
```

输出：

```plain
shape: (3, 2)
┌─────┬─────┐
│ col1│ col2│
│ --- │ --- │
│ i64 │ i64 │
├─────┼─────┤
│ 1   │ 4   │
│ 2   │ 5   │
│ 3   │ 6   │
└─────┴─────┘

shape: (3, 2)
┌─────┬─────┐
│ col1│ col2│
│ --- │ --- │
│ i64 │ i64 │
├─────┼─────┤
│ 2   │ 4   │
│ 3   │ 5   │
│ 4   │ 6   │
└─────┴─────┘
```



<h2 id="FtJvn">功能差异</h2>
`LazyFrame` 和 `DataFrame` 之间的最大区别在于，`LazyFrame` 中的数据在收集之前是不可用的。这意味着某些功能将无法使用。我们将逐步介绍不同类型的操作并指出差异。

<h3 id="Ma1Ml">聚合(<font style="color:rgb(61, 59, 73);">Aggregations)</font></h3>
所有的聚合操作（如获取列的平均值、最小值和最大值）既可以应用于`DataFrame`，也可以应用于`LazyFrame`。这些操作不需要查询计划器预先了解数据，并将在数据收集时添加到查询计划中执行。只有`DataFrame`能够使用的一组方法是水平聚合，如表 4-1 所示。水平聚合是按行跨列应用的操作，例如 `sum_horizontal()`。



表 4-1. DataFrame 与 LazyFrame 的聚合方法

| 方法 | DataFrame | LazyFrame |
| --- | --- | --- |
| `.max()` | ✓ | ✓ |
| `.max_horizontal()` | ✓ |  |
| `.mean()` | ✓ | ✓ |
| `.mean_horizontal()` | ✓ |  |
| `.median()` | ✓ | ✓ |
| `.min()` | ✓ | ✓ |
| `.min_horizontal()` | ✓ |  |
| `.null_count()` | ✓ | ✓ |
| `.product()` | ✓ | ✓ |
| `.quantile(…)` | ✓ | ✓ |
| `.std(…)` | ✓ | ✓ |
| `.sum()` | ✓ | ✓ |
| `.sum_horizontal(…)` | ✓ |  |
| `.var(…)` | ✓ | ✓ |




<h3 id="pHr8T">属性(<font style="color:rgb(61, 59, 73);">Attributes)</font></h3>
在所有可用于`DataFrame`的属性中，`LazyFrame`缺少`shape`、`height`和`flags`，如表4-2所示。前两个属性描述了`DataFrame`的列数和行数，这些信息只有在数据可用时才能给出。`flags`是一个包含指示信息的字典，如某列是否已排序，供内部优化使用。



表 4-2. DataFrame 与 LazyFrame 的属性对比

| 属性 | DataFrame | LazyFrame |
| --- | --- | --- |
| `.columns` | ✓ | ✓ |
| `.dtypes` | ✓ | ✓ |
| `.flags` | ✓ |  |
| `.height` | ✓ |  |
| `.schema` | ✓ | ✓ |
| `.shape` | ✓ |  |
| `.width` | ✓ | ✓ |


<h3 id="Ttr6Q">计算(<font style="color:rgb(61, 59, 73);">Computation)</font></h3>
`DataFrame`拥有计算方法 `fold()` 和 `hash_rows()`，而`LazyFrame`根本没有任何计算方法。这两种计算都是按行的归约操作。`fold()`允许你提供一个将两个`Series`归约为一个的函数，`hash_rows()`则将行中的所有信息散列为一个`UInt64`值。



<h3 id="ctGpl">描述性方法(<font style="color:rgb(61, 59, 73);">Descriptive)</font></h3>
`LazyFrame`仅有的描述性方法是`explain()`和`show_graph()`，它们用于展示查询计划，如表4-3所示。而`DataFrame`有许多方法可以展示数据的具体信息，如`describe()`和`estimated_size()`。



表 4-3. DataFrame 与 LazyFrame 的描述性方法

| 方法 | DataFrame | LazyFrame |
| --- | --- | --- |
| `.approx_n_unique()` | ✓ |  |
| `.describe(…)` | ✓ |  |
| `.estimated_size(…)` | ✓ |  |
| `.explain(…)` |  | ✓ |
| `.glimpse()` | ✓ |  |
| `.is_duplicated()` | ✓ |  |
| `.is_empty()` | ✓ |  |
| `.is_unique()` | ✓ |  |
| `.n_chunks()` | ✓ |  |
| `.n_unique(…)` | ✓ |  |
| `.show_graph(…)` |  | ✓ |




<h3 id="udo0Z">分组方法(GroupBy)</h3>
在`GroupBy`上下文中可以应用于组的所有方法在`DataFrame`和`LazyFrame`中是相同的，唯一的区别是`DataFrame`允许遍历组，如表4-4所示。



表 4-4. DataFrame 与 LazyFrame 的 GroupBy 方法

| 方法 | DataFrame | LazyFrame |
| --- | --- | --- |
| `.__iter__()` | ✓ |  |
| `.agg(…)` | ✓ | ✓ |
| `.all()` | ✓ | ✓ |
| `.apply(…)` | ✓ | ✓ |
| `.count()` | ✓ | ✓ |
| `.first()` | ✓ | ✓ |
| `.head(…)` | ✓ | ✓ |
| `.last()` | ✓ | ✓ |
| `.map_groups(…)` | ✓ | ✓ |
| `.max()` | ✓ | ✓ |
| `.mean()` | ✓ | ✓ |
| `.median()` | ✓ | ✓ |
| `.min()` | ✓ | ✓ |
| `.n_unique()` | ✓ | ✓ |
| `.quantile(…)` | ✓ | ✓ |
| `.sum()` | ✓ | ✓ |
| `.tail(…)` | ✓ | ✓ |




<h3 id="yyDGy">导出(<font style="color:rgb(61, 59, 73);">Exporting)</font></h3>
`DataFrame`有多种将数据导出到不同格式的选项。这些格式包括Arrow、Numpy、Pandas、字典、包含结构的`Series`，甚至包括一个包含初始化`DataFrame`所需的Python代码的字符串。由于`LazyFrame`没有任何数据，因此无法进行导出操作。



<h3 id="lte4c">操作与选择(<font style="color:rgb(61, 59, 73);">Manipulation and Selection)</font></h3>
操作（manipulation）和选择（selection）方法是最重要的功能之一。它们包含了数据操作的核心功能。



表4-5展示了两个API之间在这些方面的众多差异。

| 方法 | DataFrame | LazyFrame |
| --- | --- | --- |
| `<font style="background-color:rgb(238, 242, 246) !important;">.approx_n_unique()</font>` |  | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.bottom_k(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.cast(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.clear(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.clone()</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.drop(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.drop_in_place(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.drop_nulls(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.explode(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.extend(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.fill_nan(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.fill_null(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.filter(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.find_idx_by_name(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.first()</font>` |  | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.gather_every(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.get_column(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.get_column_index(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.get_columns()</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.group_by(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.group_by_dynamic(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.group_by_rolling(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.head(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.hstack(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.insert_at_idx(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.insert_column(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.inspect(…)</font>` |  | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.interpolate()</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.item(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.iter_columns()</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.iter_rows()</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.iter_slices(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.join(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.join_asof(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.last()</font>` |  | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.limit(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.melt(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.merge_sorted(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.partition_by()</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.pipe(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.pivot(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.rechunk()</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.rename(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.replace(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.replace_at_idx(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.replace_column(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.reverse()</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.rolling(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.row()</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.rows()</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.rows_by_key(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.sample(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.select(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.select_seq(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.set_sorted(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.shift(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.shift_and_fill(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.shrink_to_fit(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.slice(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.sort(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.tail(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.take_every(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.top_k(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.to_dummies(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.to_series(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.transpose(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.unique(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.unnest(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.unstack(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.update(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.upsample(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(255, 255, 255) !important;">.vstack(…)</font>` | ✓ |  |
| `<font style="background-color:rgb(238, 242, 246) !important;">.with_columns(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.with_columns_seq(…)</font>` | ✓ | ✓ |
| `<font style="background-color:rgb(238, 242, 246) !important;">.with_context(…)</font>` |  | ✓ |
| `<font style="background-color:rgb(255, 255, 255) !important;">.with_row_count(…)</font>` | ✓ | ✓ |


<h3 id="NFDUM">其他方法(<font style="color:rgb(61, 59, 73);">Miscellaneous)</font></h3>
其他方法是那些不属于任何其他类别的方法，如表 4-6 所示。



表 4-6. DataFrame 与 LazyFrame 的其他方法

| 方法 | DataFrame | LazyFrame |
| --- | --- | --- |
| `.cache()` |  | ✓ |
| `.collect(…)` |  | ✓ |
| `.collect_async()` |  | ✓ |
| `.corr(…)` | ✓ |  |
| `.equals(…)` | ✓ |  |
| `.fetch(…)` | ✓ |  |
| `.frame_equal(…)` | ✓ |  |
| `.lazy()` |  | ✓ |
| `.map(…)` | ✓ | ✓ |
| `.map_batches(…)` |  | ✓ |
| `.map_rows(…)` | ✓ |  |
| `.pipe(…)` | ✓ | ✓ |
| `.profile(…)` |  | ✓ |




<h2 id="LoJHR">使用 Lazy API 的流模式进行内存外计算</h2>
Lazy API 提供了一种特殊模式用于进行内存外（out-of-core）计算，即处理那些过大而无法装入内存的数据，通过对数据块进行计算来代替一次性加载全部数据。支持内存外计算的神奇之处在于，它将数据处理的限制从你的内存大小转移到了硬盘的大小，而这两者的大小可能相差几个数量级！你可以通过将 `streaming=True` 传递给 `collect()` 函数来触发该模式，将最终结果收集到内存，或者可以将结果写入磁盘，使用 `.sink_csv(…)`、`.sink_ipc(…)` 或 `.sink_parquet(…)`。如果使用 `.collect(streaming=True)`，则最终结果必须适合内存。

在流模式下，API 以行块（chunks）的方式读取数据。行块大小是根据可用线程数和数据集的列数来确定的。

如何查看系统上可用的线程数？你需要知道默认情况下（或你正在工作的容器中）计算机上可用的逻辑 CPU 核心数。通过运行以下代码，你可以找到这个数字：

```python
pl.thread_pool_size()
```

输出：

```plain
12
```

虽然这个数字通常是默认的最佳值，但可以通过环境变量增加或减少线程数。如果同时运行其他 CPU 密集型任务，而系统需要一些喘息空间以防止其他进程超时，这将非常有用。你必须在导入 Polars 之前设置此环境变量，代码示例如下：

```python
import os
os.environ["POLARS_MAX_THREADS"] = "2"
import polars as pl
```

---

**<font style="color:#1DC0C9;">注意</font>****：** 

理解 Python 环境变量。

---



**示例 4-1**

在 Python 中，环境变量是可以在运行时环境中设置和读取的键值对。它们在以下几个方面非常有用：

1. **安全性**：它们提供了一种安全的方式来存储敏感信息，如数据库凭证和 API 密钥，将它们从源码中分离出来。
2. **配置**：环境变量允许你在不修改代码的情况下更改 Python 应用程序的行为。例如，你可以设置环境变量来区分开发环境和生产环境。
3. **可移植性**：通过使用环境变量，你可以轻松地在不同环境之间迁移应用程序（本地、暂存、生产）而无需代码修改。

在 Python 中，你可以使用 `os` 模块访问环境变量，特别是通过 `os.environ`。它的行为类似于字典，你可以通过键来获取值。例如，`os.environ['POLARS_MAX_THREADS']` 将返回 Polars 允许使用的线程数。

****

**拆解：**

如果你有12个或更多的线程，`thread_factor` 将为1；如果线程较少，`thread_factor` 将大于1。这意味着线程数越多，`thread_factor` 的值越小。



$ \text{thread_factor}=max\left\{\frac{12}{\text{n_threads},1}\right\}\\

\text{chunk_size} =max\left\{\frac{50000}{\text{n - cols *thread - factor}},1000\right\}
\\\ $



![图 4-5. Thread factor](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728625935145-4a94d96a-a614-4c7e-87d7-dfcfb6000c12.png)



这段代码将块大小设置为最大值1000，或者为 `50000 / n_cols * thread_factor`。

块大小随着线程因子增加而增加，当可用线程较少时会发生这种情况。这意味着如果有更多可用线程，块大小将会缩小。这样做的目的是在同一时间处理更多的数据块，但使用更多的内存。

如果数据集中有更多的列，块大小也会减小，因为每一行包含更多数据（因此使用更多内存）。

然而，可以覆盖流式块大小的默认值。如果 Polars 确定的默认块大小仍然导致内存问题，这可能是必要的。你可以通过以下配置设置来执行此操作：

```python
pl.Config.set_streaming_chunk_size(1000)
```

<h2 id="rB4CO">提示和技巧</h2>
在接下来的部分中，我们将介绍一些使用 Polars 时的提示和技巧。这些技巧在日常使用中非常实用，通常这些信息不会出现在官方文档中，但却能让你的工作更轻松。

<h3 id="JpA9m">从 LazyFrame 到 DataFrame 以及反向转换</h3>
![图 4-6. 切换到另一 API 的操作](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728626142273-a5fc8388-6818-47ab-8015-53fc71cb1b88.png)

你可以通过一个简单的命令在两个API之间切换，如图 4-6 所示。

你可以通过在 `DataFrame` 或返回 `DataFrame` 的方法后面添加 `.lazy()`，从即时API切换到延迟API。这不会进行任何计算，但告诉查询计划器使用内存中的数据作为新的查询计划的起点。

你可以通过调用 `collect()` 或者返回`LazyFrame`的方法，从延迟API切换到即时API。这样会执行构建的查询计划，触发计算，结果将存储在内存中。如果你使用流模式并没有调用 `.collect()`，可以调用 `.sink_parquet()` 将结果写入磁盘。



<h3 id="FbkmK">连接 DataFrame 和 LazyFrame</h3>
在 Polars 中执行连接时，参与的数据结构必须是相同类型。具体来说，你不能直接将一个`DataFrame`与`LazyFrame`连接。假设你有一个包含元数据的小`DataFrame`，想要将其连接到一个大型的`LazyFrame`数据集上，直接连接会报错：

```python
lf = pl.LazyFrame({"id": [1,2,3], "value1": [4,5,6]})
df = pl.DataFrame({"id": [1,2,3], "value2": [7,8,9]})

lf.join(df, on="id")
```

报错：

```plain
TypeError: expected `other` join table to be a LazyFrame, not a 'DataFrame'
```

幸运的是，解决方法非常简单。你可以将`DataFrame`转换为延迟模式 `.lazy()`，或者将`LazyFrame`物化为`DataFrame`通过 `.collect()`。建议使用延迟API以获得更好的性能和效率。

正确的连接方式如下：

```python
lf = pl.LazyFrame({"id": [1,2,3], "value1": [4,5,6]})
df = pl.DataFrame({"id": [1,2,3], "value2": [7,8,9]})

lf.join(df.lazy(), on="id")
```

---

<h3 id="aloOH">缓存中间阶段</h3>
为了避免不必要的重新计算，你可以通过在复杂计算后添加 `.collect().lazy()` 来缓存 `LazyFrame` 到内存中。这将评估 `LazyFrame`，并将结果存储在内存中，返回一个新的 `LazyFrame`，指向存储在RAM中的数据。

示例：

```python
lf = pl.LazyFrame({"col1": [1,2,3], "col2": [4,5,6]})
# 一些耗时的计算
lf = lf.collect().lazy()
print(lf.collect())
print(lf.with_columns(pl.col("col1") + 1).collect())  # 利用缓存
```

输出：

```plain
shape: (3, 2)
┌─────┬─────┐
│ col1│ col2│
│ --- │ --- │
│ i64 │ i64 │
├─────┼─────┤
│ 1   │ 4   │
│ 2   │ 5   │
│ 3   │ 6   │
└─────┴─────┘

shape: (3, 2)
┌─────┬─────┐
│ col1│ col2│
│ --- │ --- │
│ i64 │ i64 │
├─────┼─────┤
│ 2   │ 4   │
│ 3   │ 5   │
│ 4   │ 6   │
└─────┴─────┘
```

这种模式在资源密集型计算中非常有用，它让你可以利用延迟计算的优势，同时减轻计算上的缺点。

---

<h2 id="px1kT">结论</h2>
在本章中，我们讨论了 Polars 中的即时和延迟API。你学到了：

+ 即时API及其在 Polars 中作为`DataFrame`的表示。
+ 延迟API及其在 Polars 中作为`LazyFrame`的表示。
+ 各个API的最佳使用场景。
+ 延迟API中的优化。
+ 即时和延迟API之间的功能差异。
+ 延迟API的流模式，它允许对超出内存大小的数据进行内存外计算。
+ 一些实用的技巧，如如何使用缓存避免多次计算相同的`LazyFrame`，以及如何连接`DataFrame`和`LazyFrame`。

通过这些知识，你可以确定哪个API最适合你的用例。

