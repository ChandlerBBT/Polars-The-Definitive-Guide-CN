

本章的目标是介绍表达式，这是使Polars API如此强大和优雅的原因所在。本章构成了第二部分其余章节的基础，在这些章节中，我们将更详细地讨论特定表达式及其使用方法。

---

**<font style="color:#ECAA04;">Polars表达式与正则表达式</font>**

Polars表达式不应与正则表达式混淆。正则表达式，或regex，是用于匹配文本的字符序列。例如，正则表达式 `[Pp](ol|and)ar?s` 可以匹配 pandas 和 Polars，但不能匹配 panda 或 polaris。一些Polars方法确实接受正则表达式，例如用于选择列的 `pl.col()` 和用于替换值的 `Expr.str.replace()`。Grant Skinner 的交互式网站 [RegExr](https://regexr.com/) 和 Michael FitzGerald 的书籍 _Introducing Regular Expressions_ 是学习更多关于正则表达式的有用资源。

---

表达式，在Polars中，是可重用的构建块，使您能够执行许多数据处理任务，包括选择现有列、创建新列、根据条件筛选行以及计算聚合。简而言之，它们无处不在。

表达式的应用如此广泛，以至于我们将它们分成三个章节，如图6-1所示。在第13章中，我们将介绍可以通过命名空间（在下一节中解释）访问的各种方法。

![图6-1 多种表达方法被分到3个章节](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728788776888-47ab6feb-c371-4585-afd1-455fe64e70fb.png)



在本章您将学习：

+ 什么是表达式
+ 表达式可以应用在哪里
+ 如何从现有列创建表达式
+ 如何从文字值创建表达式
+ 如何从范围创建表达式
+ 如何重命名表达式
+ 为什么表达式是使用Polars的推荐方式

之后，在第7章和第8章中，您将学习如何扩展表达式以及如何组合它们。

---

<h2 id="GrZfS">**方法和命名空间**</h2>
`pl.Expr` 类，代表一个Polars表达式，截至本文撰写时有大约350个方法。超过100个表达式方法可以通过命名空间访问：每个命名空间都处理特定数据类型的方法组。

例如，`Expr.str` 命名空间有处理字符串的方法，`Expr.dt` 命名空间有处理时间值的方法，`Expr.cat` 命名空间有处理类别的方法。这些类型及其相关方法将在第10章中介绍。在本章中，我们将关注表达式的基础知识和更一般的方法。



<h2 id="xOALr">**通过示例学习表达式**</h2>
表达式在应用时真正展现其价值。这可能听起来很显然，但表达式本身什么都不做。它们是延迟执行的，正如LazyFrames一样。在实际操作中，表达式通过将它们作为参数传递给某些DataFrame或LazyFrame方法来应用。

在我们深入讨论表达式的细节之前，我们将通过一些示例展示表达式的用法：

+ 使用 `df.select()` 方法选择列
+ 使用 `df.with_columns()` 方法创建新列
+ 使用 `df.filter()` 方法筛选行
+ 使用 `df.group_by()` 方法进行聚合
+ 使用 `df.sort()` 方法排序行



当你浏览这些示例时，请记住，关键在于表达式，而不是方法。每种方法将在各自的章节中得到更详细的介绍。

我们将使用以下关于来自世界各地的10种美味水果的DataFrame：

```python
import polars as pl

fruit = pl.read_csv("data/fruit.csv")
fruit
```

```plain
shape: (10, 5)
┌────────────┬────────┬────────┬──────────┬───────────────┐
│ name       │ weight │ color  │ is_round │ origin        │
│ ---        │ ---    │ ---    │ ---      │ ---           │
│ str        │ i64    │ str    │ bool     │ str           │
╞════════════╪════════╪════════╪══════════╪═══════════════╡
│ Avocado    │ 200    │ green  │ false    │ South America │
│ Banana     │ 120    │ yellow │ false    │ Asia          │
│ Blueberry  │ 1      │ blue   │ false    │ North America │
│ Cantaloupe │ 2500   │ orange │ true     │ Africa        │
│ Cranberry  │ 2      │ red    │ false    │ North America │
│ Elderberry │ 1      │ black  │ false    │ Europe        │
│ Orange     │ 130    │ orange │ true     │ Asia          │
│ Papaya     │ 1000   │ orange │ false    │ South America │
│ Peach      │ 150    │ orange │ true     │ Asia          │
│ Watermelon │ 5000   │ green  │ true     │ Africa        │
└────────────┴────────┴────────┴──────────┴───────────────┘
```



我们下面展示的方法同样适用于LazyFrames，但因为我们只处理10行，DataFrame就足够了。而且，由于不需要使用 `lf.collect()` 来具体化结果，示例可以更简洁。



<h3 id="WxQZH">**使用表达式选择列**</h3>
你可以使用 `df.select()` 方法从DataFrame中选择一个或多个现有列。表达式中未提到的任何列都将从输出中删除。以下代码片段选择了水果的名称、来源地和重量（以千克为单位）：

```python
fruit.select(
    pl.col("name"),  ①
    pl.col("^.*or.*$"),  ②
    pl.col("weight") / 1000,  ③
    "is_round"  ④
)
```

```plain
shape: (10, 5)
┌────────────┬────────┬───────────────┬────────┬──────────┐
│ name       │ color  │ origin        │ weight │ is_round │
│ ---        │ ---    │ ---           │ ---    │ ---      │
│ str        │ str    │ str           │ f64    │ bool     │
╞════════════╪════════╪═══════════════╪════════╪══════════╡
│ Avocado    │ green  │ South America │ 0.2    │ false    │
│ Banana     │ yellow │ Asia          │ 0.12   │ false    │
│ Blueberry  │ blue   │ North America │ 0.001  │ false    │
│ Cantaloupe │ orange │ Africa        │ 2.5    │ true     │
│ Cranberry  │ red    │ North America │ 0.002  │ false    │
│ Elderberry │ black  │ Europe        │ 0.001  │ false    │
│ Orange     │ orange │ Asia          │ 0.13   │ true     │
│ Papaya     │ orange │ South America │ 1.0    │ false    │
│ Peach      │ orange │ Asia          │ 0.15   │ true     │
│ Watermelon │ green  │ Africa        │ 5.0    │ true     │
└────────────┴────────┴───────────────┴────────┴──────────┘
```



① 函数 `pl.col()` 是开始表达式最常用的方式。参数是一个引用现有列的字符串——在此示例中为 `name`。

②`pl.col()` 也接受正则表达式作为参数。此正则表达式匹配两列 `color` 和 `origin`，因为它们的名称都包含字符串“or”。

③ 你可以使用已经熟悉的运算符（加、减、乘、除）对表达式执行算术运算。（我们将在第8章进一步讨论如何执行算术运算。）注意，Polars会自动将 `weight` 列从整数（`i64`）转换为浮点数（`f64`），以便处理小数重量。

④ 方法 `df.select()` 也接受字符串来引用现有列。这可能很方便，因为你可以少打些字。然而，既然字符串不是表达式，你就无法对其执行任何算术运算或其他操作。



<h3 id="jliEg">**使用表达式创建新列**</h3>
使用 `df.with_columns()` 方法，你可以创建一个或多个列，基于现有列或从头开始。在这个示例中，我们为 `fruit` DataFrame 添加了两列：一列表示一个水果是否真的是水果（显然始终为 `True`），另一列表示该水果是否为浆果（基于其名称）：

```python
fruit.with_columns(
    pl.lit(True).alias("is_fruit"),  ①
    pl.col("name").str.ends_with("berry").alias("is_berry")  ②
)
```

```plain
shape: (10, 7)
┌────────────┬────────┬────────┬──────────┬─────────────┬──────────┬──────────┐
│ name       │ weight │ color  │ is_round │ origin      │ is_fruit │ is_berry │
│ ---        │ ---    │ ---    │ ---      │ ---         │ ---      │ ---      │
│ str        │ i64    │ str    │ bool     │ str         │ bool     │ bool     │
╞════════════╪════════╪════════╪══════════╪═════════════╪══════════╪══════════╡
│ Avocado    │ 200    │ green  │ false    │ South Amer… │ true     │ false    │
│ Banana     │ 120    │ yellow │ false    │ Asia        │ true     │ false    │
│ Blueberry  │ 1      │ blue   │ false    │ North Amer… │ true     │ true     │
│ Cantaloupe │ 2500   │ orange │ true     │ Africa      │ true     │ false    │
│ Cranberry  │ 2      │ red    │ false    │ North Amer… │ true     │ true     │
│ Elderberry │ 1      │ black  │ false    │ Europe      │ true     │ true     │
│ Orange     │ 130    │ orange │ true     │ Asia        │ true     │ false    │
│ Papaya     │ 1000   │ orange │ false    │ South Amer… │ true     │ false    │
│ Peach      │ 150    │ orange │ true     │ Asia        │ true     │ false    │
│ Watermelon │ 5000   │ green  │ true     │ Africa      │ true     │ false    │
└────────────┴────────┴────────┴──────────┴─────────────┴──────────┴──────────┘
```

① 使用 `pl.lit()` 函数，可以基于字面值（例如 `True`）启动表达式。方法 `Expr.alias()` 允许你为新列命名并重命名现有列。

② 方法 `Expr.str.ends_with()` 是 `str` 命名空间中的众多字符串方法之一。如前所述，这些将在第11章中介绍。



<h3 id="OdAC5">**使用表达式筛选行**</h3>
要基于表达式筛选行，请使用 `df.filter()` 方法。只有在表达式计算为 `True` 的行才会被保留。此示例只保留圆形且重量超过1000克的水果：

```python
fruit.filter(
    pl.col("is_round") &  ①
    (pl.col("weight") > 1000)  ②
)
```

① 这里我们使用逻辑与（`&`）运算符组合了两个表达式。仅当两个表达式都为 `True` 时，输出才为 `True`。（我们将在第8章讨论逻辑运算符。）

② 可以通过使用比较运算符（例如大于号 `>`）将现有列转换为布尔类型。



<h3 id="AiKDa">**使用表达式进行聚合**</h3>
聚合通常涉及创建行组，然后将每组总结为一行。此示例基于 `origin` 列的最后一部分创建组，然后计算每组的水果数量及其平均重量。请注意，它在两个不同的地方使用了表达式：确定组时，以及总结组时：

```python
fruit.group_by(
    pl.col("origin").str.split(" ").list.last()  ①
).agg(
    pl.count(),  ②
    pl.col("weight").mean().alias("average_weight")  ③
)
```

```plain
shape: (4, 3)
┌─────────┬───────┬────────────────┐
│ origin  │ count │ average_weight │
│ ---     │ ---   │ ---            │
│ str     │ u32   │ f64            │
╞═════════╪═══════╪════════════════╡
│ America │ 4     │ 300.75         │
│ Europe  │ 1     │ 1.0            │
│ Asia    │ 3     │ 133.333333     │
│ Africa  │ 2     │ 3750.0         │
└─────────┴───────┴────────────────┘
```

① 此表达式的每个唯一值（`origin` 列的最后一部分）都对应一个组。

② 由函数 `pl.count()` 创建的表达式返回组中行的数量。

③ 方法 `Expr.mean()` 是用于总结数据的方法之一——将多个值转化为一个。



---

**<font style="color:#1DC0C9;">提示</font>**  
我们不想过多超前讨论，但我们非常兴奋地告诉你，多个表达式是**并行执行**的——在聚合和选择示例中就是如此。这也是 Polars 如此快速的原因之一。

---

****

<h3 id="KLti7">**使用表达式排序行**</h3>
要根据一列或多列重新排列 DataFrame，请使用 `df.sort()` 方法。这个（或许显得刻意的）示例根据名称的长度对水果进行排序：

```python
fruit.sort(
    pl.col("name").str.len_bytes(), ①
    descending=True ②
)
```

```plain
shape: (10, 5)
┌────────────┬────────┬────────┬──────────┬───────────────┐
│ name       │ weight │ color  │ is_round │ origin        │
│ ---        │ ---    │ ---    │ ---      │ ---           │
│ str        │ i64    │ str    │ bool     │ str           │
╞════════════╪════════╪════════╪══════════╪═══════════════╡
│ Cantaloupe │ 2500   │ orange │ true     │ Africa        │
│ Elderberry │ 1      │ black  │ false    │ Europe        │
│ Watermelon │ 5000   │ green  │ true     │ Africa        │
│ Blueberry  │ 1      │ blue   │ false    │ North America │
│ Cranberry  │ 2      │ red    │ false    │ North America │
│ Avocado    │ 200    │ green  │ false    │ South America │
│ Banana     │ 120    │ yellow │ false    │ Asia          │
│ Orange     │ 130    │ orange │ true     │ Asia          │
│ Papaya     │ 1000   │ orange │ false    │ South America │
│ Peach      │ 150    │ orange │ true     │ Asia          │
└────────────┴────────┴────────┴──────────┴───────────────┘
```

① 你可以根据并不存在于 `fruits` DataFrame 中的表达式进行排序。（虽然名称存在，但它们的长度并不存在。）如果只想将其用于排序，则不需要显式添加一个新列。

② 对于升序排列（默认值），请移除此参数或将其设置为 `False`。

---

<h2 id="eE6EY">**表达式究竟是什么？**</h2>
现在你已经看到了表达式的一些具体示例以及它们的应用方式，是时候定义一下表达式到底是什么了。

---

**<font style="color:#1DC0C9;">表达式定义</font>**<font style="color:#1DC0C9;">  
</font>表达式是一棵操作树，用于描述如何构建一个或多个 Series。

---

让我们将这个定义分解为五个部分：

---

**Series**

回想一下第3章，Series 是具有相同数据类型的值的数组，例如64位浮点数的 `pl.Float64` 或字符串的 `pl.String`。你可以将 Series 看作是 DataFrame 中的一列，但请记住，Series 可以单独存在，因此并不总是 DataFrame 的一部分。



**操作树(**_**<font style="color:rgb(61, 59, 73);">Tree of operations</font>**_**<font style="color:rgb(61, 59, 73);">)</font>**

一个表达式可以由以下几种情况组成：一个操作、多个操作组成的线性序列，以及组织成树状结构的多个操作。

图6-2 显示了三个示例表达式。如果这些表达式被执行，它们将产生具有值 7、12 和 6 的三列。注意，图6-2 的第三个图确实是树状的。

![图6-2 表达式是一棵操作树](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728789770192-a923e9a4-9021-4c71-a5e0-6215fbb15ce4.png)

一般来说，所有表达式都是树状结构的，但它们不一定有分支或父节点。



**描述**

表达式仅仅是一个描述；它不会自己构建任何 Series，也没有执行自身的方法。表达式只有在作为参数传递给诸如 `pl.select()` 的函数和诸如 `df.group_by()` 的方法时才会被执行。然后，一个或多个 Series 被构建。

如果你将表达式视为一个食谱，那么操作就是步骤，而函数和方法则相当于厨师。



**构建**

你并不总是能看到正在构建的 Series。构建的 Series 是否成为（或成为）DataFrame 的一部分，将取决于执行表达式的函数或方法。一个不会构建新列的示例是 `pl.filter()` 函数。构建的 Series 仅用于确定应保留 DataFrame 的哪些行；它不会成为一个新列。构建这个词也需要谨慎使用：如果表达式仅由单个引用 DataFrame 中现有列的操作组成，那么实际上并没有构建任何 Series。



**一个或多个**

一个表达式可以描述一个或多个 Series 的构建。例如，函数 `pl.all()` 引用 DataFrame 中的所有 Series。表达式 `pl.all().mul(10).name.suffix("_times_10")` 将所有现有 Series 的值乘以10，并在它们的名称后添加“_times_10”：

```python
(
    pl.DataFrame({"a": [1, 2, 3], "b": [0.4, 0.5, 0.6]})
    .with_columns(pl.all().mul(10).name.suffix("_times_10"))
)
```

```plain
shape: (3, 4)
┌─────┬─────┬────────────┬────────────┐
│ a   │ b   │ a_times_10 │ b_times_10 │
│ --- │ --- │ ---        │ ---        │
│ i64 │ f64 │ i64        │ f64        │
╞═════╪═════╪════════════╪════════════╡
│ 1   │ 0.4 │ 10         │ 4.0        │
│ 2   │ 0.5 │ 20         │ 5.0        │
│ 3   │ 0.6 │ 30         │ 6.0        │
└─────┴─────┴────────────┴────────────┘
```

使用方法 `Expr.meta.has_multiple_outputs()`，你可以检查表达式是否描述了多个 Series 的潜在构建：

```python
pl.all().mul(10).name.suffix("_times_10").meta.has_multiple_outputs()
```

```python
True
```

是否实际构建了多个 Series 取决于所应用的 DataFrame。如果 DataFrame 只有一个 Series，`pl.all()` 只会构建一个 Series。



<h3 id="fG0xb">**表达式的属性**</h3>
知道表达式的定义是一回事，理解它们在实践中如何工作则是另一回事。这里有几个值得一提的表达式属性：



**延迟执行**

表达式是延迟执行的：它们本身不做任何事情。或许它们的最重要属性就是延迟执行，因为没有这个属性，它们就不会有我们即将讨论的其他五个属性。



**依赖于函数和数据**

表达式依赖于执行它们的函数和应用它们的 DataFrame（或 LazyFrame）。函数决定了构建的 Series 会发生什么；DataFrame 决定了 Series 的类型和长度。

为了演示这一点，让我们将相同的表达式（`is_orange`）传递给三种不同的函数（方法），这些方法及其输出如下所示：

```python
is_orange = (pl.col("color") == "orange").alias("is_orange")

fruit.with_columns(is_orange)
```

```plain
shape: (10, 6)
┌────────────┬────────┬────────┬──────────┬───────────────┬───────────┐
│ name       │ weight │ color  │ is_round │ origin        │ is_orange │
│ ---        │ ---    │ ---    │ ---      │ ---           │ ---       │
│ str        │ i64    │ str    │ bool     │ str           │ bool      │
╞════════════╪════════╪════════╪══════════╪═══════════════╪═══════════╡
│ Avocado    │ 200    │ green  │ false    │ South America │ false     │
│ Banana     │ 120    │ yellow │ false    │ Asia          │ false     │
│ Blueberry  │ 1      │ blue   │ false    │ North America │ false     │
│ Cantaloupe │ 2500   │ orange │ true     │ Africa        │ true      │
│ Cranberry  │ 2      │ red    │ false    │ North America │ false     │
│ Elderberry │ 1      │ black  │ false    │ Europe        │ false     │
│ Orange     │ 130    │ orange │ true     │ Asia          │ true      │
│ Papaya     │ 1000   │ orange │ false    │ South America │ true      │
│ Peach      │ 150    │ orange │ true     │ Asia          │ true      │
│ Watermelon │ 5000   │ green  │ true     │ Africa        │ false     │
└────────────┴────────┴────────┴──────────┴───────────────┴───────────┘
```



```python
fruit.filter(is_orange)
```

```plain
shape: (4, 5)
┌────────────┬────────┬────────┬──────────┬───────────────┐
│ name       │ weight │ color  │ is_round │ origin        │
│ ---        │ ---    │ ---    │ ---      │ ---           │
│ str        │ i64    │ str    │ bool     │ str           │
╞════════════╪════════╪════════╪══════════╪═══════════════╡
│ Cantaloupe │ 2500   │ orange │ true     │ Africa        │
│ Orange     │ 130    │ orange │ true     │ Asia          │
│ Papaya     │ 1000   │ orange │ false    │ South America │
│ Peach      │ 150    │ orange │ true     │ Asia          │
└────────────┴────────┴────────┴──────────┴───────────────┘
```



```python
fruit.group_by(is_orange).len()
```

```plain
shape: (2, 2)
┌───────────┬─────┐
│ is_orange │ len │
│ ---       │ --- │
│ bool      │ u32 │
╞═══════════╪═════╡
│ false     │ 6   │
│ true      │ 4   │
└───────────┴─────┘
```

关键是你将使用相同的语法来完成不同的任务。这与表达式的下一个属性——可重用性紧密相关。



**可重用性**

表达式是Python对象。在前面的示例中，我们创建了表达式对象 `is_orange` 并通过将其传递给 `fruit` DataFrame 的不同方法来重用它。更进一步，完全可以将相同的表达式用于完全不同的 DataFrame：

```python
flowers = pl.DataFrame({
    "name": ["Tiger lily", "Blue flag", "African marigold"],
    "latin": ["Lilium columbianum", "Iris versicolor", "Tagetes erecta"],
    "color": ["orange", "purple", "orange"]
})

flowers.filter(is_orange)
```

```plain
shape: (2, 3)

┌───────────────┬─────────────────────┬─────────┐
│ name          │ latin               │ color   │
├───────────────┼─────────────────────┼─────────┤
│ Tiger lily    │ Lilium columbianum   │ orange  │
│ African marigold │ Tagetes erecta   │ orange  │
└───────────────┴─────────────────────┴─────────┘
```



**高效**

因为表达式是延迟执行的，所以你可以在执行之前优化它们。Polars 通过分析表达式中的操作，将构建 Series 所需的计算最小化。此外，当一个函数被赋予多个表达式时，它们是并行执行的。



总之，表达式具有许多有利的属性。让我们继续创建表达式吧。



<h2 id="K6AhN">**创建表达式**</h2>
每个表达式都是从第一个操作开始的。一般来说，新的表达式是使用不依赖于其他表达式的函数创建的。一旦你有了一个表达式，你可以继续通过许多方法构建它，并使用内联运算符与其他表达式组合（将在接下来的两章中讨论）。让我们来看看创建表达式的各种方式，首先从现有列开始。



<h3 id="QEVtw">**从现有列**</h3>
创建表达式最常见的方法是引用 DataFrame 中一个或多个现有列。毕竟，大多数情况下，你想要转换已经拥有的数据。这可以通过函数 `pl.col()` 完成，该函数接受列名、正则表达式和数据类型。以下是几个示例。

为了演示，我们使用方法 `df.select()` 执行表达式，并通过 `df.columns` 属性获取列名列表。你可以通过传递列名来引用特定的列：

```python
fruit.select(pl.col("color")).columns
```

```python
['color']
```

如果 DataFrame 中没有具有该特定名称的列，Polars 会抛出错误：

```python
fruit.select(pl.col("is_smelly")).columns
```

```python
ColumnNotFoundError: is_smelly

Error originated just after this operation:
DF ["name", "weight", "color", "is_round"]; PROJECT */5 COLUMNS; SELECTION: "Non
e"
```



正则表达式在引用具有共同模式的多个列时特别有用。为此，正则表达式必须以插入符号（`^`）开头，并以美元符号（`$`）结尾：

```python
fruit.select(pl.col("^.*or.*$")).columns
```

```python
['color', 'origin']
```



使用 `pl.col("*")` 或方便的别名 `pl.all()`，你可以引用所有列：

```python
fruit.select(pl.all()).columns
```

```python
['name', 'weight', 'color', 'is_round', 'origin']
```



你可以引用特定数据类型的所有列（例如，字符串类型使用 `pl.String`）：

```python
fruit.select(pl.col(pl.String)).columns
```

```python
['name', 'color', 'origin']
```



你可以给 `pl.col()` 多个列名或数据类型：

```python
fruit.select(pl.col(pl.Boolean, pl.Int64)).columns
```

```python
['weight', 'is_round']
```



或者，如果更方便的话，你可以将它们作为列表传递：

```python
fruit.select(pl.col(["name", "color"])).columns
```

```python
['name', 'color']
```



然而，你不能混合列名和数据类型：

```python
fruit.select(pl.col([pl.String, "is_round"])).columns
```

```python
TypeError: argument 'dtypes': 'str' is not a Polars data type
```



---

**<font style="color:#1DC0C9;">引用数值数据类型</font>**

要引用包含数字的所有列，你可以使用常量 `pl.NUMERIC_DTYPES`，它包含所有数值数据类型：

```python
pl.NUMERIC_DTYPES
```

```python
frozenset({Decimal,
           Float32,
           Float64,
           Int16,
           Int32,
           Int64,
           Int8,
           UInt16,
           UInt32,
           UInt64,
           UInt8})
```

你可以直接在 `pl.col()` 中使用这个常量：

```python
(
    fruit
    .with_columns((pl.col("weight") / 1000).alias("weight_kg"))
    .select(pl.col(pl.NUMERIC_DTYPES))
    .head()
)
```

```plain
shape: (5, 2)
┌────────┬───────────┐
│ weight │ weight_kg │
│ ---    │ ---       │
│ i64    │ f64       │
╞════════╪═══════════╡
│ 200    │ 0.2       │
│ 120    │ 0.12      │
│ 1      │ 0.001     │
│ 2500   │ 2.5       │
│ 2      │ 0.002     │
└────────┴───────────┘
```

---



<h3 id="rxKzp">**从字面值创建**</h3>
要基于其他 Python 值创建一个新的表达式，你可以使用函数 `pl.lit()`。`Lit` 是 "literal" 的缩写。接下来的几个示例使用 `pl.select()` 函数执行表达式，该函数从一个新的空 DataFrame 开始：

```python
pl.select(pl.lit(42))
```

```python
shape: (1, 1)
┌─────────┐
│ literal │
│ ---     │
│ i32     │
╞═════════╡
│ 42      │
└─────────┘
```

注意，列名字面上就是 `literal`。你可以使用方法 `Expr.alias()` 为这列赋予一个更好的名字：

```python
pl.select(pl.lit(42).alias("answer"))
```

```python
shape: (1, 1)
┌────────┐
│ answer │
│ ---    │
│ i32    │
╞════════╡
│ 42     │
└────────┘
```

当你使用 `pl.select()` 函数执行这些表达式时，构建的 Series 只有一个值。然而，当你对非空的 DataFrame 执行相同的表达式时，Series 的长度将等于行数：

```python
fruit.with_columns(pl.lit("Earth").alias("planet"))
```

```plain
shape: (10, 6)
┌────────────┬────────┬────────┬──────────┬───────────────┬────────┐
│ name       │ weight │ color  │ is_round │ origin        │ planet │
│ ---        │ ---    │ ---    │ ---      │ ---           │ ---    │
│ str        │ i64    │ str    │ bool     │ str           │ str    │
╞════════════╪════════╪════════╪══════════╪═══════════════╪════════╡
│ Avocado    │ 200    │ green  │ false    │ South America │ Earth  │
│ Banana     │ 120    │ yellow │ false    │ Asia          │ Earth  │
│ Blueberry  │ 1      │ blue   │ false    │ North America │ Earth  │
│ Cantaloupe │ 2500   │ orange │ true     │ Africa        │ Earth  │
│ Cranberry  │ 2      │ red    │ false    │ North America │ Earth  │
│ Elderberry │ 1      │ black  │ false    │ Europe        │ Earth  │
│ Orange     │ 130    │ orange │ true     │ Asia          │ Earth  │
│ Papaya     │ 1000   │ orange │ false    │ South America │ Earth  │
│ Peach      │ 150    │ orange │ true     │ Asia          │ Earth  │
│ Watermelon │ 5000   │ green  │ true     │ Africa        │ Earth  │
└────────────┴────────┴────────┴──────────┴───────────────┴────────┘
```



如你所见，值 `Earth` 被重复了以至于 `planet` 列的 Series 长度等于 DataFrame 中的行数。只有当你将单个值传递给函数 `pl.lit()` 时，值才会自动重复。当你传递多个值，但这些值的数量少于行数时，你会得到一个错误：

```python
fruit.with_columns(pl.lit(pl.Series([False, True])).alias("row_is_even"))
```

```python
ShapeError: unable to add a column of length 2 to a DataFrame of height 10
```

此外，值列表 `[False, True]` 首先通过 `pl.Series()` 构造函数转换为 Series。否则，Polars 将创建一个列表列，使得每行都有这两个值：

```python
fruit.with_columns(pl.lit([False, True]).alias("row_is_even"))
```

```plain
shape: (10, 6)
┌────────────┬────────┬────────┬──────────┬───────────────┬───────────────┐
│ name       │ weight │ color  │ is_round │ origin        │ row_is_even   │
│ ---        │ ---    │ ---    │ ---      │ ---           │ ---           │
│ str        │ i64    │ str    │ bool     │ str           │ list[bool]    │
╞════════════╪════════╪════════╪══════════╪═══════════════╪═══════════════╡
│ Avocado    │ 200    │ green  │ false    │ South America │ [false, true] │
│ Banana     │ 120    │ yellow │ false    │ Asia          │ [false, true] │
│ …          │ …      │ …      │ …        │ …             │ …             │
│ Peach      │ 150    │ orange │ true     │ Asia          │ [false, true] │
│ Watermelon │ 5000   │ green  │ true     │ Africa        │ [false, true] │
└────────────┴────────┴────────┴──────────┴───────────────┴───────────────┘
```



要显式重复值，并指定固定的次数，你可以使用函数 `pl.repeat()`。函数 `pl.zeros()` 和 `pl.ones()` 分别是 `pl.repeat(0.0)` 和 `pl.repeat(1.0)` 的别名：

```python
pl.select(
    pl.repeat("Ello", 3).alias("hello"),
    pl.zeros(3),
    pl.ones(3)
)
```

```python
shape: (3, 3)

┌───────┬───────┬───────┐
│ hello │ zeros │ ones  │
│ ---   │ ---   │ ---   │
│ str   │ f64   │ f64   │
├───────┼───────┼───────┤
│ Ello  │ 0.0   │ 1.0   │
│ Ello  │ 0.0   │ 1.0   │
│ Ello  │ 0.0   │ 1.0   │
└───────┴───────┴───────┘
```

请记住，每个 Series 的长度必须相同；否则你会得到错误：

```python
fruit.with_columns(pl.repeat("Earth", 9).alias("planet"))
```

输出：

```python
ShapeError: unable to add a column of length 9 to a DataFrame of height 10
```



<h3 id="tSnVT">**从范围(Ranges)创建**</h3>
Polars 提供了几个方便的函数，用于创建整数、日期、时间和日期时间的范围。它们列在表6-1中。

---

_表6-1：创建范围的函数_

| 函数 | 描述 |
| --- | --- |
| `<font style="color:#1DC0C9;">pl.arange(...)</font>` | 一个整数范围（`pl.int_range(...)` 的别名） |
| `<font style="color:#1DC0C9;">pl.date_range(...)</font>` | 一个日期范围 |
| `<font style="color:#1DC0C9;">pl.date_ranges(...)</font>` | 每个元素是一个日期范围 |
| `<font style="color:#1DC0C9;">pl.datetime_range(...)</font>` | 一个日期时间范围 |
| `<font style="color:#1DC0C9;">pl.datetime_ranges(...)</font>` | 每个元素是一个日期时间范围 |
| `<font style="color:#1DC0C9;">pl.int_range(...)</font>` | 一个整数范围 |
| `<font style="color:#1DC0C9;">pl.int_ranges(...)</font>` | 每个元素是一个整数范围 |
| `<font style="color:#1DC0C9;">pl.time_range(...)</font>` | 一个时间范围 |
| `<font style="color:#1DC0C9;">pl.time_ranges(...)</font>` | 每个元素是一个时间范围 |




以下示例展示了函数 `pl.int_range()`、它的别名 `pl.arange()` 和 `pl.int_ranges()` 的使用。它还包括对方法 `Expr.list.len()` 的简要介绍，该方法计算 `int_range` 列中每个列表的元素数量：

```python
pl.select(
    pl.int_range(0, 5).alias("start"),
    pl.arange(0, 10, 2).pow(2).alias("end")
).with_columns(
    pl.int_ranges("start", "end").alias("int_range")
).with_columns(
    pl.col("int_range").list.len().alias("range_length")
)
```

```plain
shape: (5, 4)
┌───────┬──────┬──────────────┬──────────────┐
│ start │ end  │ int_range    │ range_length │
│ ---   │ ---  │ ---          │ ---          │
│ i64   │ f64  │ list[i64]    │ u32          │
╞═══════╪══════╪══════════════╪══════════════╡
│ 0     │ 0.0  │ []           │ 0            │
│ 1     │ 4.0  │ [1, 2, 3]    │ 3            │
│ 2     │ 16.0 │ [2, 3, … 15] │ 14           │
│ 3     │ 36.0 │ [3, 4, … 35] │ 33           │
│ 4     │ 64.0 │ [4, 5, … 63] │ 60           │
└───────┴──────┴──────────────┴──────────────┘
```



请注意，函数 `pl.int_ranges()` 会生成一个 Series，其中每个元素都是一个整数列表。函数 `pl.date_ranges`、`pl.datetime_ranges` 和 `pl.time_ranges` 类似工作，但分别用于日期、日期时间和时间：

```python
from datetime import date

pl.select(
    pl.date_range(date(1985, 10, 21), date(1985, 10, 26)).alias("start"),
    pl.repeat(date(2021, 10, 21), 6).alias("end")
).with_columns(
    pl.datetime_ranges("start", "end", interval="1h").alias("range")
)
```

```plain
shape: (6, 3)
┌────────────┬────────────┬──────────────────────────────────────────────┐
│ start      │ end        │ range                                        │
│ ---        │ ---        │ ---                                          │
│ date       │ date       │ list[datetime[μs]]                           │
╞════════════╪════════════╪══════════════════════════════════════════════╡
│ 1985-10-21 │ 2021-10-21 │ [1985-10-21 00:00:00, 1985-10-21 01:00:00, … │
│ 1985-10-22 │ 2021-10-21 │ [1985-10-22 00:00:00, 1985-10-22 01:00:00, … │
│ 1985-10-23 │ 2021-10-21 │ [1985-10-23 00:00:00, 1985-10-23 01:00:00, … │
│ 1985-10-24 │ 2021-10-21 │ [1985-10-24 00:00:00, 1985-10-24 01:00:00, … │
│ 1985-10-25 │ 2021-10-21 │ [1985-10-25 00:00:00, 1985-10-25 01:00:00, … │
│ 1985-10-26 │ 2021-10-21 │ [1985-10-26 00:00:00, 1985-10-26 01:00:00, … │
└────────────┴────────────┴──────────────────────────────────────────────┘
```



在第11章中，我们将更详细地讨论时间数据的处理（例如日期和时间）。



<h3 id="JkLKK">**其他创建表达式的函数**</h3>
有许多函数可以用于创建表达式。不幸的是，我们无法在本章中全部介绍它们。不过，为了给你一个初步的了解，我们将简要介绍几个函数，它们的作用，以及我们将在何处详细讨论它们。

首先，函数 `pl.count()` 顾名思义，用于计算行数。它通常在使用 `df.group_by()` 方法进行聚合时使用。这将在第12章中介绍。

其次，函数 `pl.element()` 表示列表中的单个元素。它与方法 `Expr.list.eval()` 结合使用，以将表达式应用于列表中的每个元素。我们将在第11章中进一步解释。

最后，函数 `pl.sql_expr()` 在你想要使用 SQL 创建表达式时非常有用。



<h2 id="hhmuj">**重命名表达式**</h2>
重命名表达式——这最终决定了将构建的 Series 的名称——经常发生。有多种原因促使你想要重命名表达式，包括：

+ 更好地表达列的含义
+ 避免重复的列名
+ 清理列名
+ 更改默认的列名

---

**<font style="color:#1DC0C9;">好的命名</font>**

拥有好的表达式名称与拥有好的变量名称一样重要。它们可以极大地影响代码的质量。我们个人建议使用全部小写、使用下划线分隔单词的列名。

---

最常见的更改表达式名称的方法是 `Expr.alias()`。与表达式名称相关的其他方法可以在 `Expr.name` 命名空间中找到（见表6-2）。方法 `Expr.name.map_fields()`、`Expr.name.prefix_fields()` 和 `Expr.name.suffix_fields()` 只能在表达式的数据类型为 `pl.Struct` 时使用。

---

_表6-2：用于重命名表达式的方法_

| 方法 | 描述 |
| --- | --- |
| `<font style="color:#1DC0C9;">Expr.alias(…)</font>` | 重命名表达式。 |
| `<font style="color:#1DC0C9;">Expr.name.keep()</font>` | 保留表达式的原始根名称。 |
| `<font style="color:#1DC0C9;">Expr.name.map(…)</font>` | 通过对根名称映射函数来重命名表达式。 |
| `<font style="color:#1DC0C9;">Expr.name.prefix(…)</font>` | 在表达式的根列名称前添加前缀。 |
| `<font style="color:#1DC0C9;">Expr.name.suffix(…)</font>` | 在表达式的根列名称后添加后缀。 |
| `<font style="color:#1DC0C9;">Expr.name.to_lowercase()</font>` | 将根列名称转换为小写。 |
| `<font style="color:#1DC0C9;">Expr.name.to_uppercase()</font>` | 将根列名称转换为大写。 |
| `<font style="color:#1DC0C9;">Expr.name.map_fields(…)</font>` | 通过对字段名称映射函数来重命名结构体的字段。 |
| `<font style="color:#1DC0C9;">Expr.name.prefix_fields(…)</font>` | 为结构体的所有字段名称添加前缀。 |
| `<font style="color:#1DC0C9;">Expr.name.suffix_fields(…)</font>` | 为结构体的所有字段名称添加后缀。 |




为了演示，请参考这个包含一些任意列名的小 DataFrame：

```python
df = pl.DataFrame({"text": "value", "An integer": 5040, "BOOLEAN": True})
df
```

```plain
shape: (1, 3)
┌───────┬────────────┬─────────┐
│ text  │ An integer │ BOOLEAN │
│ ---   │ ---        │ ---     │
│ str   │ i64        │ bool    │
╞═══════╪════════════╪═════════╡
│ value │ 5040       │ true    │
└───────┴────────────┴─────────┘
```

我们可以通过多种方法修改列名：

```python
df.select(
    pl.col("text").name.to_uppercase(),
    pl.col("An integer").alias("int"),
    pl.col("BOOLEAN").name.to_lowercase(),
)
```

```plain
shape: (1, 3)
┌───────┬──────┬─────────┐
│ TEXT  │ int  │ boolean │
│ ---   │ ---  │ ---     │
│ str   │ i64  │ bool    │
╞═══════╪══════╪═════════╡
│ value │ 5040 │ true    │
└───────┴──────┴─────────┘
```



---

**<font style="color:#ECAA04;">链式命名操作</font>**

在撰写本文时，Polars 每个表达式只允许进行一个命名操作。因此，以下操作是**<font style="color:#DF2A3F;">不允许</font>**的：

```python
df.select(
    pl.all()
    .name.to_lowercase()
    .name.map(lambda s: s.replace(" ", "_"))
)
```

```python
PanicException: no `rename_alias` expected at this point
```

解决方案是将所有操作合并为一个（匿名）函数，然后通过 `Expr.name.map()` 方法应用它：

```python
df.select(
    pl.all()
    .name.map(lambda s: s.lower().replace(" ", "_"))
)
```

输出：

```python
shape: (1, 3)
┌───────┬────────────┬─────────┐
│ text  │ an_integer │ boolean │
│ ---   │ ---        │ ---     │
│ str   │ i64        │ bool    │
╞═══════╪════════════╪═════════╡
│ value │ 5040       │ true    │
└───────┴────────────┴─────────┘
```

这种限制可能会在 Polars 的未来版本中取消。

---

<h2 id="KEVSB">**表达式是符合惯例的**</h2>
你已经知道表达式是延迟执行的，并且它们需要被执行才能发挥作用。我们理解，习惯这一点可能需要一些时间，特别是如果你习惯了使用像 Pandas 这样的非延迟执行（即时执行）方式工作。

因此，这里需要提醒一点。所有表达式方法和内联操作同样适用于 Series。例如，之前用表达式过滤行的示例可以重写为直接使用 Series：

```python
fruit.filter(
    (fruit["weight"] > 1000) & fruit["is_round"]
)
```

```python
shape: (2, 5)

┌───────────┬────────┬─────────┬──────────┬─────────┐
│ name      │ weight │ color   │ is_round │ origin  │
│ ---       │ ---    │ ---     │ ---      │ ---     │
│ str       │ i64    │ str     │ bool     │ str     │
├───────────┼────────┼─────────┼──────────┼─────────┤
│ Cantaloupe│ 2500   │ orange  │ true     │ Africa  │
│ Watermelon│ 5000   │ green   │ true     │ Africa  │
└───────────┴────────┴─────────┴──────────┴─────────┘
```

如果你有 Pandas 的使用经验，那么这种语法看起来会很熟悉，你可能会在使用 Polars 时倾向于以这种方式编写代码。

尽管上面的代码生成的结果与原示例相同，但它是急切执行的。因此，它没有使用 Polars 的查询引擎，也没有进行任何优化。此外，这两个组件是串行执行的，而不是并行执行的。

当你对 LazyFrame 应用多个方法时，这一点会更加清晰。下面是一个使用表达式的示例：

```python
(
    fruit
    .lazy()
    .filter((pl.col("weight") > 1000) & pl.col("is_round"))
    .with_columns(pl.col("name").str.ends_with("berry").alias("is_berry"))
    .collect()
)
```

```plain
shape: (2, 6)
┌────────────┬────────┬────────┬──────────┬────────┬──────────┐
│ name       │ weight │ color  │ is_round │ origin │ is_berry │
│ ---        │ ---    │ ---    │ ---      │ ---    │ ---      │
│ str        │ i64    │ str    │ bool     │ str    │ bool     │
╞════════════╪════════╪════════╪══════════╪════════╪══════════╡
│ Cantaloupe │ 2500   │ orange │ true     │ Africa │ false    │
│ Watermelon │ 5000   │ green  │ true     │ Africa │ false    │
└────────────┴────────┴────────┴──────────┴────────┴──────────┘
```



再来看一个没有使用表达式的案例：

```python
(
    fruit
    .lazy()
    .filter((fruit["weight"] > 1000) & fruit["is_round"])
    .with_columns(fruit["name"].str.ends_with("berry").alias("is_berry"))
    .collect()
)
```

```python
ShapeError: unable to add a column of length 10 to a DataFrame of height 2
```



没错：Polars 无法优化执行计划，现在你还必须小心地按正确顺序应用方法以避免错误。（错误的原因是 `df.filter()` 方法将 DataFrame 缩减为两行，而变量 `fruit` 仍然引用具有10行的 DataFrame。）



基于这些原因，我们始终鼓励你使用表达式。在 Polars 中，惰性操作是有回报的。

 

<h2 id="lRuWU">**结论**</h2>
表达式是 Polars 的核心。在本章中，我们介绍了表达式的基础知识：它们是什么、在哪里使用、如何创建，以及为什么它们如此优雅和高效。在下一章中，我们将解释如何通过添加操作继续构建表达式。

