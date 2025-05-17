

上一章讨论了列，而本章将讨论 DataFrame 中的行。我们将主要介绍可以对行执行的两种操作：过滤和排序。

通过过滤，您可以根据行的值选择其中的一部分。通过排序，您可以根据其值重新排列行；行的数量保持不变。

在本章中，您将学习如何：

+ 使用 `df.filter()` 方法过滤行
+ 使用 `df.sort()` 方法排序行
+ 应用与过滤和排序相关的其他各种方法

您将使用一个关于电动工具的小型 DataFrame，这些工具通常可以在业余木工的车库中找到。对于每个工具，我们有其类型、产品代码、品牌、是否无绳、价格以及转速 (RPM)。这是 `tools` DataFrame 的样子：

```python
tools = pl.read_csv("data/tools.csv")
tools
```

```plain
shape: (10, 6)
┌───────────────────────┬──────────────┬────────┬──────────┬───────┬───────┐
│ tool                  │ product      │ brand  │ cordless │ price │ rpm   │
│ ---                   │ ---          │ ---    │ ---      │ ---   │ ---   │
│ str                   │ str          │ str    │ bool     │ i64   │ i64   │
╞═══════════════════════╪══════════════╪════════╪══════════╪═══════╪═══════╡
│ Rotary Hammer         │ HR2230       │ Makita │ false    │ 199   │ 1050  │
│ Miter Saw             │ GCM 8 SJL    │ Bosch  │ false    │ 391   │ 5500  │
│ Plunge Cut Saw        │ DSP600ZJ     │ Makita │ true     │ 459   │ 6300  │
│ Impact Driver         │ DTD157Z      │ Makita │ true     │ 156   │ 3000  │
│ Jigsaw                │ PST 900 PEL  │ Bosch  │ false    │ 79    │ 3100  │
│ Angle Grinder         │ DGA504ZJ     │ Makita │ true     │ 229   │ 8500  │
│ Nail Gun              │ DPSB2IN1-XJ  │ DeWalt │ true     │ 129   │ null  │
│ Router                │ POF 1400 ACE │ Bosch  │ false    │ 185   │ 28000 │
│ Random Orbital Sander │ DBO180ZJ     │ Makita │ true     │ 199   │ 11000 │
│ Table Saw             │ DWE7485      │ DeWalt │ false    │ 516   │ 5800  │
└───────────────────────┴──────────────┴────────┴──────────┴───────┴───────┘
```

让我们先看看过滤行的方法。



<h2 id="P4JG9">过滤行</h2>
可以使用 `df.filter()` 方法过滤 DataFrame 的行。过滤使我们能够回答包含“至少”或“等于”这类短语的问题。例如，哪些工具是 Makita 牌的？或者：哪些工具是无绳的？下图（drawing-filter.png）概念性地说明了这个操作。<font style="color:#E746A4;"><注意：原书网页图片失效了，等后面作者补上传后再补充></font>

要过滤行，您需要指定要保留的行。这可以通过表达式、列名和约束来完成。我们首先讨论表达式，因为它们是这三种方法中最灵活的。



<h3 id="NuYYn">基于表达式的过滤</h3>
第一种过滤行的方法是使用表达式。您已经在第六章见过它们的应用。它们是三种过滤方式中最灵活的，因为您可以使用各种类型的比较（如等于、大于等于）并结合布尔代数（如 OR 和 AND）。如果需要复习，比较和布尔代数操作将在第八章中讨论。

表达式允许您构建各种过滤器。只需确保表达式会评估为一个布尔序列（Series）。`True` 表示对应的行将被保留，`False` 表示将被丢弃。

让我们过滤 `tools` DataFrame 以保留我们喜欢的工具，这些工具恰好是 Makita 牌的无绳工具：

```python
tools.filter(
    pl.col("cordless") &  ①
    (pl.col("brand") == "Makita")
)
```

```plain
shape: (4, 6)
┌───────────────────────┬──────────┬────────┬──────────┬───────┬───────┐
│ tool                  │ product  │ brand  │ cordless │ price │ rpm   │
│ ---                   │ ---      │ ---    │ ---      │ ---   │ ---   │
│ str                   │ str      │ str    │ bool     │ i64   │ i64   │
╞═══════════════════════╪══════════╪════════╪══════════╪═══════╪═══════╡
│ Plunge Cut Saw        │ DSP600ZJ │ Makita │ true     │ 459   │ 6300  │
│ Impact Driver         │ DTD157Z  │ Makita │ true     │ 156   │ 3000  │
│ Angle Grinder         │ DGA504ZJ │ Makita │ true     │ 229   │ 8500  │
│ Random Orbital Sander │ DBO180ZJ │ Makita │ true     │ 199   │ 11000 │
└───────────────────────┴──────────┴────────┴──────────┴───────┴───────┘
```

<font style="color:rgb(61, 59, 73);">①你不需要写 </font>`<font style="color:rgb(61, 59, 73);">pl.col("cordless") == True</font>`<font style="color:rgb(61, 59, 73);">，因为列 </font>`<font style="color:rgb(61, 59, 73);">cordless</font>`<font style="color:rgb(61, 59, 73);"> 的数据类型已经是布尔值。</font>

<font style="color:rgb(61, 59, 73);"></font>

---

**<font style="color:#01B2BC;">用逗号代替与运算符</font>**

<font style="color:rgb(61, 59, 73);">如果您的表达式由多个部分组成，并且这些部分是通过 AND 运算符（</font>`<font style="color:rgb(61, 59, 73);">&</font>`<font style="color:rgb(61, 59, 73);">）组合的，那么您可以选择将这些部分作为单独的参数传递给 </font>`<font style="color:rgb(61, 59, 73);">df.filter()</font>`<font style="color:rgb(61, 59, 73);"> 方法。这意味着上一段代码可以重写为：</font>

```python
tools.filter(
    pl.col("cordless"),
    pl.col("brand") == "Makita"
)
```

<font style="color:rgb(61, 59, 73);">根据您的偏好，这样可能会提高过滤器的可读性。请注意，这种方式不适用于 OR 运算符（</font>`<font style="color:rgb(61, 59, 73);">|</font>`<font style="color:rgb(61, 59, 73);">）。</font>

---

<h3 id="wJlDx"><font style="color:rgb(61, 59, 73);">基于列名的过滤</font></h3>
<font style="color:rgb(61, 59, 73);">使用 </font>`<font style="color:rgb(61, 59, 73);">df.filter()</font>`<font style="color:rgb(61, 59, 73);"> 的第二种方法是通过指定列名。如果某列是布尔值类型，例如 </font>`<font style="color:rgb(61, 59, 73);">tools</font>`<font style="color:rgb(61, 59, 73);"> DataFrame 中的 </font>`<font style="color:rgb(61, 59, 73);">cordless</font>`<font style="color:rgb(61, 59, 73);"> 列，您可以直接使用列名，而不需要将其转换为表达式。例如，要选择所有无绳工具（不只是 Makita 牌的工具），您可以使用：</font>

```python
tools.filter("cordless")
```

```plain
shape: (5, 6)
┌───────────────────────┬─────────────┬────────┬──────────┬───────┬───────┐
│ tool                  │ product     │ brand  │ cordless │ price │ rpm   │
│ ---                   │ ---         │ ---    │ ---      │ ---   │ ---   │
│ str                   │ str         │ str    │ bool     │ i64   │ i64   │
╞═══════════════════════╪═════════════╪════════╪══════════╪═══════╪═══════╡
│ Plunge Cut Saw        │ DSP600ZJ    │ Makita │ true     │ 459   │ 6300  │
│ Impact Driver         │ DTD157Z     │ Makita │ true     │ 156   │ 3000  │
│ Angle Grinder         │ DGA504ZJ    │ Makita │ true     │ 229   │ 8500  │
│ Nail Gun              │ DPSB2IN1-XJ │ DeWalt │ true     │ 129   │ null  │
│ Random Orbital Sander │ DBO180ZJ    │ Makita │ true     │ 199   │ 11000 │
└───────────────────────┴─────────────┴────────┴──────────┴───────┴───────┘
```

您可以指定多个列名，但请记住它们都需要是布尔类型。



---

**<font style="color:#ECAA04;">Polars 无法处理真值判断</font>**

在 Python 语言中，有真值和假值的概念。某个值是否为真值或假值，取决于它是否可以转换为布尔值。如果值被转换为布尔值时为 `True`，则为真值；反之为假值。假值包括 `False` 本身、数字 0、空序列、空集合和空字符串。其他一切都被视为真值。这意味着像 `(my_name != "") and (len(my_list) > 0)` 这样的 Python 代码可以重写为 `my_name and my_list`。

由于这个语言概念，您可能会认为 Python Polars 在过滤时允许使用非布尔列和表达式。然而，Polars 是用 Rust 编写的，因此比 Python 更严格：只有布尔列和构建布尔序列的表达式可以用于过滤。

您可以通过使用比较操作将非布尔表达式转换为布尔值。例如，要测试非空字符串和非空列表，您可以分别使用 `pl.col("my_name") != ""` 和 `pl.col("my_list").list.len() > 0`。

---

<h3 id="aGtXR">基于约束的过滤</h3>
使用 `df.filter()` 的第三种方法是通过指定约束。约束由列名和一个值组成。再次过滤无绳的 Makita 工具时，使用约束的过滤方式如下所示：

```python
tools.filter(cordless=True, brand="Makita")
```

```plain
shape: (4, 6)
┌───────────────────────┬──────────┬────────┬──────────┬───────┬───────┐
│ tool                  │ product  │ brand  │ cordless │ price │ rpm   │
│ ---                   │ ---      │ ---    │ ---      │ ---   │ ---   │
│ str                   │ str      │ str    │ bool     │ i64   │ i64   │
╞═══════════════════════╪══════════╪════════╪══════════╪═══════╪═══════╡
│ Plunge Cut Saw        │ DSP600ZJ │ Makita │ true     │ 459   │ 6300  │
│ Impact Driver         │ DTD157Z  │ Makita │ true     │ 156   │ 3000  │
│ Angle Grinder         │ DGA504ZJ │ Makita │ true     │ 229   │ 8500  │
│ Random Orbital Sander │ DBO180ZJ │ Makita │ true     │ 199   │ 11000 │
└───────────────────────┴──────────┴────────┴──────────┴───────┴───────┘
```



实际上，列名是作为关键字参数指定的。由于 Python 语言的工作机制，这有一些限制：

+ 列名只能包含字母（`a-z`，`A-Z`）、数字（`0-9`）和下划线（`_`），不能以数字开头，也不能是 Python 中的保留关键字（例如，`if`，`class`，`global`）。
+ 当您将约束与其他两种方法（表达式和列名）结合使用时，约束必须出现在最后。
+ 必须始终指定值，包括 `True`。
+ 仅支持相等比较，必须使用一个等号（`=`）而不是两个。此外，Python 风格指南规定，等号周围不应有空格。

---

**<font style="color:#01B2BC;">不要限制自己</font>**

由于这些限制，我们建议不要使用约束。我们的推荐是使用表达式进行过滤。它们虽然更冗长，但至少不会限制您的表达能力。

---

<h2 id="WIkl6">排序行</h2>
通过排序，您可以根据一个或多个列中的值更改行的顺序。行的数量保持不变。排序允许我们回答包含“最多”或“最少”这类短语的问题。例如，哪个品牌的工具最多？或者：价格最低的工具是什么？下图（drawing-sort.png）概念性地说明了这个操作。

<font style="color:#E746A4;"><注意：原书网页图片失效了，等后面作者补上传后再补充></font>

大多数情况下，您会对数字进行排序，但您也可以对字符串、日期和时间进行排序。您还可以对容器数据类型（如结构体和列表）进行排序，稍后我们会展示。在接下来的几节中，我们将讨论如何基于单列、多列和表达式进行排序。

<h3 id="nhzLM">基于单列的排序</h3>
要进行排序，您可以使用 `df.sort()` 方法。调用此方法最简单的方法是指定一个列名：

```python
tools.sort("price")
```

```plain
shape: (10, 6)
┌────────────────┬─────────────┬────────┬──────────┬───────┬──────┐
│ tool           │ product     │ brand  │ cordless │ price │ rpm  │
│ ---            │ ---         │ ---    │ ---      │ ---   │ ---  │
│ str            │ str         │ str    │ bool     │ i64   │ i64  │
╞════════════════╪═════════════╪════════╪══════════╪═══════╪══════╡
│ Jigsaw         │ PST 900 PEL │ Bosch  │ false    │ 79    │ 3100 │
│ Nail Gun       │ DPSB2IN1-XJ │ DeWalt │ true     │ 129   │ null │
│ …              │ …           │ …      │ …        │ …     │ …    │
│ Plunge Cut Saw │ DSP600ZJ    │ Makita │ true     │ 459   │ 6300 │
│ Table Saw      │ DWE7485     │ DeWalt │ false    │ 516   │ 5800 │
└────────────────┴─────────────┴────────┴──────────┴───────┴──────┘
```

正如您所见，默认情况下，值按升序排序。表 10-1 列出了 `df.sort()` 方法接受的其他参数。



_表 10-1：_`_df.sort()_`_ 方法的常见参数_

| 参数 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">descending</font>**` | 按降序排序。当按多列排序时，可以通过传递布尔序列按每列分别指定。默认 `False`。 |
| `**<font style="color:#01B2BC;">nulls_last</font>**` | 将空值放在最后。默认 `False`。 |
| `**<font style="color:#01B2BC;">multithreaded</font>**` | 使用多线程排序。默认 `True`。 |
| `**<font style="color:#01B2BC;">maintain_order</font>**` | 如果元素相等，是否应保持顺序。默认 `False`。 |


> a. 仅当您的 Polars 代码已经是多线程应用程序的一部分时，才能将此值设置为 `False`。
>



<h3 id="RZspH">逆序排序</h3>
您可以通过将 `descending` 关键字设置为 `True` 来更改默认排序顺序：

```python
tools.sort("price", descending=True)
```

```plain
shape: (10, 6)
┌────────────────┬─────────────┬────────┬──────────┬───────┬──────┐
│ tool           │ product     │ brand  │ cordless │ price │ rpm  │
│ ---            │ ---         │ ---    │ ---      │ ---   │ ---  │
│ str            │ str         │ str    │ bool     │ i64   │ i64  │
╞════════════════╪═════════════╪════════╪══════════╪═══════╪══════╡
│ Table Saw      │ DWE7485     │ DeWalt │ false    │ 516   │ 5800 │
│ Plunge Cut Saw │ DSP600ZJ    │ Makita │ true     │ 459   │ 6300 │
│ …              │ …           │ …      │ …        │ …     │ …    │
│ Nail Gun       │ DPSB2IN1-XJ │ DeWalt │ true     │ 129   │ null │
│ Jigsaw         │ PST 900 PEL │ Bosch  │ false    │ 79    │ 3100 │
└────────────────┴─────────────┴────────┴──────────┴───────┴──────┘
```



---

**<font style="color:#ECAA04;">向上还是向下？</font>**

请确保使用 `descending` 关键字，而不是 `ascending`，否则会出现错误：

```python
tools.sort("price", ascending=False)
```

```plain
TypeError: DataFrame.sort() got an unexpected keyword argument 'ascending'
```

这很容易忘记，尤其是当您习惯使用 Pandas 时，在 Pandas 中可以使用 `ascending=False` 来反转排序顺序。

---

<h3 id="vPopW">基于多列的排序</h3>
要基于多列进行排序，您可以指定多个列名作为单独的参数：

```python
tools.sort("brand", "price")
```

```plain
shape: (10, 6)
┌────────────────┬──────────────┬────────┬──────────┬───────┬───────┐
│ tool           │ product      │ brand  │ cordless │ price │ rpm   │
│ ---            │ ---          │ ---    │ ---      │ ---   │ ---   │
│ str            │ str          │ str    │ bool     │ i64   │ i64   │
╞════════════════╪══════════════╪════════╪══════════╪═══════╪═══════╡
│ Jigsaw         │ PST 900 PEL  │ Bosch  │ false    │ 79    │ 3100  │
│ Router         │ POF 1400 ACE │ Bosch  │ false    │ 185   │ 28000 │
│ …              │ …            │ …      │ …        │ …     │ …     │
│ Angle Grinder  │ DGA504ZJ     │ Makita │ true     │ 229   │ 8500  │
│ Plunge Cut Saw │ DSP600ZJ     │ Makita │ true     │ 459   │ 6300  │
└────────────────┴──────────────┴────────┴──────────┴───────┴───────┘
```



默认情况下，您指定的所有列都会按升序排序。将 `descending` 设置为 `True` 将应用于所有列。如果您希望每列的排序方向不同，可以传递一个布尔值列表给 `descending`：

```python
tools.sort("brand", "price", descending=[False, True])
```

```plain
shape: (10, 6)
┌───────────────────────┬──────────────┬────────┬──────────┬───────┬───────┐
│ tool                  │ product      │ brand  │ cordless │ price │ rpm   │
│ ---                   │ ---          │ ---    │ ---      │ ---   │ ---   │
│ str                   │ str          │ str    │ bool     │ i64   │ i64   │
╞═══════════════════════╪══════════════╪════════╪══════════╪═══════╪═══════╡
│ Miter Saw             │ GCM 8 SJL    │ Bosch  │ false    │ 391   │ 5500  │
│ Router                │ POF 1400 ACE │ Bosch  │ false    │ 185   │ 28000 │
│ …                     │ …            │ …      │ …        │ …     │ …     │
│ Random Orbital Sander │ DBO180ZJ     │ Makita │ true     │ 199   │ 11000 │
│ Impact Driver         │ DTD157Z      │ Makita │ true     │ 156   │ 3000  │
└───────────────────────┴──────────────┴────────┴──────────┴───────┴───────┘
```

请确保布尔值的数量与列的数量相等。

<h3 id="Hxrbm">基于表达式的排序</h3>
`df.sort()` 方法也接受一个或多个表达式：

```python
tools.sort(pl.col("rpm") / pl.col("price"))
```

```plain
shape: (10, 6)
┌───────────────────────┬──────────────┬────────┬──────────┬───────┬───────┐
│ tool                  │ product      │ brand  │ cordless │ price │ rpm   │
│ ---                   │ ---          │ ---    │ ---      │ ---   │ ---   │
│ str                   │ str          │ str    │ bool     │ i64   │ i64   │
╞═══════════════════════╪══════════════╪════════╪══════════╪═══════╪═══════╡
│ Nail Gun              │ DPSB2IN1-XJ  │ DeWalt │ true     │ 129   │ null  │
│ Rotary Hammer         │ HR2230       │ Makita │ false    │ 199   │ 1050  │
│ …                     │ …            │ …      │ …        │ …     │ …     │
│ Random Orbital Sander │ DBO180ZJ     │ Makita │ true     │ 199   │ 11000 │
│ Router                │ POF 1400 ACE │ Bosch  │ false    │ 185   │ 28000 │
└───────────────────────┴──────────────┴────────┴──────────┴───────┴───────┘
```

请注意，表达式不会显示为 DataFrame 中的列。与过滤一样，表达式会为您提供最大的灵活性。不过，根据我们的经验，您通常会在 DataFrame 中已经存在的列上进行排序。



<h3 id="mFb0a">对嵌套数据类型排序</h3>
您不能直接对嵌套数据类型（如 Struct、List 和 Array）进行排序。您需要先提取或创建一个可排序的值。为了演示这一点，让我们创建一个新的 DataFrame `tools_collection`，该 DataFrame 按品牌将所有工具分组为 Struct 列表：

```python
tools_collection = tools.group_by("brand").agg(collection=pl.struct(pl.all()))
tools_collection
```

```plain
shape: (3, 2)
┌────────┬───────────────────────────────────────────────────────────────┐
│ brand  │ collection                                                    │
│ ---    │ ---                                                           │
│ str    │ list[struct[6]]                                               │
╞════════╪═══════════════════════════════════════════════════════════════╡
│ DeWalt │ [{"Nail Gun","DPSB2IN1-XJ","DeWalt",true,129,null}, {"Table … │
│ Makita │ [{"Rotary Hammer","HR2230","Makita",false,199,1050}, {"Plung… │
│ Bosch  │ [{"Miter Saw","GCM 8 SJL","Bosch",false,391,5500}, {"Jigsaw"… │
└────────┴───────────────────────────────────────────────────────────────┘
```



如果你直接尝试对 `collection` 列进行排序，会出现错误。不过，你可以根据列表的长度进行排序，因为长度是一个可以排序的整数。

```python
tools_collection.sort(pl.col("collection").list.len(), descending=True)
```

```plain
shape: (3, 2)
┌────────┬───────────────────────────────────────────────────────────────┐
│ brand  │ collection                                                    │
│ ---    │ ---                                                           │
│ str    │ list[struct[6]]                                               │
╞════════╪═══════════════════════════════════════════════════════════════╡
│ Makita │ [{"Rotary Hammer","HR2230","Makita",false,199,1050}, {"Plung… │
│ Bosch  │ [{"Miter Saw","GCM 8 SJL","Bosch",false,391,5500}, {"Jigsaw"… │
│ DeWalt │ [{"Nail Gun","DPSB2IN1-XJ","DeWalt",true,129,null}, {"Table … │
└────────┴───────────────────────────────────────────────────────────────┘
```

**<font style="color:rgb(61, 59, 73);">  
</font>**另一个例子是根据每个品牌的平均价格进行排序：

```python
tools_collection.sort(
    pl.col("collection").list.eval(
        pl.element().struct.field("price")
    ).list.mean()
)
```

```plain
shape: (3, 2)
┌────────┬────────────────────────────────────────────────────────────────────┐
│ brand  │ collection                                                         │
│ ---    │ ---                                                                │
│ str    │ list[struct[6]]                                                    │
╞════════╪════════════════════════════════════════════════════════════════════╡
│ Bosch  │ [{"Miter Saw","GCM 8 SJL","Bosch",false,391,5500}, {"Jigsaw","PST… │
│ Makita │ [{"Rotary Hammer","HR2230","Makita",false,199,1050}, {"Plunge Cut… │
│ DeWalt │ [{"Nail Gun","DPSB2IN1-XJ","DeWalt",true,129,null}, {"Table Saw",… │
└────────┴────────────────────────────────────────────────────────────────────┘
```



---

**<font style="color:#01B2BC;">先物质化，再排序(Materialize First, Sort Second)</font>**

有时候，就像上面的代码片段一样，事情会变得有些复杂，让你怀疑自己是否在正确地排序。在这种情况下，先使用 `df.with_columns()` 方法构造一个新列来检查你正在排序的值是很有帮助的：

```python
tools_collection.with_columns(
    mean_price=pl.col("collection").list.eval(
        pl.element().struct.field("price")
    ).list.mean()
).sort("mean_price")
```

```plain
shape: (3, 3)
┌────────┬──────────────────────────────────────┬────────────┐
│ brand  │ collection                           │ mean_price │
│ ---    │ ---                                  │ ---        │
│ str    │ list[struct[6]]                      │ f64        │
╞════════╪══════════════════════════════════════╪════════════╡
│ Bosch  │ [{"Miter Saw","GCM 8 SJL","Bosch",f… │ 218.333333 │
│ Makita │ [{"Rotary Hammer","HR2230","Makita"… │ 248.4      │
│ DeWalt │ [{"Nail Gun","DPSB2IN1-XJ","DeWalt"… │ 322.5      │
└────────┴──────────────────────────────────────┴────────────┘
```

结果表明我们确实在正确的值上进行了排序。现在，我们可以安全地将 `df.with_columns()` 重新转换为 `df.sort()`。

---

<h2 id="WjliR">行操作的其他相关方法</h2>
除了过滤和排序，还有一些值得了解的相关行操作：

<h3 id="Zb1KV">过滤缺失值</h3>
有时，您的分析或机器学习算法无法处理缺失值。`df.drop_nulls()` 方法仅保留没有缺失值的行。您可以指定应考虑哪些列。例如：

```python
tools.drop_nulls("rpm").height
```

```plain
9
```



默认情况下，会考虑所有列，在这种情况下，效果等同于：

```python
df.filter(pl.all_horizontal(pl.all().is_not_null()))
```



<h3 id="DjH8h">切片(Slicing)</h3>
有时，您希望根据它们在 DataFrame 中的位置保留行，而不考虑它们包含的值。这通常称为切片（slicing），并且有几种方法可以实现：

+ 使用 `df.head()` 和 `df.tail()` 您可以分别保留前几行或后几行。例如：前五行。
+ 使用 `df.slice()` 您可以保留一定范围的行。例如，从第三行到第七行。
+ 使用 `df.gather()` 您可以保留特定的行。例如，第一行、第二行和第五行。
+ 使用 `df.gather_every()` 您可以按固定间隔保留行。例如，每隔一行保留一次。

您当然可以组合这些方法来创建复杂的切片。例如：

```python
(
    tools.with_row_index()
    .gather_every(2).head(3)
)
```

```plain
shape: (3, 7)
┌───────┬────────────────┬─────────────┬───┬──────────┬───────┬──────┐
│ index │ tool           │ product     │ … │ cordless │ price │ rpm  │
│ ---   │ ---            │ ---         │   │ ---      │ ---   │ ---  │
│ u32   │ str            │ str         │   │ bool     │ i64   │ i64  │
╞═══════╪════════════════╪═════════════╪═══╪══════════╪═══════╪══════╡
│ 0     │ Rotary Hammer  │ HR2230      │ … │ false    │ 199   │ 1050 │
│ 2     │ Plunge Cut Saw │ DSP600ZJ    │ … │ true     │ 459   │ 6300 │
│ 4     │ Jigsaw         │ PST 900 PEL │ … │ false    │ 79    │ 3100 │
└───────┴────────────────┴─────────────┴───┴──────────┴───────┴──────┘
```

该方法 `df.with_row_index()` 用于明确保留了哪些行位置。

<h3 id="xC4iJ">前几行与后几行</h3>
使用 `df.top_k()` 和 `df.bottom_k()` 方法，您可以保留值最大的或最小的前几行。例如，要保留价格最高的前三个工具：

```python
tools.top_k(3, by="price")
```

```plain
shape: (3, 6)
┌────────────────┬───────────┬────────┬──────────┬───────┬──────┐
│ tool           │ product   │ brand  │ cordless │ price │ rpm  │
│ ---            │ ---       │ ---    │ ---      │ ---   │ ---  │
│ str            │ str       │ str    │ bool     │ i64   │ i64  │
╞════════════════╪═══════════╪════════╪══════════╪═══════╪══════╡
│ Table Saw      │ DWE7485   │ DeWalt │ false    │ 516   │ 5800 │
│ Plunge Cut Saw │ DSP600ZJ  │ Makita │ true     │ 459   │ 6300 │
│ Miter Saw      │ GCM 8 SJL │ Bosch  │ false    │ 391   │ 5500 │
└────────────────┴───────────┴────────┴──────────┴───────┴──────┘
```

该代码本质上等同于 `tools.sort("price", descending=True)` 后跟 `tools.head(3)`。

<h3 id="OCmvO">抽样</h3>
`df.sample()` 方法根据随机性过滤行。例如，要只保留 20% 的行：

```python
tools.sample(fraction=0.2)
```

```plain
shape: (2, 6)
┌───────────────┬──────────────┬────────┬──────────┬───────┬───────┐
│ tool          │ product      │ brand  │ cordless │ price │ rpm   │
│ ---           │ ---          │ ---    │ ---      │ ---   │ ---   │
│ str           │ str          │ str    │ bool     │ i64   │ i64   │
╞═══════════════╪══════════════╪════════╪══════════╪═══════╪═══════╡
│ Rotary Hammer │ HR2230       │ Makita │ false    │ 199   │ 1050  │
│ Router        │ POF 1400 ACE │ Bosch  │ false    │ 185   │ 28000 │
└───────────────┴──────────────┴────────┴──────────┴───────┴───────┘
```



<h3 id="Im0ME">半连接（Semi Joins）</h3>
另一种过滤方式是与另一个 DataFrame 进行半连接。例如，假设你有一个名为 `saws` 的 DataFrame，其中包含所有类型的锯子。你可以使用这个 DataFrame 来仅保留 `tools` DataFrame 中的锯子：

```python
saws = pl.DataFrame({"tool": ["Table Saw", "Plunge Cut Saw", "Miter Saw",
                              "Jigsaw", "Bandsaw", "Chainsaw", "Seesaw"]})
tools.join(saws, how="semi", on="tool")
```

```plain
shape: (4, 6)
┌────────────────┬─────────────┬────────┬──────────┬───────┬──────┐
│ tool           │ product     │ brand  │ cordless │ price │ rpm  │
│ ---            │ ---         │ ---    │ ---      │ ---   │ ---  │
│ str            │ str         │ str    │ bool     │ i64   │ i64  │
╞════════════════╪═════════════╪════════╪══════════╪═══════╪══════╡
│ Miter Saw      │ GCM 8 SJL   │ Bosch  │ false    │ 391   │ 5500 │
│ Plunge Cut Saw │ DSP600ZJ    │ Makita │ true     │ 459   │ 6300 │
│ Jigsaw         │ PST 900 PEL │ Bosch  │ false    │ 79    │ 3100 │
│ Table Saw      │ DWE7485     │ DeWalt │ false    │ 516   │ 5800 │
└────────────────┴─────────────┴────────┴──────────┴───────┴──────┘
```

在第 13 章您将会深入学习**连接(Joining)**。

<h2 id="NV5mQ">本章要点</h2>
在本章中，我们讨论了过滤和排序行以及一些相关操作。关键要点如下：

+ 基于表达式的过滤为你提供了最大的灵活性。
+ 进行过滤时，表达式必须评估为布尔序列（Boolean Series）。
+ 基于约束的过滤有许多限制。
+ 由逗号分隔的表达式、列名和约束在底层是通过 AND 运算符（`&`）组合在一起的。
+ 基于单列的排序通常就足够了。
+ 使用 `descending=True` 反转默认的排序顺序。
+ 要排序嵌套数据类型，首先需要创建或提取可排序的值。
+ 有许多相关的行操作，包括切片和抽样。

在下一章中，我们将探讨如何处理特殊数据类型，例如字符串、分类数据和时间数据。

> 1. 本章中介绍的方法也可以应用于 LazyFrames。
>

