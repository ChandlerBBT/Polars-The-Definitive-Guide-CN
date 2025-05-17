

现在你已经对表达式的工作方式有了扎实的理解，接下来让我们看看如何使用它们。在本章中，我们将介绍与DataFrame列相关的操作。（下一章将关注与DataFrame行相关的操作。）我们将重点讨论选择现有列和创建新列，这可能是处理数据时最常见的操作。

首先，我们将重新讨论如何使用我们在**第6章**中已经见过的 `pl.col()` 函数选择列。然后，我们将向你介绍一种更灵活的选择列的方法：使用所谓的选择器。选择器提供了各种方式来根据列的名称、数据类型和位置指定列。它们也可以以不同的方式组合。接着，我们将介绍如何创建新列。最后，我们将简要讨论与列相关的其他操作，例如重命名列和组合两个DataFrame的列。

在本章中，你将学习如何：

+ 使用 `df.select()` 方法选择列
+ 使用选择器根据列名、数据类型和位置指定列
+ 使用集合操作组合选择器
+ 重新排列列
+ 使用 `df.with_columns()` 方法创建一个或多个新列
+ 删除列
+ 重命名列

你将使用一个关于我们最喜欢的《星球大战》宇宙中的反叛者的DataFrame进行练习。

```python
starwars = pl.read_parquet("data/starwars.parquet")
rebels = (
    starwars
    .drop("films")
    .filter(pl.col("name").is_in(["Luke Skywalker", "Leia Organa", "Han Solo"]))
)

print(rebels[:,:6])
print(rebels[:,6:11])
print(rebels[:,11:])
```

```plain
shape: (3, 6)
┌────────────────┬────────┬──────┬────────────┬────────────┬───────────┐
│ name           │ height │ mass │ hair_color │ skin_color │ eye_color │
│ ---            │ ---    │ ---  │ ---        │ ---        │ ---       │
│ str            │ u16    │ f64  │ str        │ str        │ str       │
╞════════════════╪════════╪══════╪════════════╪════════════╪═══════════╡
│ Han Solo       │ 180    │ 80.0 │ brown      │ fair       │ brown     │
│ Leia Organa    │ 150    │ 49.0 │ brown      │ light      │ brown     │
│ Luke Skywalker │ 172    │ 77.0 │ blond      │ fair       │ blue      │
└────────────────┴────────┴──────┴────────────┴────────────┴───────────┘
shape: (3, 5)
┌────────────┬────────┬───────────┬───────────┬─────────┐
│ birth_year │ sex    │ gender    │ homeworld │ species │
│ ---        │ ---    │ ---       │ ---       │ ---     │
│ f64        │ cat    │ cat       │ str       │ str     │
╞════════════╪════════╪═══════════╪═══════════╪═════════╡
│ 29.0       │ male   │ masculine │ Corellia  │ Human   │
│ 19.0       │ female │ feminine  │ Alderaan  │ Human   │
│ 19.0       │ male   │ masculine │ Tatooine  │ Human   │
└────────────┴────────┴───────────┴───────────┴─────────┘
shape: (3, 4)
┌───────────────────────┬───────────────────────┬────────────┬──────────────┐
│ vehicles              │ starships             │ birth_date │ screen_time  │
│ ---                   │ ---                   │ ---        │ ---          │
│ list[str]             │ list[str]             │ date       │ duration[μs] │
╞═══════════════════════╪═══════════════════════╪════════════╪══════════════╡
│ null                  │ ["Millennium Falcon"… │ 1948-06-01 │ 1h 12m 37s   │
│ ["Imperial Speeder B… │ null                  │ 1958-05-30 │ 1h 3m 40s    │
│ ["Snowspeeder", "Imp… │ ["X-wing", "Imperial… │ 1958-05-30 │ 1h 58m 44s   │
└───────────────────────┴───────────────────────┴────────────┴──────────────┘
```



`**rebels**` 数据框有三行十五列，包含各种数据类型，正好适合我们演示各种列操作。这个数据框来自 R 包 `dplyr`。我们添加了两列：`birth_date` 和 `screen_time`_<font style="color:#E746A4;"></font>_

<h2 id="lkAVy">选择列</h2>
可以使用 `df.select()` 方法选择 DataFrame 的列。你可以选择一列、多列，或所有列，顺序随意。如果你确保它们没有相同的名称，你甚至可以多次选择同一列。（列名必须唯一。）稍后我们还会看到，使用此方法你甚至可以创建新列。图 9-1 概念性地展示了此操作。

![图 9-1 选择操作根据列名、数据类型或位置保留列](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728820596943-01608b2c-91ff-4ba6-b909-6461672cd959.png)



选择列在你有很多列时特别有用，并且你知道有一些列将不再需要。

要选择列，你需要指定要保留哪些列。在**第6章**中，你已经看到了两种方法：使用列名作为字符串，或者使用 `pl.col()` 方法。前者的优势是输入更简短；后者的优势是它是一个表达式，提供了定制列的能力。这里有一个示例来帮助你回忆：

```python
rebels.select(
    "name",
    pl.col("homeworld"),
    pl.col("^.*_color$"),
    (pl.col("height") / 100).alias("height_m")
)
```

```plain
shape: (3, 6)
┌────────────────┬───────────┬────────────┬────────────┬───────────┬──────────┐
│ name           │ homeworld │ hair_color │ skin_color │ eye_color │ height_m │
│ ---            │ ---       │ ---        │ ---        │ ---       │ ---      │
│ str            │ str       │ str        │ str        │ str       │ f64      │
╞════════════════╪═══════════╪════════════╪════════════╪═══════════╪══════════╡
│ Han Solo       │ Corellia  │ brown      │ fair       │ brown     │ 1.8      │
│ Leia Organa    │ Alderaan  │ brown      │ light      │ brown     │ 1.5      │
│ Luke Skywalker │ Tatooine  │ blond      │ fair       │ blue      │ 1.72     │
└────────────────┴───────────┴────────────┴────────────┴───────────┴──────────┘
```

请注意，列将按你指定的顺序显示。

在下一节中，我们将介绍第三种指定所需列的方法：使用选择器。

<h3 id="iFz98">选择器简介</h3>
解释选择器的最佳方式是展示它们的用法。要使用选择器，首先需要导入 `polars.selectors` 子模块：

```python
import polars.selectors as cs
```

通常将其导入为 `cs`，代表列选择器（column selectors）。在这个子模块中，有超过30个函数可用于指定列。

---

**<font style="color:#1DC0C9;">选择器无处不在</font>**

选择器不仅可以与 `df.select()` 方法一起使用，还可以在任何可以指定列的地方使用，包括 `df.filter()` 和 `df.group_by()`。

---



最像 `pl.col()` 函数的选择器函数是 `cs.by_name()`。因此，我们之前的示例可以重写为：

```python
rebels.select(
    "name",
    cs.by_name("homeworld"),
    cs.by_name("^.*_color$"),
    (cs.by_name("height") / 100).alias("height_m")
)
```

```plain
shape: (3, 6)
┌────────────────┬───────────┬────────────┬────────────┬───────────┬──────────┐
│ name           │ homeworld │ hair_color │ skin_color │ eye_color │ height_m │
│ ---            │ ---       │ ---        │ ---        │ ---       │ ---      │
│ str            │ str       │ str        │ str        │ str       │ f64      │
╞════════════════╪═══════════╪════════════╪════════════╪═══════════╪══════════╡
│ Han Solo       │ Corellia  │ brown      │ fair       │ brown     │ 1.8      │
│ Leia Organa    │ Alderaan  │ brown      │ light      │ brown     │ 1.5      │
│ Luke Skywalker │ Tatooine  │ blond      │ fair       │ blue      │ 1.72     │
└────────────────┴───────────┴────────────┴────────────┴───────────┴──────────┘
```



单独使用此函数其实并不能真正展现选择器的优势。（它比 `pl.col()` 的输入还要长。）选择器的灵活性在与其他函数结合并使用集合操作时变得更加清晰。

在接下来的三节中，我们将介绍三种类型的选择器：根据列名、列数据类型和位置进行指定。

<h3 id="WuE4G">基于名称选择列</h3>
除了 `cs.by_name()` 函数外，还有一些其他函数允许你根据列的名称指定列。这些函数列在表 9-1 中。

__

_表 9-1：根据名称选择列的函数_

| 函数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">cs.alpha(…)</font>**` | 选择所有名称为字母的列（如：只包含字母的列）。 |
| `**<font style="color:#1DC0C9;">cs.alphanumeric(…)</font>**` | 选择所有名称为字母数字的列（如：只包含字母和数字0-9的列）。 |
| `**<font style="color:#1DC0C9;">cs.by_name(…)</font>**` | 选择所有匹配给定名称的列。 |
| `**<font style="color:#1DC0C9;">cs.contains(…)</font>**` | 选择名称包含给定子字符串的列。 |
| `**<font style="color:#1DC0C9;">cs.digit(…)</font>**` | 选择名称仅由数字组成的所有列。 |
| `**<font style="color:#1DC0C9;">cs.ends_with(…)</font>**` | 选择名称以给定子字符串结尾的列。 |
| `**<font style="color:#1DC0C9;">cs.matches(…)</font>**` | 选择所有匹配给定正则表达式模式的列。 |
| `**<font style="color:#1DC0C9;">cs.starts_with(…)</font>**` | 选择名称以给定子字符串开头的列。 |


以下是一些示例。要选择所有名称以 `birth_` 开头的列，使用 `cs.starts_with()` 函数：

```python
rebels.select(cs.starts_with("birth_"))
```

```plain
shape: (3, 2)
┌────────────┬────────────┐
│ birth_year │ birth_date │
│ ---        │ ---        │
│ f64        │ date       │
╞════════════╪════════════╡
│ 29.0       │ 1948-06-01 │
│ 19.0       │ 1958-05-30 │
│ 19.0       │ 1958-05-30 │
└────────────┴────────────┘
```



要选择所有名称以 `_color` 结尾的列，使用 `cs.ends_with()` 函数：

```python
rebels.select(cs.ends_with("_color"))
```

```plain
shape: (3, 3)
┌────────────┬────────────┬───────────┐
│ hair_color │ skin_color │ eye_color │
│ ---        │ ---        │ ---       │
│ str        │ str        │ str       │
╞════════════╪════════════╪═══════════╡
│ brown      │ fair       │ brown     │
│ brown      │ light      │ brown     │
│ blond      │ fair       │ blue      │
└────────────┴────────────┴───────────┘
```



要根据子字符串匹配名称，使用 `cs.contains()` 函数：

```python
rebels.select(cs.contains("_"))
```

```plain
shape: (3, 6)
┌────────────┬────────────┬───────────┬────────────┬────────────┬──────────────┐
│ hair_color │ skin_color │ eye_color │ birth_year │ birth_date │ screen_time  │
│ ---        │ ---        │ ---       │ ---        │ ---        │ ---          │
│ str        │ str        │ str       │ f64        │ date       │ duration[μs] │
╞════════════╪════════════╪═══════════╪════════════╪════════════╪══════════════╡
│ brown      │ fair       │ brown     │ 29.0       │ 1948-06-01 │ 1h 12m 37s   │
│ brown      │ light      │ brown     │ 19.0       │ 1958-05-30 │ 1h 3m 40s    │
│ blond      │ fair       │ blue      │ 19.0       │ 1958-05-30 │ 1h 58m 44s   │
└────────────┴────────────┴───────────┴────────────┴────────────┴──────────────┘
```



`cs.matches()` 函数允许你使用正则表达式指定列。例如，要选择名称由四个小写字母组成的列：

```python
rebels.select(cs.matches("^[a-z]{4}$"))
```

```plain
shape: (3, 2)
┌────────────────┬──────┐
│ name           │ mass │
│ ---            │ ---  │
│ str            │ f64  │
╞════════════════╪══════╡
│ Han Solo       │ 80.0 │
│ Leia Organa    │ 49.0 │
│ Luke Skywalker │ 77.0 │
└────────────────┴──────┘
```

接下来让我们看看能基于列的数据类型筛选列的方法。

<h3 id="NBUGP">基于数据类型选择列</h3>
第二类选择器是基于列的数据类型。有些情况下，你可能更关心列的数据类型，而不是列名。比如，当你想要对DataFrame中的所有数值型列进行汇总并计算它们的均值时：

```python
rebels.group_by("hair_color").agg(cs.numeric().mean())
```

```plain
shape: (2, 4)
┌────────────┬────────┬──────┬────────────┐
│ hair_color │ height │ mass │ birth_year │
│ ---        │ ---    │ ---  │ ---        │
│ str        │ f64    │ f64  │ f64        │
╞════════════╪════════╪══════╪════════════╡
│ brown      │ 165.0  │ 64.5 │ 24.0       │
│ blond      │ 172.0  │ 77.0 │ 19.0       │
└────────────┴────────┴──────┴────────────┘
```

`cs.numeric()`函数可以选择所有无符号整数、有符号整数以及浮点数。同样，`cs.temporal()`函数可以选择所有日期、时间、日期时间和持续时间列，非常方便。表9-2列出了与数据类型相关的所有选择器函数。

_表9-2. 基于数据类型选择列的函数_

| 函数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">cs.binary()</font>**` | 选择所有二进制列。 |
| `**<font style="color:#1DC0C9;">cs.boolean()</font>**` | 选择所有布尔列。 |
| `**<font style="color:#1DC0C9;">cs.by_dtype(…)</font>**` | 选择所有与指定数据类型匹配的列。 |
| `**<font style="color:#1DC0C9;">cs.categorical()</font>**` | 选择所有分类列。 |
| `**<font style="color:#1DC0C9;">cs.date()</font>**` | 选择所有日期列。 |
| `**<font style="color:#1DC0C9;">cs.datetime(…)</font>**` | 选择所有日期时间列，可选按时间单位/时区筛选。 |
| `**<font style="color:#1DC0C9;">cs.decimal()</font>**` | 选择所有十进制列。 |
| `**<font style="color:#1DC0C9;">cs.duration(…)</font>**` | 选择所有持续时间列，可选按时间单位筛选。 |
| `**<font style="color:#1DC0C9;">cs.float()</font>**` | 选择所有浮点数列。 |
| `**<font style="color:#1DC0C9;">cs.integer()</font>**` | 选择所有整数列。 |
| `**<font style="color:#1DC0C9;">cs.numeric()</font>**` | 选择所有数值列。 |
| `**<font style="color:#1DC0C9;">cs.signed_integer()</font>**` | 选择所有有符号整数列。 |
| `**<font style="color:#1DC0C9;">cs.string(…)</font>**` | 选择所有字符串列（可选包含分类字符串列）。 |
| `**<font style="color:#1DC0C9;">cs.temporal()</font>**` | 选择所有时间类型列。 |
| `**<font style="color:#1DC0C9;">cs.time()</font>**` | 选择所有时间列。 |
| `**<font style="color:#1DC0C9;">cs.unsigned_integer()</font>**` | 选择所有无符号整数列。 |


更多示例：

```python
rebels.select(cs.string())
```

```plain
shape: (3, 6)
┌────────────────┬────────────┬────────────┬───────────┬───────────┬─────────┐
│ name           │ hair_color │ skin_color │ eye_color │ homeworld │ species │
│ ---            │ ---        │ ---        │ ---       │ ---       │ ---     │
│ str            │ str        │ str        │ str       │ str       │ str     │
╞════════════════╪════════════╪════════════╪═══════════╪═══════════╪═════════╡
│ Han Solo       │ brown      │ fair       │ brown     │ Corellia  │ Human   │
│ Leia Organa    │ brown      │ light      │ brown     │ Alderaan  │ Human   │
│ Luke Skywalker │ blond      │ fair       │ blue      │ Tatooine  │ Human   │
└────────────────┴────────────┴────────────┴───────────┴───────────┴─────────┘
```

```python
rebels.select(cs.temporal())
```

```plain
shape: (3, 2)
┌────────────┬──────────────┐
│ birth_date │ screen_time  │
│ ---        │ ---          │
│ date       │ duration[μs] │
╞════════════╪══════════════╡
│ 1948-06-01 │ 1h 12m 37s   │
│ 1958-05-30 │ 1h 3m 40s    │
│ 1958-05-30 │ 1h 58m 44s   │
└────────────┴──────────────┘
```



嵌套数据类型更为繁琐，因为你还需要指定内部的数据类型：

```python
rebels.select(cs.by_dtype(pl.List(pl.String)))
```

```plain
shape: (3, 2)
┌───────────────────────┬───────────────────────┐
│ vehicles              │ starships             │
│ ---                   │ ---                   │
│ list[str]             │ list[str]             │
╞═══════════════════════╪═══════════════════════╡
│ null                  │ ["Millennium Falcon"… │
│ ["Imperial Speeder B… │ null                  │
│ ["Snowspeeder", "Imp… │ ["X-wing", "Imperial… │
└───────────────────────┴───────────────────────┘
```



<h3 id="kOJYQ">基于位置选择列</h3>
第三类选择器基于列的位置进行选择。表9-3列出了与位置相关的三个选择器函数。

_表9-3. 基于位置选择列的函数_

| 函数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">cs.by_index(…)</font>**` | 选择所有与给定索引（或范围对象）匹配的列。 |
| `**<font style="color:#1DC0C9;">cs.first()</font>**` | 选择当前范围内的第一列。 |
| `**<font style="color:#1DC0C9;">cs.last()</font>**` | 选择当前范围内的最后一列。 |


在很少的情况下你会使用这种选择方式。一个例子是，当你有一个包含许多列的DataFrame，这些列遵循某种模式，但列名不够具体以区分开来。在这种情况下，你可以使用`cs.by_index()`函数和范围对象来选择每隔三列的列。在我们的`rebels` DataFrame中，没有什么能将每隔三列的列关联起来，但如果我们想选择每隔三列的列，可以这样做：

```python
rebels.select(cs.by_index(range(0, 999, 3))) ①
```

```plain
shape: (3, 5)
┌────────────────┬────────────┬────────────┬───────────┬───────────────────────┐
│ name           │ hair_color │ birth_year │ homeworld │ starships             │
│ ---            │ ---        │ ---        │ ---       │ ---                   │
│ str            │ str        │ f64        │ str       │ list[str]             │
╞════════════════╪════════════╪════════════╪═══════════╪═══════════════════════╡
│ Han Solo       │ brown      │ 29.0       │ Corellia  │ ["Millennium Falcon"… │
│ Leia Organa    │ brown      │ 19.0       │ Alderaan  │ null                  │
│ Luke Skywalker │ blond      │ 19.0       │ Tatooine  │ ["X-wing", "Imperial… │
└────────────────┴────────────┴────────────┴───────────┴───────────────────────┘
```

① 范围的结束值并不重要，只要它大于列的总数即可。



另一个例子：假设你刚刚使用`df.with_columns()`方法创建了两个新列（将在下一节介绍），并且你想选择这些新列以及`name`列。因为你知道这些新列被添加到了DataFrame的末尾，你可以这样操作：



```python
rebels.select("name", cs.by_index(range(-2, 0)))
```

```plain
shape: (3, 3)
┌────────────────┬────────────┬──────────────┐
│ name           │ birth_date │ screen_time  │
│ ---            │ ---        │ ---          │
│ str            │ date       │ duration[μs] │
╞════════════════╪════════════╪══════════════╡
│ Han Solo       │ 1948-06-01 │ 1h 12m 37s   │
│ Leia Organa    │ 1958-05-30 │ 1h 3m 40s    │
│ Luke Skywalker │ 1958-05-30 │ 1h 58m 44s   │
└────────────────┴────────────┴──────────────┘
```

如果只有一列，您也可以使用 `cs.last()`方法。

---

**<font style="color:#ECAA04;">超出范围</font>**

如果你将超出范围的索引传递给`cs.index()`，你会得到一个错误：

```python
rebels.select(cs.by_index(20))
```

```plain
ColumnNotFoundError: nth
```



然而，如果你传递的是部分或完全超出范围的范围对象（例如`range(20, 22)`），则不会报错；你只会得到一个空的DataFrame。

```python
rebels.select(cs.by_index(range(20, 22)))
```

```plain
shape: (0, 0)
┌┐
╞╡
└┘
```

---



<h3 id="rtAiu">**组合选择器**</h3>
选择器可以组合使用，以进行复杂的选择。这时候它们的灵活性就变得明显了：选择器允许你以之前无法做到的方式来指定列。

例如，在第6章中，我们演示了无法同时通过列名和数据类型来指定列。而使用选择器，这就变得可能了。假设你想选择列名为`hair_color`的列以及所有数值类型的列，可以这样操作：

```python
rebels.select(cs.by_name("hair_color") | cs.numeric())
```

```plain
shape: (3, 4)
┌────────┬──────┬────────────┬────────────┐
│ height │ mass │ hair_color │ birth_year │
│ ---    │ ---  │ ---        │ ---        │
│ u16    │ f64  │ str        │ f64        │
╞════════╪══════╪════════════╪════════════╡
│ 180    │ 80.0 │ brown      │ 29.0       │
│ 150    │ 49.0 │ brown      │ 19.0       │
│ 172    │ 77.0 │ blond      │ 19.0       │
└────────┴──────┴────────────┴────────────┘
```



字符 `|` 是 OR 操作符，它是支持选择器的五个集合操作符之一，表9-4中列出了这些操作符及其描述。

_表9-4. 用于组合选择器的集合操作符_

| 操作 | 内联操作符 | 描述 |
| --- | --- | --- |
| **<font style="color:#1DC0C9;">并集</font>** | `**<font style="color:#1DC0C9;">|</font>**` | x 或 y 或两个都包含 |
| **<font style="color:#1DC0C9;">交集</font>** | `**<font style="color:#1DC0C9;">&</font>**` | x 和 y |
| **<font style="color:#1DC0C9;">差集</font>** | `**<font style="color:#1DC0C9;">-</font>**` | x 并且不在 y 中 |
| **<font style="color:#1DC0C9;">异或</font>** | `**<font style="color:#1DC0C9;">^</font>**` | x 或 y，但不能同时存在 |
| **<font style="color:#1DC0C9;">否定</font>** | `**<font style="color:#1DC0C9;">~</font>**` | 不在 x 中 |


下面的代码片段演示了这两个选择器 x 和 y 应用于玩具 DataFrame `df` 时，五个集合操作符的用法：

```python
df = pl.DataFrame({"d": 1, "i": True, "s": True, "c": True, "o": 1.0})

print(df)

x = cs.by_name("d", "i", "s")
y = cs.boolean()

print("\nselector => columns")

for s in ["x", "y", "x | y", "x & y", "x - y", "x ^ y", "~x", "x - x"]:
    print(f"{s:8} => {cs.expand_selector(df, eval(s))}")
```

```plain
shape: (1, 5)
┌─────┬──────┬──────┬──────┬─────┐
│ d   │ i    │ s    │ c    │ o   │
│ --- │ ---  │ ---  │ ---  │ --- │
│ i64 │ bool │ bool │ bool │ f64 │
╞═════╪══════╪══════╪══════╪═════╡
│ 1   │ true │ true │ true │ 1.0 │
└─────┴──────┴──────┴──────┴─────┘

selector => columns
x        => ('d', 'i', 's')
y        => ('i', 's', 'c')
x | y    => ('d', 'i', 's', 'c')
x & y    => ('i', 's')
x - y    => ('d',)
x ^ y    => ('d', 'c')
~x       => ('c', 'o')
x - x    => ()
```



虽然我们称它们为集合操作符，但结果实际上是元组（tuples）。这点很有用，因为使用这些操作符时，会保留列的顺序。

`x - x` 这个选择器说明了你可能最终选择到没有任何列，如果将其应用于一个DataFrame，结果将是一个空的DataFrame。

```python
df.select(x - x)
```

```plain
shape: (0, 0)
┌┐
╞╡
└┘
```



---

**<font style="color:#1DC0C9;">调出指定列</font>**

这里有一个很巧妙的代码片段，使用Python 3.8引入的海象运算符（`:=`）可以轻松地将特定列移到前面。

```python
print(df.select(first := cs.by_name("c", "i"), ~first))
print(df.select(first := cs.last(), ~first))
```

```plain
shape: (1, 5)
┌──────┬──────┬─────┬──────┬─────┐
│ c    │ i    │ d   │ s    │ o   │
│ ---  │ ---  │ --- │ ---  │ --- │
│ bool │ bool │ i64 │ bool │ f64 │
╞══════╪══════╪═════╪══════╪═════╡
│ true │ true │ 1   │ true │ 1.0 │
└──────┴──────┴─────┴──────┴─────┘
shape: (1, 5)
┌─────┬─────┬──────┬──────┬──────┐
│ o   │ d   │ i    │ s    │ c    │
│ --- │ --- │ ---  │ ---  │ ---  │
│ f64 │ i64 │ bool │ bool │ bool │
╞═════╪═════╪══════╪══════╪══════╡
│ 1.0 │ 1   │ true │ true │ true │
└─────┴─────┴──────┴──────┴──────┘
```

海象运算符允许你先在第一个参数中先将选择器分配给一个变量，然后在第二个参数中重用并修改该变量。

第二行特别有用，尤其是在你刚创建新列之后，这将是下一节的主题。

---

<h2 id="hJego">**创建列**</h2>
你可以使用`df.with_columns()`方法创建一个或多个新列。新列会被添加到DataFrame的最右端。图9-2概念性地展示了这个操作。

![图9-2 在最右边新增一列](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728822541277-ee5f1a72-055a-4f82-80dd-713da943f1b7.png)

例如，使用`mass`和`height`列，我们可以计算每个反叛者的体重指数（BMI）：

```python
rebels.with_columns(bmi=pl.col("mass") / ((pl.col("height") / 100) ** 2))
```

```plain
shape: (3, 16)
┌────────────────┬────────┬──────┬───┬────────────┬──────────────┬───────────┐
│ name           │ height │ mass │ … │ birth_date │ screen_time  │ bmi       │
│ ---            │ ---    │ ---  │   │ ---        │ ---          │ ---       │
│ str            │ u16    │ f64  │   │ date       │ duration[μs] │ f64       │
╞════════════════╪════════╪══════╪═══╪════════════╪══════════════╪═══════════╡
│ Han Solo       │ 180    │ 80.0 │ … │ 1948-06-01 │ 1h 12m 37s   │ 24.691358 │
│ Leia Organa    │ 150    │ 49.0 │ … │ 1958-05-30 │ 1h 3m 40s    │ 21.777778 │
│ Luke Skywalker │ 172    │ 77.0 │ … │ 1958-05-30 │ 1h 58m 44s   │ 26.027582 │
└────────────────┴────────┴──────┴───┴────────────┴──────────────┴───────────┘
```



在这个例子中，所有原始列都被保留，新的列被添加到末尾。

你也可以在一次`df.with_columns()`调用中创建多个新列。我们还可以计算这些反叛者在死星主反应堆被摧毁时的年龄：

```python
from datetime import datetime

rebels.with_columns(
    bmi=pl.col("mass") / ((pl.col("height") / 100) ** 2),
    age_destroy=((datetime(1983, 5, 25) - pl.col("birth_date"))
                .dt.total_days() / 356).cast(pl.UInt8)
)
```

```plain
shape: (3, 17)
┌────────────────┬────────┬──────┬───┬──────────────┬───────────┬─────────────┐
│ name           │ height │ mass │ … │ screen_time  │ bmi       │ age_destroy │
│ ---            │ ---    │ ---  │   │ ---          │ ---       │ ---         │
│ str            │ u16    │ f64  │   │ duration[μs] │ f64       │ u8          │
╞════════════════╪════════╪══════╪═══╪══════════════╪═══════════╪═════════════╡
│ Han Solo       │ 180    │ 80.0 │ … │ 1h 12m 37s   │ 24.691358 │ 35          │
│ Leia Organa    │ 150    │ 49.0 │ … │ 1h 3m 40s    │ 21.777778 │ 25          │
│ Luke Skywalker │ 172    │ 77.0 │ … │ 1h 58m 44s   │ 26.027582 │ 25          │
└────────────────┴────────┴──────┴───┴──────────────┴───────────┴─────────────┘
```



表达式不能相互依赖，因为它们是并行执行的。这意味着如果你想创建两个新列，第二个新列不能依赖于第一个新列。例如，下面的代码片段中，我们试图添加BMI分类列，但会产生错误，因为找不到BMI列：

```python
rebels.with_columns(
    bmi=pl.col("mass") / ((pl.col("height") / 100) ** 2),
    bmi_cat=pl.col("bmi").cut([18.5, 25], labels=["Underweight",
                                                  "Normal",
                                                  "Overweight"])
)
```

```plain
ColumnNotFoundError: bmi
```

** **

解决办法是多次使用`df.with_columns()`方法：

```python
(
    rebels
    .with_columns(bmi=pl.col("mass") / ((pl.col("height") / 100) ** 2))
    .with_columns(bmi_cat=pl.col("bmi").cut([18.5, 25], labels=["Underweight",
                                                                "Normal",
                                                                "Overweight"]))
)
```

```plain
shape: (3, 17)
┌────────────────┬────────┬──────┬───┬──────────────┬───────────┬────────────┐
│ name           │ height │ mass │ … │ screen_time  │ bmi       │ bmi_cat    │
│ ---            │ ---    │ ---  │   │ ---          │ ---       │ ---        │
│ str            │ u16    │ f64  │   │ duration[μs] │ f64       │ cat        │
╞════════════════╪════════╪══════╪═══╪══════════════╪═══════════╪════════════╡
│ Han Solo       │ 180    │ 80.0 │ … │ 1h 12m 37s   │ 24.691358 │ Normal     │
│ Leia Organa    │ 150    │ 49.0 │ … │ 1h 3m 40s    │ 21.777778 │ Normal     │
│ Luke Skywalker │ 172    │ 77.0 │ … │ 1h 58m 44s   │ 26.027582 │ Overweight │
└────────────────┴────────┴──────┴───┴──────────────┴───────────┴────────────┘
```

  
你还可以使用`df.select()`方法来创建新列。这允许你同时选择一部分列并创建新列。请记住，在Python中关键字参数必须出现在最后，所以如果你想让新列出现在某个中间位置，务必使用`Expr.alias()`方法来为其命名。让我们看看哪些《星球大战》角色拥有最高的BMI：

```python
(
    starwars
    .select(
        "name",
        (pl.col("mass") / ((pl.col("height") / 100) ** 2)).alias("bmi"),  ①
        "species"
    )
    .drop_nulls().top_k(5, by="bmi")  ②
)
```

```plain
shape: (5, 3)
┌───────────────────────┬────────────┬────────────────┐
│ name                  │ bmi        │ species        │
│ ---                   │ ---        │ ---            │
│ str                   │ f64        │ str            │
╞═══════════════════════╪════════════╪════════════════╡
│ Jabba Desilijic Tiure │ 443.428571 │ Hutt           │
│ Dud Bolt              │ 50.928022  │ Vulptereen     │
│ Yoda                  │ 39.02663   │ Yoda's species │
│ Owen Lars             │ 37.874006  │ Human          │
│ IG-88                 │ 35.0       │ Droid          │
└───────────────────────┴────────────┴────────────────┘
```

① 请注意，`mass`和`height`列本身不需要被选择。

② 在下一章中，你将学习更多关于`Expr.drop_nulls()`和`Expr.top_k()`方法的内容。



<h2 id="TeXzg">列操作的其他相关方法</h2>
除了选择和创建列之外，还有一些相关的列操作值得了解。



<h3 id="w4vAt">删除列</h3>
有时，指定不需要保留的列比指定所有需要保留的列更为方便。为此，可以使用`df.drop()`方法：

```python
rebels.drop("name", "films", "screen_time", strict=False)  ①
```

```plain
shape: (3, 13)
┌────────┬──────┬────────────┬───┬───────────┬───────────┬───────────┐
│ height │ mass │ hair_color │ … │ vehicles  │ starships │ birth_dat │
│ ---    │ ---  │ ---        │   │ ---       │ ---       │ e         │
│ u16    │ f64  │ str        │   │ list[str] │ list[str] │ ---       │
│        │      │            │   │           │           │ date      │
╞════════╪══════╪════════════╪═══╪═══════════╪═══════════╪═══════════╡
│ 180    │ 80.0 │ brown      │ … │ null      │ ["Millenn │ 1948-06-0 │
│        │      │            │   │           │ ium       │ 1         │
│        │      │            │   │           │ Falcon"…  │           │
│ 150    │ 49.0 │ brown      │ … │ ["Imperia │ null      │ 1958-05-3 │
│        │      │            │   │ l Speeder │           │ 0         │
│        │      │            │   │ B…        │           │           │
│ 172    │ 77.0 │ blond      │ … │ ["Snowspe │ ["X-wing" │ 1958-05-3 │
│        │      │            │   │ eder",    │ , "Imperi │ 0         │
│        │      │            │   │ "Imp…     │ al…       │           │
└────────┴──────┴────────────┴───┴───────────┴───────────┴───────────┘
```

①将关键字参数`strict`设置为`False`，可以让你在指定DataFrame中不存在的列时不会报错。

`df.drop()`方法可以说比以下方式更简单明了：

```python
rebels.select(~cs.by_name("name", "films", "screen_time"))
```

或：

```python
rebels.select(cs.exclude("name", "films", "screen_time"))
```



<h3 id="jztvB">重命名</h3>
从技术上讲，你可以使用`df.select()`方法来重命名列，但这种方式并不方便，因为你必须指定所有要重命名和保留的列。更好的方式是使用`df.rename()`方法，该方法可以接受一个字典或函数。下面的代码片段演示了这两种方法：

```python
(
    rebels
    .rename({"homeworld": "planet", "mass": "weight"})
    .rename(lambda s: s.removesuffix("_color"))
    .select("name", "planet", "weight", "hair", "skin", "eye")  ①
)
```

```plain
shape: (3, 6)
┌────────────────┬──────────┬────────┬───────┬───────┬───────┐
│ name           │ planet   │ weight │ hair  │ skin  │ eye   │
│ ---            │ ---      │ ---    │ ---   │ ---   │ ---   │
│ str            │ str      │ f64    │ str   │ str   │ str   │
╞════════════════╪══════════╪════════╪═══════╪═══════╪═══════╡
│ Han Solo       │ Corellia │ 80.0   │ brown │ fair  │ brown │
│ Leia Organa    │ Alderaan │ 49.0   │ brown │ light │ brown │
│ Luke Skywalker │ Tatooine │ 77.0   │ blond │ fair  │ blue  │
└────────────────┴──────────┴────────┴───────┴───────┴───────┘
```

① 这里的`df.select()`仅用于确保新列名彼此相邻显示。



<h3 id="u0aQo">合并(Stacking)</h3>
如果你有一个与第一个DataFrame长度相同的第二个DataFrame或一个或多个Series，那么你可以通过水平堆叠它们来进行合并，使用`df.hstack()`方法。让我们创建两个小的DataFrame和一个包含引用的新Series，并将它们合并在一起：

```python
rebel_names = rebels.select("name")
rebel_colors = rebels.select(cs.ends_with("_color"))
rebel_quotes = pl.Series("quote", ["You know, sometimes I amaze myself.",
                                   "That doesn't sound too hard.",
                                   "I have a bad feeling about this."])

(
    rebel_names
    .hstack(rebel_colors)
    .hstack([rebel_quotes]) ①
```

```plain
shape: (3, 5)
┌────────────────┬────────────┬────────────┬───────────┬──────────────────┐
│ name           │ hair_color │ skin_color │ eye_color │ quote            │
│ ---            │ ---        │ ---        │ ---       │ ---              │
│ str            │ str        │ str        │ str       │ str              │
╞════════════════╪════════════╪════════════╪═══════════╪══════════════════╡
│ Han Solo       │ brown      │ fair       │ brown     │ You know,        │
│                │            │            │           │ sometimes I      │
│                │            │            │           │ amaze myself.    │
│ Leia Organa    │ brown      │ light      │ brown     │ That doesn't     │
│                │            │            │           │ sound too hard.  │
│ Luke Skywalker │ blond      │ fair       │ blue      │ I have a bad     │
│                │            │            │           │ feeling about    │
│                │            │            │           │ this.            │
└────────────────┴────────────┴────────────┴───────────┴──────────────────┘
```

① 请确保传递的是Series的列表。

以这种方式合并DataFrame或Series时，你必须确保它们正确对齐，否则会得到错误的结果。在许多情况下，将DataFrame合并在一起更好，这部分内容将在第13章中学习。



<h3 id="nUxOp">添加行索引</h3>
如果你想添加一个递增整数的列，可以使用`df.with_row_index()`方法。此方法接受两个可选参数：`name`和`offset`，其默认值分别为“index”和0：

```python
rebels.with_row_index(name="rebel_id", offset=1)
```

```plain
shape: (3, 16)
┌──────────┬────────────────┬────────┬───┬────────────┬──────────────┐
│ rebel_id │ name           │ height │ … │ birth_date │ screen_time  │
│ ---      │ ---            │ ---    │   │ ---        │ ---          │
│ u32      │ str            │ u16    │   │ date       │ duration[μs] │
╞══════════╪════════════════╪════════╪═══╪════════════╪══════════════╡
│ 1        │ Han Solo       │ 180    │ … │ 1948-06-01 │ 1h 12m 37s   │
│ 2        │ Leia Organa    │ 150    │ … │ 1958-05-30 │ 1h 3m 40s    │
│ 3        │ Luke Skywalker │ 172    │ … │ 1958-05-30 │ 1h 58m 44s   │
└──────────┴────────────────┴────────┴───┴────────────┴──────────────┘
```



<h2 id="Vh06x">**要点总结**</h2>
在本章中，我们学习了选择和创建列以及一些相关操作。关键要点包括：

+ 你可以通过多种方式选择列。
+ 在许多情况下，使用`pl.col()`就足以指定列。
+ 选择器允许你根据列名、数据类型和位置来指定列。
+ 基于位置的选择通常不是一个好主意。
+ 选择器提供了更多灵活性，因为它们可以通过集合操作符组合使用。
+ 你可以使用`df.select()`或`df.with_columns()`创建新列。
+ 当创建多个有依赖关系的新列时，应该多次调用`df.with_columns()`。
+ 还有许多相关的列操作，包括重命名和堆叠。

在下一章中，我们将探讨如何过滤和排序行。

