

现在你已经了解了表达式的基础知识并掌握了各种扩展表达式的方法，是时候学习如何组合它们了。

每当你要构建的系列基于多个值或列时，组合表达式是必要的。这种情况比你想象的要常见：例如，当你想要计算两个浮点列的比率、基于多个条件过滤行，或将多个字符串列合并为一个。

事实上，你已经在前几章中多次组合表达式了。让我们看一个来自第6章的例子，来刷新你的记忆：

```python
fruit = pl.read_csv("data/fruit.csv")
fruit.filter(pl.col("is_round") & (pl.col("weight") > 1000))
```

```plain
shape: (2, 5)

┌─────────────┬────────┬────────┬──────────┬─────────┐
│ name        │ weight │ color  │ is_round │ origin  │
│ ---         │ ---    │ ---    │ ---      │ ---     │
│ str         │ i64    │ str    │ bool     │ str     │
├─────────────┼────────┼────────┼──────────┼─────────┤
│ Cantaloupe  │ 2500   │ orange │ true     │ Africa  │
│ Watermelon  │ 5000   │ green  │ true     │ Africa  │
└─────────────┴────────┴────────┴──────────┴─────────┘
```

这段代码在两步中将两个现有列（`is_round` 和 `weight`）以及一个值（`1000`）组合为一个表达式。`df.filter()` 方法随后使用该表达式来过滤行。

由于括号的组织方式，比较运算符大于 (`>`) 将 `weight` 列与值 `1000` 组合。当值更大时，它产生布尔值 `True`。第二步，布尔与运算符 (`&`) 将 `is_round` 列与第一步构造的系列组合。只有当它们都为 `True` 时，输出才为 `True`。`df.filter()` 方法将 `True` 解释为“保留这一行”。



这只是 Polars 中组合表达式的冰山一角。在本章中，你将学习内联运算符与方法链之间的区别。你还将学习如何通过以下方式组合表达式：

+ 通过算术运算，如加法和乘法
+ 通过比较，如大于或等于
+ 通过布尔代数，如合取和否定
+ 通过按位操作，如与和异或
+ 使用各种模块级函数



<h2 id="VB5y7">**内联运算符与方法**</h2>
在前两章中，你使用方法链来扩展表达式。要组合表达式，你通常可以使用内联运算符而不是方法链。两种方法产生相同的结果，如图 8-1 所示。

---

**<font style="color:#1DC0C9;">注意</font>**

虽然每个内联运算符都有对应的 `Expr` 方法，但并不是每个方法（或函数）都具有对应的内联运算符。组合表达式的示例包括方法 `Expr.dot()` 和函数 `pl.concat_list()`。

---

![图 8-1. 使用内联运算符或方法链组合表达式会产生相同的结果](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728817026061-ca257284-c64d-4a90-80b5-c3b5adaaa389.png)

第一张图的内容是一个Polars库的代码示例，该代码通过两种方法来相乘两列`i`和`j`，并生成一个新的DataFrame：

```python
(
    pl.DataFrame({
        "i": [6, 0, 2, 2.5],
        "j": [7, 1, 2, 3]
    })
    .with_columns(
        (pl.col("i") * pl.col("j")).alias("*"),
        pl.col("i").mul(pl.col("j")).alias("Expr.mul()")
    )
)
```

```plain
shape: (4, 4)
┌─────┬─────┬──────┬────────────┐
│ i   │ j   │ *    │ Expr.mul() │
│ --- │ --- │ ---  │ ---        │
│ f64 │ i64 │ f64  │ f64        │
╞═════╪═════╪══════╪════════════╡
│ 6.0 │ 7   │ 42.0 │ 42.0       │
│ 0.0 │ 1   │ 0.0  │ 0.0        │
│ 2.0 │ 2   │ 4.0  │ 4.0        │
│ 2.5 │ 3   │ 7.5  │ 7.5        │
└─────┴─────┴──────┴────────────┘
```



正如预期的那样，这两种方法产生了相同的结果。我们认为，内联操作符周围的空格可以清楚地表明你正在将两个表达式组合成一个新的表达式。出于这个原因，我们通常建议使用相应的内联操作符（如果存在）。然而，请注意，在使用内联操作符的方法中，你需要将新的表达式用括号括起来，以便继续使用附加方法。

这就是关于乘法表达式的全部内容。现在让我们来看一些其他的算术操作。

<h2 id="rqKpa">算术操作</h2>
算术是任何与数据相关的任务的基石。你可以对表达式和Python值进行算术运算。

这里有一个有趣的例子，将 `weight` 列（一个表达式）除以1000（一个Python整数）：

```python
fruit.select(
    pl.col("name"),
    (pl.col("weight") / 1000)
)
```

```plain
shape: (10, 2)
┌────────────┬────────┐
│ name       │ weight │
│ ---        │ ---    │
│ str        │ f64    │
╞════════════╪════════╡
│ Avocado    │ 0.2    │
│ Banana     │ 0.12   │
│ Blueberry  │ 0.001  │
│ Cantaloupe │ 2.5    │
│ Cranberry  │ 0.002  │
│ Elderberry │ 0.001  │
│ Orange     │ 0.13   │
│ Papaya     │ 1.0    │
│ Peach      │ 0.15   │
│ Watermelon │ 5.0    │
└────────────┴────────┘
```



_表8-1 列出了所有用于在Polars中进行算术运算的内联操作符和方法。_

| 内联操作符 | 方法 | 描述 |
| --- | --- | --- |
| **<font style="color:#1DC0C9;">+</font>** | `**<font style="color:#1DC0C9;">Expr.add(…)</font>**` | 加法 |
| **<font style="color:#1DC0C9;">-</font>** | `**<font style="color:#1DC0C9;">Expr.sub(…)</font>**` | 减法 |
| **<font style="color:#1DC0C9;">*</font>** | `**<font style="color:#1DC0C9;">Expr.mul(…)</font>**` | 乘法 |
| **<font style="color:#1DC0C9;">/</font>** | `**<font style="color:#1DC0C9;">Expr.truediv(…)</font>**` | 除法 |
| **<font style="color:#1DC0C9;">//</font>** | `**<font style="color:#1DC0C9;">Expr.floordiv(…)</font>**` | 整数除 |
| **<font style="color:#1DC0C9;">**</font>** | `**<font style="color:#1DC0C9;">Expr.pow(…)</font>**` | 乘方 |
| **<font style="color:#1DC0C9;">%</font>** | `**<font style="color:#1DC0C9;">Expr.mod(…)</font>**` | 取模 |
| **<font style="color:#1DC0C9;">N/A</font>** | `**<font style="color:#1DC0C9;">Expr.dot(…)</font>**` | 点积 |


以下代码片段演示了如何在两个整数列 `i` 和 `j` 上使用这些内联操作符。当需要时，Polars会自动创建一个浮点列。由于方法 `Expr.dot()` 没有相应的内联操作符，我们改为使用该方法。

```python
pl.Config(float_precision=2, tbl_cell_numeric_alignment="RIGHT") ①

(
    pl.DataFrame({
        "i": [0, 2, 2, -2, -2],
        "j": [1, 2, 3, 4, -5]
    })
    .with_columns(
        (pl.col("i") + pl.col("j")).alias("i + j"),
        (pl.col("i") - pl.col("j")).alias("i - j"),
        (pl.col("i") * pl.col("j")).alias("i * j"),
        (pl.col("i") / pl.col("j")).alias("i / j"),
        (pl.col("i") // pl.col("j")).alias("i // j"),
        (pl.col("i") ** pl.col("j")).alias("i ** j"),
        (pl.col("j") % 2).alias("j % 2"), ②
        pl.col("i").dot(pl.col("j")).alias("i ⋅ j"), ③
    )
)
```

```plain
shape: (5, 10)
┌─────┬─────┬───────┬───────┬───────┬───────┬────────┬────────┬───────┬───────┐
│   i │   j │ i + j │ i - j │ i * j │ i / j │ i // j │ i ** j │ j % 2 │ i ⋅ j │
│ --- │ --- │   --- │   --- │   --- │   --- │    --- │    --- │   --- │   --- │
│ i64 │ i64 │   i64 │   i64 │   i64 │   f64 │    i64 │    f64 │   i64 │   i64 │
╞═════╪═════╪═══════╪═══════╪═══════╪═══════╪════════╪════════╪═══════╪═══════╡
│   0 │   1 │     1 │    -1 │     0 │  0.00 │      0 │   0.00 │     1 │    12 │
│   2 │   2 │     4 │     0 │     4 │  1.00 │      1 │   4.00 │     0 │    12 │
│   2 │   3 │     5 │    -1 │     6 │  0.67 │      0 │   8.00 │     1 │    12 │
│  -2 │   4 │     2 │    -6 │    -8 │ -0.50 │     -1 │  16.00 │     0 │    12 │
│  -2 │  -5 │    -7 │     3 │    10 │  0.40 │      0 │  -0.03 │     1 │    12 │
└─────┴─────┴───────┴───────┴───────┴───────┴────────┴────────┴───────┴───────┘
```

① 我们临时更改了这两个显示设置，以便在页面上适应这个宽DataFrame。

② 模运算符（%）接受第二个表达式，和其他算术运算一样。

③ 因为点积操作没有对应的内联操作符，所以我们在列名中使用了一个点字符（.）。点积也是这里唯一使用整个列而不是元素的操作；因此，最后一列包含相同的值（即12），共出现五次。

<h2 id="KkBoP">比较操作</h2>
这些实验中的哪个产生了显著的结果？90年代上映的电影中哪些IMDB评分为8.7或更高？这些电压是否在允许范围内？这些都是与比较值相关的所有数据问题。

在Polars中比较值的工作方式与Python几乎相同，唯一的不同是它们不能被链接（将在下文解释）。

```python
pl.select(pl.lit("a") > pl.lit("b"))
```

```plain
shape: (1, 1)
┌─────────┐
│ literal │
│ ---     │
│ bool    │
╞═════════╡
│ false   │
└─────────┘
```



你很可能会比较两列数值型数据（例如 `pl.Int8` 和 `pl.Float64`）。你还可以比较字符串和时间数据类型（包括 `pl.Date`、`pl.DateTime`、`pl.Duration` 和 `pl.Time`）。

比较操作总是构造一个布尔序列。这种序列可以作为列添加到DataFrame中，但更常用于过滤行，使用 `df.filter()`，或在条件表达式中使用 `pl.when()`。

以下是一个示例，使用DataFrame `fruit` 将列 `weight` 与值1000进行比较，使用大于或等于操作符（`>=`）。构造的布尔序列用于过滤行。

```python
(
    fruit.select(
        pl.col("name"),
        pl.col("weight"),
    )
    .filter(pl.col("weight") >= 1000)
)
```

```plain
shape: (3, 2)
┌────────────┬────────┐
│ name       │ weight │
│ ---        │ ---    │
│ str        │ i64    │
╞════════════╪════════╡
│ Cantaloupe │ 2500   │
│ Papaya     │ 1000   │
│ Watermelon │ 5000   │
└────────────┴────────┘
```



_表 8-2 列出了在Polars中执行比较操作的所有内联操作符和方法。_

| 内联操作符 | 方法 | 描述 |
| --- | --- | --- |
| < | `Expr.lt(…)` | 小于 |
| <= | `Expr.le(…)` | 小于或等于 |
| == | `Expr.eq(…)` | 等于 |
| >= | `Expr.ge(…)` | 大于或等于 |
| > | `Expr.gt(…)` | 大于 |
| != | `Expr.ne(…)` | 不等于 |


---

**<font style="color:#1DC0C9;">链式比较</font>**

在Python中，你可以链式使用上表列出的内联操作符。考虑以下例子，它使用小于操作符（`<`）两次来测试 `x` 的值是否在3和5之间：

```python
x = 4
3 < x < 5
```

```plain
True
```

然而，在Polars中，如果你这么做，你会得到一个错误：

```python
pl.select(pl.lit(3) < pl.lit(x) < pl.lit(5))
```

```plain
TypeError: the truth value of an Expr is ambiguous

You probably got here by using a Python standard library function instea
d of the native expressions API.
Here are some things you might want to try:
- instead of `pl.col('a') and pl.col('b')`, use `pl.col('a') & pl.col('b
')`
- instead of `pl.col('a') in [y, z]`, use `pl.col('a').is_in([y, z])`
- instead of `max(pl.col('a'), pl.col('b'))`, use `pl.max_horizontal(pl.
col('a'), pl.col('b'))`
```



解决方案是执行两个独立的比较，并使用AND（`&`）操作符将它们组合：

```python
pl.select((pl.lit(3) < pl.lit(x)) & (pl.lit(x) < pl.lit(5))).item()
```

```plain
True
```

在下一节中，你将学习更多关于AND（`&`）操作符的内容，我们将在那里讨论如何使用布尔代数组合表达式。在这个特定的情况下，另一个解决方案是使用方法 `Expr.is_between()`：

```python
pl.select(pl.lit(x).is_between(3, 5)).item()
```

```plain
True
```

---



接下来，我们将对两个数值列 `a` 和 `b` 应用一些比较操作符。

```python
(
    pl.DataFrame({
        "a": [-273.15, 0, 42, 100],
        "b": [1.4142, 2.7183, 42, 3.1415]
    })
    .with_columns(
        (pl.col("a") == pl.col("b")).alias("a == b"),
        (pl.col("a") <= pl.col("b")).alias("a <= b"),
        (pl.all() > 0).name.suffix(" > 0"),
        ((pl.col("b") - pl.lit(2).sqrt()).abs() < 1e-3).alias("b ≈ √2"), ①
        ((1 < pl.col("b")) & (pl.col("b") < 3)).alias("1 < b < 3")
    )
)
```

```plain
shape: (4, 8)
┌─────────┬────────┬────────┬────────┬───────┬───────┬────────┬───────────┐
│ a       │ b      │ a == b │ a <= b │ a > 0 │ b > 0 │ b ≈ √2 │ 1 < b < 3 │
│ ---     │ ---    │ ---    │ ---    │ ---   │ ---   │ ---    │ ---       │
│ f64     │ f64    │ bool   │ bool   │ bool  │ bool  │ bool   │ bool      │
╞═════════╪════════╪════════╪════════╪═══════╪═══════╪════════╪═══════════╡
│ -273.15 │ 1.4142 │ false  │ true   │ false │ true  │ true   │ true      │
│ 0.0     │ 2.7183 │ false  │ true   │ false │ true  │ false  │ true      │
│ 42.0    │ 42.0   │ true   │ true   │ true  │ true  │ false  │ false     │
│ 100.0   │ 3.1415 │ false  │ false  │ true  │ true  │ false  │ false     │
└─────────┴────────┴────────┴────────┴───────┴───────┴────────┴───────────┘
```

① 在这里，我们同时使用算术和比较运算来组合表达式。



以下代码片段展示了几种不同数据类型之间的比较。两种比较是不可行的：字符串与数字比较、DateTime与Time比较。

```python
pl.select(
    bool_num=pl.lit(True) > 0,
    time_time=pl.time(23, 58) > pl.time(0, 0),
    datetime_date=pl.datetime(1969, 7, 21, 2, 56) < pl.date(1976, 7, 20),
    str_num=pl.lit("5") < pl.lit(3).cast(pl.String), ①
    datetime_time=pl.datetime(1999, 1, 1).dt.time() != pl.time(0, 0), ②
).transpose(include_header=True,
            header_name="comparison",
            column_names=["allowed"])
```

```plain
shape: (5, 2)
┌───────────────┬─────────┐
│ comparison    │ allowed │
│ ---           │ ---     │
│ str           │ bool    │
╞═══════════════╪═════════╡
│ bool_num      │ true    │
│ time_time     │ true    │
│ datetime_date │ true    │
│ str_num       │ false   │
│ datetime_time │ false   │
└───────────────┴─────────┘
```

① 你不能将字符串和数字进行比较。解决方案是首先将数字转换为字符串，使用 `Expr.cast(pl.String)`。

② 你也不能将DateTime与Time进行比较。解决方案是首先从DateTime中提取时间部分，使用 `Expr.dt.time()` 方法。

<h2 id="wuvrB">布尔代数运算</h2>
在上一节中，我们结合了两个比较表达式，以检查 `x` 的值是否在两个值之间。让我们再次使用这个示例，但将 `x` 的值设置为7。我们还会将这两个比较表达式赋值给两个变量 `p` 和 `q`。

```python
x = 7
p = pl.lit(3) < pl.lit(x)  # True
q = pl.lit(x) < pl.lit(5)  # False
pl.select(p & q).item()
```

```plain
False
```

我们使用布尔操作符 AND（`&`）结合表达式 `p` 和 `q`，它的结果为 `True`，当且仅当 `p` 和 `q` 都为 `True`。由于 `q` 为 `False`，所以结果是 `False`。这个布尔操作被称为合取（conjunction）。

合取是布尔代数的三种基本操作之一：合取（`&`）、析取（`|`）、和否定（`~`）。使用这三种基本操作，你可以创建任何二级布尔操作。

Polars 提供了一种二级操作：异或（`^`）。**表 8-3** 列出了四个布尔操作及其内联操作符和方法。



_表 8-3：内联操作符及其对应的用于执行布尔运算的方法_

| 操作 | 内联操作符 | 方法 | 描述 |
| --- | --- | --- | --- |
| 合取（Conjunction） | `**<font style="color:#1DC0C9;">&</font>**` | `**<font style="color:#1DC0C9;">Expr.and_(…)</font>**` | 逻辑与（Logical AND） |
| 析取（Disjunction） | **<font style="color:#1DC0C9;">`</font>** | **<font style="color:#1DC0C9;">`</font>** | `Expr.or_(…)` |
| 否定（Negation） | `**<font style="color:#1DC0C9;">~</font>**` | `**<font style="color:#1DC0C9;">Expr.not_(…)</font>**` | 逻辑非（Logical NOT） |
| 异或（Exclusive OR） | `**<font style="color:#1DC0C9;">^</font>**` | `**<font style="color:#1DC0C9;">Expr.xor(…)</font>**` | 逻辑异或（Logical XOR） |




---

**<font style="color:#1DC0C9;">丑陋的下划线</font>**

在Python中，`and`、`or` 和 `not` 是保留关键字。这就是为什么表 8-3 中列出的前三个方法在结尾带有下划线（`_`）。这些下划线看起来不太美观，但你很可能会使用对应的内联操作符（`&`、`|` 和 `~`）来代替。

---



合取操作只有在两个表达式都为 `True` 时才会返回 `True`。以下代码片段对所有可能的 `p` 和 `q` 组合应用六个布尔运算（表 8-3 中列出的四个操作符加上两个额外操作符 NAND 和 NOR）。输出结果被称为**真值表**。

```python
(
    pl.DataFrame({
        "p": [True, True, False, False],
        "q": [True, False, True, False]
    })
    .with_columns(
        (pl.col("p") & pl.col("q")).alias("p & q"),
        (pl.col("p") | pl.col("q")).alias("p | q"),
        (~pl.col("p")).alias("~p"),
        (pl.col("p") ^ pl.col("q")).alias("p ^ q"),
        (~(pl.col("p") & pl.col("q"))).alias("p ↑ q"),  ①
        ((pl.col("p").or_(pl.col("q"))).not_()).alias("p ↓ q")  ②
    )
)
```

```plain
shape: (4, 8)
┌───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┐
│ p     │ q     │ p & q │ p | q │ ~p    │ p ^ q │ p ↑ q │ p ↓ q │
│ ---   │ ---   │ ---   │ ---   │ ---   │ ---   │ ---   │ ---   │
│ bool  │ bool  │ bool  │ bool  │ bool  │ bool  │ bool  │ bool  │
╞═══════╪═══════╪═══════╪═══════╪═══════╪═══════╪═══════╪═══════╡
│ true  │ true  │ true  │ true  │ false │ false │ false │ false │
│ true  │ false │ false │ true  │ false │ true  │ true  │ false │
│ false │ true  │ false │ true  │ true  │ true  │ true  │ false │
│ false │ false │ false │ false │ true  │ false │ true  │ true  │
└───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘
```

①NAND（非与）操作符不属于Polars的一部分，但可以通过结合NOT（~）和AND（&）操作符来模拟。

②NOR（非或）操作符同样不属于Polars。这里我们使用一种替代语法，使用方法而不是内联操作符。



能够通过这些布尔操作结合布尔表达式，让你能够表达复杂的表达式关系。在下一节中，我们将应用相同的方法和内联操作符，但用于整数而不是布尔值。

<h2 id="QRA09">位运算</h2>
你还可以对整数应用**AND（**`**&**`**）**、**OR（**`**|**`**）**、**XOR（**`**^**`**）**和**NOT（**`**~**`**）**操作符。在这种情况下，这些操作符执行的是位运算。

下面是一个示例，应用位或操作符（`|`）于数值10和34，逻辑上得出42：

```python
pl.select(pl.lit(10) | pl.lit(34)).item()
```

```plain
42
```

在底层，Polars对10和34中的每一对位应用或操作符。当至少有一个输入位为1时，输出位为1：

```plain
00001010 (十进制 10)
OR 00100010 (十进制 34)
=  00101010 (十进制 42)
```

因此，10 | 34 得到 42，因为在10或34中，从右往左的第二位、第四位和第六位都是1。你可以将这些位看作一系列布尔值——逻辑是一样的。

**表 8-4** 列出了四种位运算及其内联操作符和方法。



_表 8-4：用于执行位运算的内联操作符及其对应的方法_

| 内联操作符 | 方法 | 描述 |
| --- | --- | --- |
| `**<font style="color:#1DC0C9;">&</font>**` | `**<font style="color:#1DC0C9;">Expr.and_(…)</font>**` | 位与（Bitwise AND） |
| `**<font style="color:#1DC0C9;">|</font>**` | `**<font style="color:#1DC0C9;">Expr.or_(…)</font>**` | 位或（Bitwise OR） |
| `**<font style="color:#1DC0C9;">~</font>**` | `**<font style="color:#1DC0C9;">Expr.not_(…)</font>**` | 位非（Bitwise NOT） |
| `**<font style="color:#1DC0C9;">^</font>**` | `**<font style="color:#1DC0C9;">Expr.xor(…)</font>**` | 位异或（Bitwise XOR） |


以下代码片段将**表 8-4** 中列出的位运算应用于一对整数：

```python
bits = (
    pl.DataFrame({
        "x": [1, 1, 0, 0, 7, 10],
        "y": [1, 0, 1, 0, 2, 34]
    }, schema={"x": pl.UInt8, "y": pl.UInt8}) ①
    .with_columns(
        (pl.col("x") & pl.col("y")).alias("x & y"),
        (pl.col("x") | pl.col("y")).alias("x | y"),
        (~pl.col("x")).alias("~x"),
        (pl.col("x") ^ pl.col("y")).alias("x ^ y"),
    )
)
bits
```

```plain
shape: (6, 6)
┌─────┬─────┬───────┬───────┬─────┬───────┐
│ x   │ y   │ x & y │ x | y │ ~x  │ x ^ y │
│ --- │ --- │ ---   │ ---   │ --- │ ---   │
│ u8  │ u8  │ u8    │ u8    │ u8  │ u8    │
╞═════╪═════╪═══════╪═══════╪═════╪═══════╡
│ 1   │ 1   │ 1     │ 1     │ 254 │ 0     │
│ 1   │ 0   │ 0     │ 1     │ 254 │ 1     │
│ 0   │ 1   │ 0     │ 1     │ 255 │ 1     │
│ 0   │ 0   │ 0     │ 0     │ 255 │ 0     │
│ 7   │ 2   │ 2     │ 7     │ 248 │ 5     │
│ 10  │ 34  │ 2     │ 42    │ 245 │ 40    │
└─────┴─────┴───────┴───────┴─────┴───────┘
```

① 我们使用 8 位无符号整数（`pl.UInt8`），这样就可以在位级别上更容易理解这些操作。你可以将位运算符应用于任何整数类型。



让我们来看一下这些整数的二进制字符串表示，来理解每个操作符是如何工作的：

```python
bits.select(pl.all().map_elements("{0:08b}".format))
```

```plain
MapWithoutReturnDtypeWarning: Calling `map_elements` without specifying `return_
dtype` can lead to unpredictable results. Specify `return_dtype` to silence this
 warning.
shape: (6, 6)
┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ x        │ y        │ x & y    │ x | y    │ ~x       │ x ^ y    │
│ ---      │ ---      │ ---      │ ---      │ ---      │ ---      │
│ str      │ str      │ str      │ str      │ str      │ str      │
╞══════════╪══════════╪══════════╪══════════╪══════════╪══════════╡
│ 00000001 │ 00000001 │ 00000001 │ 00000001 │ 11111110 │ 00000000 │
│ 00000001 │ 00000000 │ 00000000 │ 00000001 │ 11111110 │ 00000001 │
│ 00000000 │ 00000001 │ 00000000 │ 00000001 │ 11111111 │ 00000001 │
│ 00000000 │ 00000000 │ 00000000 │ 00000000 │ 11111111 │ 00000000 │
│ 00000111 │ 00000010 │ 00000010 │ 00000111 │ 11111000 │ 00000101 │
│ 00001010 │ 00100010 │ 00000010 │ 00101010 │ 11110101 │ 00101000 │
└──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
```



---

**<font style="color:#1DC0C9;">一和零</font>**

当你使用 1 和 0 来表示布尔值时，这些操作符的结果与它们是布尔值时相同，除了 NOT 操作符。`True` 的取反是 `False`，而 `1` 的取反是 `254`（而不是 `0`），因为前7位加起来等于254（128 + 64 + 32 + 16 + 8 + 4 + 2 = 254）。我们建议在表达式或列只能取两个值的情况下，使用布尔值。

---

<h2 id="aEaEB">使用函数</h2>
**表 8-5** 列出了所有用于将现有表达式组合成单一表达式的模块级函数。



_表 8-5：用于组合表达式的模块级函数_

| 函数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">pl.all_horizontal(…)</font>**` | 水平地跨列计算按位与 |
| `**<font style="color:#1DC0C9;">pl.any_horizontal(…)</font>**` | 水平地跨列计算按位或 |
| `**<font style="color:#1DC0C9;">pl.arctan2(…)</font>**` | 计算两个参数的反正切（弧度） |
| `**<font style="color:#1DC0C9;">pl.arctan2d(…)</font>**` | 计算两个参数的反正切（角度） |
| `**<font style="color:#1DC0C9;">pl.arg_sort_by(…)</font>**` | 返回将列排序的行索引 |
| `**<font style="color:#1DC0C9;">pl.arg_where(…)</font>**` | 返回条件为真的索引 |
| `**<font style="color:#1DC0C9;">pl.coalesce(…)</font>**` | 从左到右合并列，保留第一个非空值 |
| `**<font style="color:#1DC0C9;">pl.concat_list(…)</font>**` | 水平地跨列将列连接成单个列表列 |
| `**<font style="color:#1DC0C9;">pl.concat_str(…)</font>**` | 水平地跨列将列连接成单个字符串列 |
| `**<font style="color:#1DC0C9;">pl.corr(…)</font>**` | 计算两列/表达式之间的Pearson或Spearman秩相关系数 |
| `**<font style="color:#1DC0C9;">pl.cov(…)</font>**` | 计算两列/表达式之间的协方差 |
| `**<font style="color:#1DC0C9;">pl.cum_fold(…)</font>**` | 在列间水平地累加折叠，使用左折叠 |
| `**<font style="color:#1DC0C9;">pl.cum_reduce(…)</font>**` | 在列间水平地累加减少，使用左折叠 |
| `**<font style="color:#1DC0C9;">pl.cum_sum_horizontal(…)</font>**` | 水平地跨列累加所有值 |
| `**<font style="color:#1DC0C9;">pl.fold(…)</font>**` | 水平地跨多列累加/按行累加，使用左折叠 |
| `**<font style="color:#1DC0C9;">pl.format(…)</font>**` | 将表达式格式化为字符串 |
| `**<font style="color:#1DC0C9;">pl.map_batches(…)</font>**` | 映射自定义函数到多个列/表达式上 |
| `**<font style="color:#1DC0C9;">pl.max_horizontal(…)</font>**` | 水平地跨列获取最大值 |
| `**<font style="color:#1DC0C9;">pl.min_horizontal(…)</font>**` | 水平地跨列获取最小值 |
| `**<font style="color:#1DC0C9;">pl.reduce(…)</font>**` | 水平地跨多列累加/按行累加，使用左折叠 |
| `**<font style="color:#1DC0C9;">pl.rolling_corr(…)</font>**` | 计算两列/表达式之间的滚动相关系数 |
| `**<font style="color:#1DC0C9;">pl.rolling_cov(…)</font>**` | 计算两列/表达式之间的滚动协方差 |
| `**<font style="color:#1DC0C9;">pl.struct(…)</font>**` | 将列收集到结构列中 |
| `**<font style="color:#1DC0C9;">pl.sum_horizontal(…)</font>**` | 水平地跨列求和 |
| `**<font style="color:#1DC0C9;">pl.when(…)</font>**` | 启动一个条件表达式（when-then-otherwise） |




我们不能详细讨论所有的函数，但这里有几个值得注意的例子。

首先是两个函数，它们将多个表达式的值组合成一个结构。函数 `pl.concat_list()` 和 `pl.struct()` 分别创建一个列表和一个结构体。

```python
scientists = pl.DataFrame({
    'first_name': ['George', 'Grace', 'John', 'Kurt', 'Ada'],
    'last_name': ['Boole', 'Hopper', 'Tukey', 'Gödel', 'Lovelace'],
    'country': ['England', 'United States', 'United States', 'Austria-Hungary', 'England']
})
scientists
```

```plain
shape: (5, 3)
┌────────────┬───────────┬─────────────────┐
│ first_name │ last_name │ country         │
│ ---        │ ---       │ ---             │
│ str        │ str       │ str             │
╞════════════╪═══════════╪═════════════════╡
│ George     │ Boole     │ England         │
│ Grace      │ Hopper    │ United States   │
│ John       │ Tukey     │ United States   │
│ Kurt       │ Gödel     │ Austria-Hungary │
│ Ada        │ Lovelace  │ England         │
└────────────┴───────────┴─────────────────┘
```

```python
scientists.select(
    pl.concat_list(pl.col("^*_name$")).alias("concat_list"),
    pl.struct(pl.all()).alias("struct")
)
```

```plain
shape: (5, 2)
┌─────────────────────┬────────────────────────────────────┐
│ concat_list         │ struct                             │
│ ---                 │ ---                                │
│ list[str]           │ struct[3]                          │
╞═════════════════════╪════════════════════════════════════╡
│ ["George", "Boole"] │ {"George","Boole","England"}       │
│ ["Grace", "Hopper"] │ {"Grace","Hopper","United States"} │
│ ["John", "Tukey"]   │ {"John","Tukey","United States"}   │
│ ["Kurt", "Gödel"]   │ {"Kurt","Gödel","Austria-Hungary"} │
│ ["Ada", "Lovelace"] │ {"Ada","Lovelace","England"}       │
└─────────────────────┴────────────────────────────────────┘
```



其次，函数 `pl.concat_str()` 和 `pl.format()` 基于多个表达式创建一个字符串。后者为你提供了在如何组合字符串上更大的灵活性。以下是一个示例：

```python
scientists.select(
    pl.concat_str(pl.all(), separator=" ").alias("concat_str"),
    pl.format("{}, {} from {}",
              "last_name", "first_name", "country").alias("format")
)
```

```plain
shape: (5, 2)
┌────────────────────────────┬──────────────────────────────────┐
│ concat_str                 │ format                           │
│ ---                        │ ---                              │
│ str                        │ str                              │
╞════════════════════════════╪══════════════════════════════════╡
│ George Boole England       │ Boole, George from England       │
│ Grace Hopper United States │ Hopper, Grace from United States │
│ John Tukey United States   │ Tukey, John from United States   │
│ Kurt Gödel Austria-Hungary │ Gödel, Kurt from Austria-Hungary │
│ Ada Lovelace England       │ Lovelace, Ada from England       │
└────────────────────────────┴──────────────────────────────────┘
```



最后，函数 `pl.all_horizontal()` 和 `pl.any_horizontal()` 类似于在多列上使用 **AND（**`&`**）** 和 **OR（**`|`**）** 操作符。这对于你有许多列需要组合时特别有用，你不需要全部手动写出。例如：

```python
prefs = pl.DataFrame({
    "id": [1, 7, 42, 101, 999],
    "has_pet": [True, False, True, False, True],
    "likes_travel": [False, False, False, False, True],
    "likes_movies": [True, False, True, False, True],
    "likes_books": [False, False, True, True, True]
}).with_columns(
    pl.all_horizontal(pl.exclude("id")).alias("all"),
    pl.any_horizontal(pl.exclude("id")).alias("any"),
)
prefs
```

```plain
shape: (5, 7)
┌─────┬─────────┬──────────────┬──────────────┬─────────────┬───────┬───────┐
│ id  │ has_pet │ likes_travel │ likes_movies │ likes_books │ all   │ any   │
│ --- │ ---     │ ---          │ ---          │ ---         │ ---   │ ---   │
│ i64 │ bool    │ bool         │ bool         │ bool        │ bool  │ bool  │
╞═════╪═════════╪══════════════╪══════════════╪═════════════╪═══════╪═══════╡
│ 1   │ true    │ false        │ true         │ false       │ false │ true  │
│ 7   │ false   │ false        │ false        │ false       │ false │ false │
│ 42  │ true    │ false        │ true         │ true        │ false │ true  │
│ 101 │ false   │ false        │ false        │ true        │ false │ true  │
│ 999 │ true    │ true         │ true         │ true        │ true  │ true  │
└─────┴─────────┴──────────────┴──────────────┴─────────────┴───────┴───────┘
```



相关的函数有 `pl.sum_horizontal()`、`pl.max_horizontal()` 和 `pl.min_horizontal()`，它们分别计算跨列的和、最大值和最小值。它们可以同时作用于布尔和数值列：

```python
prefs.select(
    pl.sum_horizontal(pl.all()).alias("sum"),
    pl.max_horizontal(pl.all()).alias("max"),
    pl.min_horizontal(pl.all()).alias("min")
)
```

```plain
shape: (5, 3)
┌──────┬─────┬─────┐
│ sum  │ max │ min │
│ ---  │ --- │ --- │
│ i64  │ i64 │ i64 │
╞══════╪═════╪═════╡
│ 4    │ 1   │ 0   │
│ 7    │ 7   │ 0   │
│ 46   │ 42  │ 0   │
│ 103  │ 101 │ 0   │
│ 1005 │ 999 │ 1   │
└──────┴─────┴─────┘
```



函数 `pl.when()` 创建一个条件表达式。你可以将其视为向量化的 `if` 语句。以下是一个示例：

```python
prefs.select(
    pl.col("id"),
    pl.when(pl.all_horizontal(pl.col("^likes_.*$")))
        .then(pl.lit("Likes everything"))
        .when(pl.any_horizontal(pl.col("^likes_.*$")))
        .then(pl.lit("Likes something"))
        .otherwise(pl.lit("Likes nothing"))
        .alias("likes_what")
)
```

```plain
shape: (5, 2)
┌─────┬──────────────────┐
│ id  │ likes_what       │
│ --- │ ---              │
│ i64 │ str              │
╞═════╪══════════════════╡
│ 1   │ Likes something  │
│ 7   │ Likes nothing    │
│ 42  │ Likes something  │
│ 101 │ Likes something  │
│ 999 │ Likes everything │
└─────┴──────────────────┘
```

<h2 id="cE4nV">结论</h2>
这总结了第三部分，**表达式**（Express）的最后一章。你现在可以开始、继续并组合表达式了。



---

<h2 id="Ogy5a">本章注释</h2>
1. 因为否定操作符（`~`）作用于单个表达式，它不是组合表达式，但我们仍然在这里讨论它。这只是逻辑上的问题。
2. **NAND** 代表非与。**NOR** 代表非或。
3. 位运算可能有点小众，但毕竟这是**终极指南**。
4. 参见道格拉斯·亚当斯的《银河系漫游指南》以获得更详细的解释。

