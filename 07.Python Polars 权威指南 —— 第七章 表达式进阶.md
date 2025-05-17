

---

在上一章中，你学习了如何从零开始创建表达式。一个简单的表达式只能达到一定的效果。在本章中，你将学习如何通过添加额外的操作（或方法）继续构建表达式。

更具体地说，你将学习如何：

+ 执行数学转换
+ 处理缺失值
+ 对值应用平滑处理
+ 选择特定的值
+ 使用统计汇总值

---

**<font style="color:#1DC0C9;">大量的方法</font>**

本章讨论了超过 138 个方法。不可能在这里详细解释和演示每一个方法。请参考 [Polars API 参考](https://pola-rs.github.io/polars/) 以获取更多细节和示例。

---

在本章的一些代码片段中，我们使用了 `math` 和 `numpy` 模块来访问某些常量（例如 `math.pi`），以及生成随机值：

```python
import math
import numpy as np

print(f"{math.pi=}")
rng = np.random.default_rng(1729)
print(f"rng.random()={rng.random()}")
```

```plain
math.pi=3.141592653589793
rng.random()=0.03074202960516803
```



<h2 id="nVzgV">**操作类型**</h2>
与其将138个方法呈现为一长串列表，我们根据它们使用的输入和输出的形状将它们组织成五个部分。在这五个部分中，我们根据需要将方法分组。未归入任何类别的方法放在“其他”类别中。图7-1显示了用于继续表达式的操作类型。

![](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728812843890-dfdf42d4-3783-4282-85ea-200abf580462.png)

---

**<font style="color:#ECAA04;">相关方法，不同部分</font>**

尽管我们相信这种组织方式是有用的，但它确实会导致某些相关方法出现在不同的部分。例如，`Expr.unique()` 和 `Expr.is_unique()` 都与唯一值有关，但由于前者可能会减少 Series 的长度，而后者不会，它们被放在了不同的部分。

---

这里有四个示例来演示我们对各种操作类型的理解。



<h3 id="rEjfY">**示例 A：逐元素操作**</h3>
在第一个示例中，我们将使用两种方法创建两个附加列：`Expr.sqrt()` 和 `Expr.interpolate()`。这两种方法都是逐元素操作的（即，它们一次处理一个元素）并保持 Series 的长度。

```python
penguins = (
    pl.read_csv("data/penguins.csv", null_values="NA")
    .select(
        "species",
        "island",
        "sex",
        "year",
        pl.col("body_mass_g").alias("mass") / 1000
    )
)
penguins.with_columns(
    pl.col("mass").sqrt().alias("mass_sqrt"), ①
    pl.col("mass").interpolate().alias("mass_filled") ②
)
```

```plain
shape: (344, 7)
┌───────────┬───────────┬────────┬──────┬───────┬───────────┬─────────────┐
│ species   │ island    │ sex    │ year │ mass  │ mass_sqrt │ mass_filled │
│ ---       │ ---       │ ---    │ ---  │ ---   │ ---       │ ---         │
│ str       │ str       │ str    │ i64  │ f64   │ f64       │ f64         │
╞═══════════╪═══════════╪════════╪══════╪═══════╪═══════════╪═════════════╡
│ Adelie    │ Torgersen │ male   │ 2007 │ 3.75  │ 1.936492  │ 3.75        │
│ Adelie    │ Torgersen │ female │ 2007 │ 3.8   │ 1.949359  │ 3.8         │
│ Adelie    │ Torgersen │ female │ 2007 │ 3.25  │ 1.802776  │ 3.25        │
│ Adelie    │ Torgersen │ null   │ 2007 │ null  │ null      │ 3.35        │
│ Adelie    │ Torgersen │ female │ 2007 │ 3.45  │ 1.857418  │ 3.45        │
│ …         │ …         │ …      │ …    │ …     │ …         │ …           │
│ Chinstrap │ Dream     │ male   │ 2009 │ 4.0   │ 2.0       │ 4.0         │
│ Chinstrap │ Dream     │ female │ 2009 │ 3.4   │ 1.843909  │ 3.4         │
│ Chinstrap │ Dream     │ male   │ 2009 │ 3.775 │ 1.942936  │ 3.775       │
│ Chinstrap │ Dream     │ male   │ 2009 │ 4.1   │ 2.024846  │ 4.1         │
│ Chinstrap │ Dream     │ female │ 2009 │ 3.775 │ 1.942936  │ 3.775       │
└───────────┴───────────┴────────┴──────┴───────┴───────────┴─────────────┘
```

① `Expr.sqrt()` 方法计算 `mass` 列的平方根。请注意，空值仍然保持为空值。

② 更好的做法是按物种插值缺失值，以便获得更准确的结果。



<h3 id="Xf7qE">**示例 B：汇总为一个值的操作**</h3>
在第二个示例中，我们应用了两种将 Series 汇总为单一值的方法：

```python
penguins.select(
    pl.col("mass").mean(),
    pl.col("island").mode().first() ①
)
```

```plain
shape: (1, 2)
┌──────────┬────────┐
│ mass     │ island │
│ ---      │ ---    │
│ f64      │ str    │
╞══════════╪════════╡
│ 4.201754 │ Biscoe │
└──────────┴────────┘
```

① 注意——根据输入的不同，`Expr.mode()` 方法可能会生成多个值。这就是为什么我们使用 `Expr.first()` 方法继续表达式，以确保只有一个值。



<h3 id="s9jZh">**示例 C：汇总为一个或多个值的操作**</h3>
在第三个示例中，我们使用 `Expr.unique()` 方法获取 Series 中的唯一值。这是一种汇总为一个或多个值的操作类型。

```python
penguins.select(
    pl.col("island").unique()
)
```

```plain
shape: (3, 1)
┌───────────┐
│ island    │
│ ---       │
│ str       │
╞═══════════╡
│ Dream     │
│ Biscoe    │
│ Torgersen │
└───────────┘
```



**示例 D：扩展的操作**

在第四个示例中，我们使用 `Expr.extend_constant()` 方法将特定值附加到 Series 的末尾。这种操作类型使用得较少。这个例子可能有点人为造作，但它确实说明了如果添加其他方法，表达式可以变得多么强大：

```python
penguins.select(
    pl.col("species")
    .unique() ①
    .repeat_by(3000) ②
    .explode() ③
    .extend_constant("Saiyan", n=1) ④
)
```

```plain
shape: (9_001, 1)
┌───────────┐
│ species   │
│ ---       │
│ str       │
╞═══════════╡
│ Chinstrap │
│ Chinstrap │
│ Chinstrap │
│ Chinstrap │
│ Chinstrap │
│ …         │
│ Adelie    │
│ Adelie    │
│ Adelie    │
│ Adelie    │
│ Saiyan    │
└───────────┘
```

① 获取 Series 的三个唯一值。

② 将每个值重复 3000 次。这将生成一个包含三个长列表的 Series。

③ 使用 explode 方法将三个长列表展开为一个包含 9000 个值的长 Series。

④ 在 Series 末尾添加一个值。结果是长度刚好超过 9000 的 Series。



通过这四个示例，你应该对可以用于继续表达式的操作类型有一定的了解。



<h2 id="SCVCs">逐元素操作</h2>
本节讨论的是一次处理一个元素的操作。每个元素的计算是独立的，并且它们出现的顺序无关紧要。示例包括 `Expr.sqrt()` 方法（用于计算每个值的平方根）和 `Expr.round()` 方法（用于四舍五入值）。

在接下来的五个子节中，我们将探讨与数学变换、三角函数、四舍五入和分组、缺失值或无穷值相关的逐元素操作，以及其他操作。



<h3 id="tEHsZ">执行数学变换的操作</h3>
数学变换（例如计算对数或平方根）是任何与数据相关的任务的基础。本节中列出的方法（见表 7-1）都执行一些数学变换。两个表达式之间的算术运算（如加法和乘法）将在下一章讨论，因为这主要涉及到合并表达式。



表 7-1 基础数学变换的元素级操作

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.abs()</font>**` | 计算绝对值。 |
| `**<font style="color:#1DC0C9;">Expr.cbrt()</font>**` | 计算元素的立方根。 |
| `**<font style="color:#1DC0C9;">Expr.exp()</font>**` | 元素级别地计算指数。 |
| `**<font style="color:#1DC0C9;">Expr.log(…)</font>**` | 计算给定底数的对数。 |
| `**<font style="color:#1DC0C9;">Expr.log10()</font>**` | 计算输入数组的以10为底的对数，逐元素。 |
| `**<font style="color:#1DC0C9;">Expr.log1p()</font>**` | 计算每个元素加一的自然对数。 |
| `**<font style="color:#1DC0C9;">Expr.sign()</font>**` | 元素级别地计算符号指示。 |
| `**<font style="color:#1DC0C9;">Expr.sqrt()</font>**` | 计算元素的平方根。 |




`Expr.abs()`、`Expr.exp()`、`Expr.log()`、`Expr.log10()`、`Expr.log1p()`、`Expr.sign()`、`Expr.sqrt()` 方法在下面的代码片段中得到了演示，适用于各种数值。`Expr.cbrt()` 方法的用法相似。

```python
pl.DataFrame({"x": [-2, 0, 0.5, 1, math.e, 1000]})
.with_columns(
    abs=pl.col("x").abs(),
    exp=pl.col("x").exp(),
    log2=pl.col("x").log(2), ①
    log10=pl.col("x").log10(),
    log1p=pl.col("x").log1p(),
    sign=pl.col("x").sign(),
    sqrt=pl.col("x").sqrt(),
)
```

```plain
shape: (6, 8)
┌──────────┬──────────┬────────┬────────┬────────┬───────┬──────┬────────┐
│ x        │ abs      │ exp    │ log2   │ log10  │ log1p │ sign │ sqrt   │
│ ---      │ ---      │ ---    │ ---    │ ---    │ ---   │ ---  │ ---    │
│ f64      │ f64      │ f64    │ f64    │ f64    │ f64   │ i64  │ f64    │
╞══════════╪══════════╪════════╪════════╪════════╪═══════╪══════╪════════╡
│ -2.000   │ 2.000    │ 0.135  │ NaN    │ NaN    │ NaN   │ -1   │ NaN    │
│ 0.000    │ 0.000    │ 1.000  │ -inf   │ -inf   │ 0.000 │ 0    │ 0.000  │
│ 0.500    │ 0.500    │ 1.649  │ -1.000 │ -0.301 │ 0.405 │ 1    │ 0.707  │
│ 1.000    │ 1.000    │ 2.718  │ 0.000  │ 0.000  │ 0.693 │ 1    │ 1.000  │
│ 2.718    │ 2.718    │ 15.154 │ 1.443  │ 0.434  │ 1.313 │ 1    │ 1.649  │
│ 1000.000 │ 1000.000 │ inf    │ 9.966  │ 3.000  │ 6.909 │ 1    │ 31.623 │
└──────────┴──────────┴────────┴────────┴────────┴───────┴──────┴────────┘
```

①方法 `Expr.log()` 是此处唯一需要参数的方法，即对数的底数。

<h3 id="HFgAu">**与三角学相关的操作**</h3>
三角学是研究角度和三角形边长关系的数学分支。它在数据科学的各个方面（包括信号处理、空间数据、分析和特征工程）中起着至关重要的作用。表 7-2 列出了 Polars 支持的所有与三角学相关的方法。

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.arccos()</font>**` | 计算反余弦的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.arccosh()</font>**` | 计算反双曲余弦的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.arcsin()</font>**` | 计算反正弦的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.arcsinh()</font>**` | 计算反双曲正弦的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.arctan()</font>**` | 计算反正切的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.arctanh()</font>**` | 计算反双曲正切的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.cos()</font>**` | 计算余弦的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.cosh()</font>**` | 计算双曲余弦的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.degrees()</font>**` | 从弧度转换为角度。 |
| `**<font style="color:#1DC0C9;">Expr.radians()</font>**` | 从角度转换为弧度。 |
| `**<font style="color:#1DC0C9;">Expr.sin()</font>**` | 计算正弦的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.sinh()</font>**` | 计算双曲正弦的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.tan()</font>**` | 计算正切的元素级值。 |
| `**<font style="color:#1DC0C9;">Expr.tanh()</font>**` | 计算双曲正切的元素级值。 |




在下面的代码片段中，我们将 `Expr.arccos()`、`Expr.cos()`、`Expr.degrees()`、`Expr.radians()` 和 `Expr.sin()` 方法应用于各种数值。其余的方法，即 `Expr.arccosh()`、`Expr.arcsinh()`、`Expr.arctan()`、`Expr.arctanh()`、`Expr.cosh()`、`Expr.sinh()`、`Expr.tan()` 和 `Expr.tanh()` 可以以类似的方式使用。这些方法都不需要参数。

```python
(
    pl.DataFrame({"x": [-math.pi, 0, 1, math.pi, 2*math.pi, 90, 180, 360]})
    .with_columns(
        arccos=pl.col("x").arccos(),  ①
        cos=pl.col("x").cos(),
        degrees=pl.col("x").degrees(),
        radians=pl.col("x").radians(),
        sin=pl.col("x").sin(),
    )
)
```

```plain
shape: (8, 6)
┌───────────┬──────────┬───────────┬──────────────┬───────────┬─────────────┐
│ x         │ arccos   │ cos       │ degrees      │ radians   │ sin         │
│ ---       │ ---      │ ---       │ ---          │ ---       │ ---         │
│ f64       │ f64      │ f64       │ f64          │ f64       │ f64         │
╞═══════════╪══════════╪═══════════╪══════════════╪═══════════╪═════════════╡
│ -3.141593 │ NaN      │ -1.0      │ -180.0       │ -0.054831 │ -1.2246e-16 │
│ 0.0       │ 1.570796 │ 1.0       │ 0.0          │ 0.0       │ 0.0         │
│ 1.0       │ 0.0      │ 0.540302  │ 57.29578     │ 0.017453  │ 0.841471    │
│ 3.141593  │ NaN      │ -1.0      │ 180.0        │ 0.054831  │ 1.2246e-16  │
│ 6.283185  │ NaN      │ 1.0       │ 360.0        │ 0.109662  │ -2.4493e-16 │
│ 90.0      │ NaN      │ -0.448074 │ 5156.620156  │ 1.570796  │ 0.893997    │
│ 180.0     │ NaN      │ -0.59846  │ 10313.240312 │ 3.141593  │ -0.801153   │
│ 360.0     │ NaN      │ -0.283691 │ 20626.480625 │ 6.283185  │ 0.958916    │
└───────────┴──────────┴───────────┴──────────────┴───────────┴─────────────┘
```

① 对于元素级操作，当某个操作产生了NaN时，其他值不会受到影响。



<h3 id="HlYpM">**执行舍入和分类的操作**</h3>
有时，数据包含了过多的精度或太多不同的值。在这种情况下，舍入或将它们分割为离散类别会很有用。表7-3列出了Polars提供的相关方法。

_表7-3. 用于舍入和分箱的元素级操作_

| 方法 | 描述 |
| --- | --- |
| `Expr.ceil()` | 舍入到最近的整数值。 |
| `Expr.clip(...)` | 将数组中的值限制在最小值和最大值之间。 |
| `Expr.cut(...)` | 将连续值切割为离散的类别。 |
| `Expr.floor()` | 向下舍入到最近的整数值。 |
| `Expr.qcut(...)` | 根据分位数将连续值切割为离散的类别。 |
| `Expr.round(...)` | 按小数位舍入底层的浮点数据。 |


---

下方提到的示例展示了这些方法（并且调用了两次 `Expr.round()`），适用于一系列数字。

```python
(
    pl.DataFrame({"x": [-6, -0.5, 0, 0.5, math.pi, 9.9, 9.99, 9.999]})
    .with_columns(
        ceil=pl.col("x").ceil(),
        clip=pl.col("x").clip(-1, 1),
        cut=pl.col("x").cut([-1, 1], labels=["bad", "neutral", "good"]),①
        floor=pl.col("x").floor(),
        qcut=pl.col("x").qcut([0.5], labels=["below median", "above median"]),
        round2=pl.col("x").round(2),
        round0=pl.col("x").round(0),  ②
    )
)
```

```plain
shape: (8, 8)
┌──────────┬──────┬──────┬─────────┬───────┬──────────────┬────────┬────────┐
│ x        │ ceil │ clip │ cut     │ floor │ qcut         │ round2 │ round0 │
│ ---      │ ---  │ ---  │ ---     │ ---   │ ---          │ ---    │ ---    │
│ f64      │ f64  │ f64  │ cat     │ f64   │ cat          │ f64    │ f64    │
╞══════════╪══════╪══════╪═════════╪═══════╪══════════════╪════════╪════════╡
│ -6.0     │ -6.0 │ -1.0 │ bad     │ -6.0  │ below median │ -6.0   │ -6.0   │
│ -0.5     │ -0.0 │ -0.5 │ neutral │ -1.0  │ below median │ -0.5   │ -1.0   │
│ 0.0      │ 0.0  │ 0.0  │ neutral │ 0.0   │ below median │ 0.0    │ 0.0    │
│ 0.5      │ 1.0  │ 0.5  │ neutral │ 0.0   │ below median │ 0.5    │ 1.0    │
│ 3.141593 │ 4.0  │ 1.0  │ good    │ 3.0   │ above median │ 3.14   │ 3.0    │
│ 9.9      │ 10.0 │ 1.0  │ good    │ 9.0   │ above median │ 9.9    │ 10.0   │
│ 9.99     │ 10.0 │ 1.0  │ good    │ 9.0   │ above median │ 9.99   │ 10.0   │
│ 9.999    │ 10.0 │ 1.0  │ good    │ 9.0   │ above median │ 10.0   │ 10.0   │
└──────────┴──────┴──────┴─────────┴───────┴──────────────┴────────┴────────┘
```

①`Expr.cut()` 和 `Expr.qcut()` 方法构造一个类别型序列。如果你希望将其转换为整数类型，可以在表达式中添加 `Expr.cast(pl.Int64)`。

② 即使使用 `Expr.round(0)`（或者通过 `Expr.ceil()` 或 `Expr.floor()`）将小数舍入为零，数据类型仍然保持为浮点型。



<h3 id="PEvtU">**处理缺失值或无穷值的操作**</h3>
当你的数据基于现实世界时，难免会有一些缺失值。NaN 或无穷值通常是某些无效转换的结果。如果你需要处理这些情况，Polars 提供了一些方便的方法（见表 7-4）。本章后面还有更多方法可以用来处理系列级别的缺失值。

_表 7-4. 涉及缺失值或无穷值的元素级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.fill_nan(...)</font>**` | 用填充值替换浮点类型的 NaN 值。 |
| `**<font style="color:#1DC0C9;">Expr.fill_null(...)</font>**` | 使用指定的值或策略替换空值。 |
| `**<font style="color:#1DC0C9;">Expr.is_finite()</font>**` | 返回一个布尔型序列，指示哪些值是有限的。 |
| `**<font style="color:#1DC0C9;">Expr.is_infinite()</font>**` | 返回一个布尔型序列，指示哪些值是无穷的。 |
| `**<font style="color:#1DC0C9;">Expr.is_nan()</font>**` | 返回一个布尔型序列，指示哪些值是 NaN。 |
| `**<font style="color:#1DC0C9;">Expr.is_not_nan()</font>**` | 返回一个布尔型序列，指示哪些值不是 NaN。 |
| `**<font style="color:#1DC0C9;">Expr.is_not_null()</font>**` | 返回一个布尔型序列，指示哪些值不是空值。 |
| `**<font style="color:#1DC0C9;">Expr.is_null()</font>**` | 返回一个布尔型序列，指示哪些值是空值。 |


下面的代码片段应用了 `Expr.fill_nan()`、`Expr.fill_null()`、`Expr.is_finite()`、`Expr.is_infinite()`、`Expr.is_nan()` 和 `Expr.is_null()` 方法来处理一些数值，这些数值中有些是无穷的或缺失的。`Expr.is_not_nan()` 和 `Expr.is_not_null()` 方法分别与 `Expr.is_nan()` 和 `Expr.is_null()` 方法相反。

```python
x = [42, math.nan, None, math.inf, -math.inf]
(
    pl.DataFrame({"x": x})
    .with_columns(
        fill_nan=pl.col("x").fill_nan(999),
        fill_null=pl.col("x").fill_null(0),
        is_finite=pl.col("x").is_finite(),
        is_infinite=pl.col("x").is_finite(),
        is_nan=pl.col("x").is_nan(),
        is_null=pl.col("x").is_null(),
    )
)
```

```plain
shape: (5, 7)
┌──────┬──────────┬───────────┬───────────┬─────────────┬────────┬─────────┐
│ x    │ fill_nan │ fill_null │ is_finite │ is_infinite │ is_nan │ is_null │
│ ---  │ ---      │ ---       │ ---       │ ---         │ ---    │ ---     │
│ f64  │ f64      │ f64       │ bool      │ bool        │ bool   │ bool    │
╞══════╪══════════╪═══════════╪═══════════╪═════════════╪════════╪═════════╡
│ 42.0 │ 42.0     │ 42.0      │ true      │ true        │ false  │ false   │
│ NaN  │ 999.0    │ NaN       │ false     │ false       │ true   │ false   │
│ null │ null     │ 0.0       │ null      │ null        │ null   │ true    │
│ inf  │ inf      │ inf       │ false     │ false       │ false  │ false   │
│ -inf │ -inf     │ -inf      │ false     │ false       │ false  │ false   │
└──────┴──────────┴───────────┴───────────┴─────────────┴────────┴─────────┘
```



---

**<font style="color:#1DC0C9;">NaN 与 Null 的区别</font>**

这是一个很好的提醒，NaN 和 null 不是相同的类型。如果你需要填充一个系列中的这两种类型的值，可以将 `Expr.fill_nan()` 和 `Expr.fill_null()` 添加到表达式中。如果你需要判断一个值是 NaN 还是 null，可以结合使用 `Expr.is_nan()` 和 `Expr.is_null()`，并通过布尔“或”运算符（`|`）进行判断：

```python
(
    pl.DataFrame({"x": x})
    .with_columns(
        fill_both=pl.col("x").fill_nan(0).fill_null(0),
        is_either=(
            pl.col("x").is_nan() | pl.col("x").is_null()
        ),
    )
)
```

```plain
shape: (5, 3)
┌──────┬───────────┬───────────┐
│ x    │ fill_both │ is_either │
│ ---  │ ---       │ ---       │
│ f64  │ f64       │ bool      │
╞══════╪═══════════╪═══════════╡
│ 42.0 │ 42.0      │ false     │
│ NaN  │ 0.0       │ true      │
│ null │ 0.0       │ true      │
│ inf  │ inf       │ false     │
│ -inf │ -inf      │ false     │
└──────┴───────────┴───────────┘
```

在下一章中，你将了解更多关于布尔运算符的内容。至于是否真的需要将 NaN 和 null 视为相同的情况，取决于具体任务。

---



<h3 id="KDp44">**其他操作**</h3>
有三个元素级操作不属于上述任何类别（见表 7-5）。

_表 7-5. 其他元素级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.hash(...)</font>**` | 对选择中的元素进行哈希运算。 |
| `**<font style="color:#1DC0C9;">Expr.repeat_by(...)</font>**` | 按指定表达式重复此系列中的元素。 |
| `**<font style="color:#1DC0C9;">Expr.replace(...)</font>**` | 根据映射字典替换列中的值。 |


下面的代码片段展示了这三个方法的使用：

```python
(
    pl.DataFrame({"x": ["here", "there", "their", "they're"]})
    .with_columns(
        hash=pl.col("x").hash(seed=1337), ①
        repeat_by=pl.col("x").repeat_by(3),
        replace=pl.col("x").replace({
            "here": "there",
            "they're": "they are",
        }),
    )
)
```

```plain
shape: (4, 4)
┌─────────┬──────────────────────┬─────────────────────────────────┬──────────┐
│ x       │ hash                 │ repeat_by                       │ replace  │
│ ---     │ ---                  │ ---                             │ ---      │
│ str     │ u64                  │ list[str]                       │ str      │
╞═════════╪══════════════════════╪═════════════════════════════════╪══════════╡
│ here    │ 12695211751326448172 │ ["here", "here", "here"]        │ there    │
│ there   │ 17329794691236705436 │ ["there", "there", "there"]     │ there    │
│ their   │ 2663095961041830581  │ ["their", "their", "their"]     │ their    │
│ they're │ 6743063676290245144  │ ["they're", "they're", "they'r… │ they are │
└─────────┴──────────────────────┴─────────────────────────────────┴──────────┘
```

1. 使用 `Expr.hash()` 方法时，不同的计算机或不同版本的 Polars 将生成不同的哈希值。更多信息可以在 [AHash 网站](https://docs.rs/ahash/latest/ahash/) 上找到。



<h2 id="O96yW">**非缩减的系列级操作**</h2>
在接下来的部分中，我们不再关注元素级操作，而是系列级操作。这意味着整个系列会作为一个整体被转换，值本身（有时还有它们的顺序）相互依赖。例如，`Expr.cum_sum()` 方法用于计算累积和，`Expr.forward_fill()` 方法用于填充缺失值。

在接下来的六个小节中，我们将探讨不会改变系列长度的操作，包括累积、填充、移位、计算滚动统计、排序等操作。



<h3 id="b4lYQ">**累积操作**</h3>
累积操作通过系列进行进展，并保持例如累积和或最大值等状态。请参阅表 7-6，了解 Polars 提供的所有累积方法。

_表 7-6. 累积的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.cum_count(...)</font>**` | 获取在每个元素处计算的累积计数数组。 |
| `**<font style="color:#1DC0C9;">Expr.cum_max(...)</font>**` | 获取在每个元素处计算的累积最大值数组。 |
| `**<font style="color:#1DC0C9;">Expr.cum_min(...)</font>**` | 获取在每个元素处计算的累积最小值数组。 |
| `**<font style="color:#1DC0C9;">Expr.cum_prod(...)</font>**` | 获取在每个元素处计算的累积乘积数组。 |
| `**<font style="color:#1DC0C9;">Expr.cum_sum(...)</font>**` | 获取在每个元素处计算的累积和数组。 |
| `**<font style="color:#1DC0C9;">Expr.diff(...)</font>**` | 计算第 n 个离散差异。 |
| `**<font style="color:#1DC0C9;">Expr.pct_change(...)</font>**` | 计算值之间的百分比变化。 |


这些方法都接受一个参数 `reverse`，该参数指示在应用操作之前是否应该先将系列反转。以下代码片段对多种数值（包括缺失值和 NaN）应用了所有这些方法：

```python
(
    pl.DataFrame({"x": [0, 1, 2, None, 2, np.NaN, -1, 2]})
    .with_columns(
        cum_count=pl.col("x").cum_count(), ①
        cum_max=pl.col("x").cum_max(),
        cum_min=pl.col("x").cum_min(),
        cum_prod=pl.col("x").cum_prod(reverse=True), ②
        cum_sum=pl.col("x").cum_sum(),
        diff=pl.col("x").diff(),
        pct_change=pl.col("x").pct_change(),
    )
)
```

```plain
shape: (8, 8)
┌──────┬───────────┬─────────┬─────────┬──────────┬─────────┬──────┬────────────┐
│ x    │ cum_count │ cum_max │ cum_min │ cum_prod │ cum_sum │ diff │ pct_change │
│ ---  │ ---       │ ---     │ ---     │ ---      │ ---     │ ---  │ ---        │
│ f64  │ u32       │ f64     │ f64     │ f64      │ f64     │ f64  │ f64        │
╞══════╪═══════════╪═════════╪═════════╪══════════╪═════════╪══════╪════════════╡
│ 0.0  │ 1         │ 0.0     │ 0.0     │ NaN      │ 0.0     │ null │ null       │
│ 1.0  │ 2         │ 1.0     │ 0.0     │ NaN      │ 1.0     │ 1.0  │ inf        │
│ 2.0  │ 3         │ 2.0     │ 0.0     │ NaN      │ 3.0     │ 1.0  │ 1.0        │
│ null │ 3         │ null    │ null    │ null     │ null    │ null │ 0.0        │
│ 2.0  │ 4         │ 2.0     │ 0.0     │ NaN      │ 5.0     │ null │ 0.0        │
│ NaN  │ 5         │ 2.0     │ 0.0     │ NaN      │ NaN     │ NaN  │ NaN        │
│ -1.0 │ 6         │ 2.0     │ -1.0    │ -2.0     │ NaN     │ NaN  │ NaN        │
│ 2.0  │ 7         │ 2.0     │ -1.0    │ 2.0      │ NaN     │ 3.0  │ -3.0       │
└──────┴───────────┴─────────┴─────────┴──────────┴─────────┴──────┴────────────┘
```

1. `Expr.cum_count()` 方法不会计数缺失值。
2. 如果我们不反转这个操作，那么整个列将被零填充。



---

**<font style="color:#1DC0C9;">传播的 NaN</font>**

NaN 可能会影响系列级操作的输出。在此示例中：

+ `Expr.cum_count()`、`Expr.cum_max()` 和 `Expr.cum_min()` 的输出完全不受影响。
+ 一旦遇到 NaN，`Expr.cum_prod()` 和 `Expr.cum_sum()` 的输出会受到持续影响。
+ 对于每个 NaN，`Expr.diff()` 和 `Expr.pct_change()` 的输出只会受到两个值的影响。

---

****

<h3 id="oDrIy">**填充和移位操作**</h3>
表 7-7 列出了用于填充和移位的非缩减系列级方法。

__

_表 7-7. 用于填充和移位的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.backward_fill(...)</font>**` | 用下一个可见的值填充缺失值。 |
| `**<font style="color:#1DC0C9;">Expr.forward_fill(...)</font>**` | 用最近看到的值填充缺失值。 |
| `**<font style="color:#1DC0C9;">Expr.interpolate(...)</font>**` | 使用插值法填充缺失值。 |
| `**<font style="color:#1DC0C9;">Expr.shift(...)</font>**` | 按指定的周期移位值。 |


让我们将 `Expr.backward_fill()`、`Expr.forward_fill()`、`Expr.interpolate()`（两次）和 `Expr.shift()`（两次）应用于一些值，包括缺失值：

```python
(
    pl.DataFrame({"x": [-1, 0, 1, None, None, 3, 4, math.nan, 6]})
    .with_columns(
        backward_fill=pl.col("x").backward_fill(), ①
        forward_fill=pl.col("x").forward_fill(limit=1),
        interp1=pl.col("x").interpolate(method="linear"), ②
        interp2=pl.col("x").interpolate(method="nearest"),
        shift1=pl.col("x").shift(1),
        shift2=pl.col("x").shift(-2),
    )
)
```

```plain
shape: (9, 7)
┌──────┬───────────────┬──────────────┬──────────┬─────────┬────────┬────────┐
│ x    │ backward_fill │ forward_fill │ interp1  │ interp2 │ shift1 │ shift2 │
│ ---  │ ---           │ ---          │ ---      │ ---     │ ---    │ ---    │
│ f64  │ f64           │ f64          │ f64      │ f64     │ f64    │ f64    │
╞══════╪═══════════════╪══════════════╪══════════╪═════════╪════════╪════════╡
│ -1.0 │ -1.0          │ -1.0         │ -1.0     │ -1.0    │ null   │ 1.0    │
│ 0.0  │ 0.0           │ 0.0          │ 0.0      │ 0.0     │ -1.0   │ null   │
│ 1.0  │ 1.0           │ 1.0          │ 1.0      │ 1.0     │ 0.0    │ null   │
│ null │ 3.0           │ 1.0          │ 1.666667 │ 1.0     │ 1.0    │ 3.0    │
│ null │ 3.0           │ null         │ 2.333333 │ 3.0     │ null   │ 4.0    │
│ 3.0  │ 3.0           │ 3.0          │ 3.0      │ 3.0     │ null   │ NaN    │
│ 4.0  │ 4.0           │ 4.0          │ 4.0      │ 4.0     │ 3.0    │ 6.0    │
│ NaN  │ NaN           │ NaN          │ NaN      │ NaN     │ 4.0    │ null   │
│ 6.0  │ 6.0           │ 6.0          │ 6.0      │ 6.0     │ NaN    │ null   │
└──────┴───────────────┴──────────────┴──────────┴─────────┴────────┴────────┘
```

①NaN 值不会被填充或插值。

② 注意两种插值方法 `linear` 和 `nearest` 之间的区别。前者在系列中的前一个和下一个非缺失值之间进行插值，而后者使用实际最近的非缺失值。



<h3 id="kg4Ij">**与重复值相关的操作**</h3>
有四个非缩减的系列级方法处理唯一值和重复值（见表 7-8）。还有其他处理重复值的方法，但它们会缩短系列的长度，因此将在本章稍后讨论。

_表 7-8. 返回布尔型系列的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.is_duplicated()</font>**` | 获取一个布尔型系列，指示哪些值是重复的。 |
| `**<font style="color:#1DC0C9;">Expr.is_first_distinct()</font>**` | 获取一个布尔型系列，指示哪些值是首次唯一的。 |
| `**<font style="color:#1DC0C9;">Expr.is_last_distinct()</font>**` | 获取一个布尔型系列，指示哪些值是最后唯一的。 |
| `**<font style="color:#1DC0C9;">Expr.is_unique()</font>**` | 获取一个布尔型系列，指示哪些值是唯一的。 |


下面我们将这四个方法应用于一组字符串：

```python
(
    pl.DataFrame({"x": ["A", "C", "D", "C"]}) ①
    .with_columns(
        is_duplicated=pl.col("x").is_duplicated(),
        is_first_distinct=pl.col("x").is_first_distinct(),
        is_last_distinct=pl.col("x").is_last_distinct(),
        is_unique=pl.col("x").is_unique(),
    )
)
```

```plain
shape: (4, 5)
┌─────┬───────────────┬───────────────────┬──────────────────┬───────────┐
│ x   │ is_duplicated │ is_first_distinct │ is_last_distinct │ is_unique │
│ --- │ ---           │ ---               │ ---              │ ---       │
│ str │ bool          │ bool              │ bool             │ bool      │
╞═════╪═══════════════╪═══════════════════╪══════════════════╪═══════════╡
│ A   │ false         │ true              │ true             │ true      │
│ C   │ true          │ true              │ false            │ false     │
│ D   │ false         │ true              │ true             │ true      │
│ C   │ true          │ false             │ true             │ false     │
└─────┴───────────────┴───────────────────┴──────────────────┴───────────┘
```

请记住这里的很多方法也适用于其他数据类型。



<h3 id="Fpy9O">**计算滚动统计的操作**</h3>
滚动统计用于平滑系列的值（参见表 7-9）。

_表 7-9. 用于滚动统计的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.ewm_mean(...)</font>**` | 计算指数加权移动平均。 |
| `**<font style="color:#1DC0C9;">Expr.ewm_std(...)</font>**` | 计算指数加权移动标准差。 |
| `**<font style="color:#1DC0C9;">Expr.ewm_var(...)</font>**` | 计算指数加权移动方差。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_apply(...)</font>**` | 应用自定义的滚动窗口函数。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_map(...)</font>**` | 计算自定义的滚动窗口函数。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_max(...)</font>**` | 在数组中应用滚动最大值（移动最大值）。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_mean(...)</font>**` | 在数组中应用滚动均值（移动均值）。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_median(...)</font>**` | 计算滚动中位数。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_min(...)</font>**` | 在数组中应用滚动最小值（移动最小值）。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_quantile(...)</font>**` | 计算滚动分位数。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_skew(...)</font>**` | 计算滚动偏度。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_std(...)</font>**` | 计算滚动标准差。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_sum(...)</font>**` | 在数组中应用滚动和（移动和）。 |
| `**<font style="color:#1DC0C9;">Expr.rolling_var(...)</font>**` | 计算滚动方差。 |


以下代码片段将 `Expr.ewm_mean()`、`Expr.rolling_mean()` 和 `Expr.rolling_min()` 应用于某些股票数据的 `close` 列。其余方法的工作方式类似：

```plain
shape: (124, 5)
┌────────────┬────────────┬────────────┬──────────────┬─────────────┐
│ date       │ close      │ ewm_mean   │ rolling_mean │ rolling_min │
│ ---        │ ---        │ ---        │ ---          │ ---         │
│ date       │ f64        │ f64        │ f64          │ f64         │
╞════════════╪════════════╪════════════╪══════════════╪═════════════╡
│ 2023-01-03 │ 143.149994 │ 143.149994 │ null         │ null        │
│ 2023-01-04 │ 147.490005 │ 145.464667 │ null         │ null        │
│ 2023-01-05 │ 142.649994 │ 144.398755 │ null         │ null        │
│ 2023-01-06 │ 148.589996 │ 145.664782 │ null         │ null        │
│ 2023-01-09 │ 156.279999 │ 148.388917 │ null         │ null        │
│ …          │ …          │ …          │ …            │ …           │
│ 2023-06-26 │ 406.320007 │ 407.54911  │ 425.805716   │ 406.320007  │
│ 2023-06-27 │ 418.76001  │ 408.950473 │ 424.695718   │ 406.320007  │
│ 2023-06-28 │ 411.170013 │ 409.227915 │ 422.445718   │ 406.320007  │
│ 2023-06-29 │ 408.220001 │ 409.101926 │ 418.180006   │ 406.320007  │
│ 2023-06-30 │ 423.019989 │ 410.841684 │ 417.118574   │ 406.320007  │
└────────────┴────────────┴────────────┴──────────────┴─────────────┘
```



因为在表格中很难看出这些方法的区别，所以让我们将其可视化（参见图 7-2）。

```python
from matplotlib.dates import DateFormatter

stock.plot.line(
    x="date",
    y=["close", "ewm_mean", "rolling_mean", "rolling_min"],
    xformatter=DateFormatter("%b %Y")
)
```

![图 7-2. 多种滚动统计操作应用于股票数据](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728814672840-402ad31c-0ab8-4918-9829-544c56038186.png)

在第 17 章您将学到更多关于使用 Polars 创建并可视化数据的内容。



<h3 id="WmeU8">**排序操作**</h3>
表 7-10 列出了 Polars 提供的用于排序表达式的方法。

_表 7-10. 用于排序的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.arg_sort(...)</font>**` | 获取对该列排序的索引值。 |
| `**<font style="color:#1DC0C9;">Expr.shuffle(...)</font>**` | 打乱该表达式的内容。 |
| `**<font style="color:#1DC0C9;">Expr.sort(...)</font>**` | 对该列进行排序。 |
| `**<font style="color:#1DC0C9;">Expr.sort_by(...)</font>**` | 按其他列的顺序对该列进行排序。 |
| `**<font style="color:#1DC0C9;">Expr.reverse()</font>**` | 反转选择。 |
| `**<font style="color:#1DC0C9;">Expr.rank(...)</font>**` | 给数据分配排名，适当处理平局。 |


---

**<font style="color:#ECAA04;">排序表达式并不常见</font>**

在现实世界的数据集中，一行通常代表一个观察值或事件。因此，你很可能希望对整个行进行排序，以确保每个观察值或事件的测量保持在一起。然而，本节中的方法仅处理单个表达式或列。

---

让我们将这六种方法应用于一组数字。我们还添加了一个列 `y` 来演示 `Expr.sort_by()` 方法：

```python
(
    pl.DataFrame({
        "x": [1, 3, None, 3, 7],
        "y": ["D", "I", "S", "C", "O"],
    })
    .with_columns(
        arg_sort=pl.col("x").arg_sort(),
        shuffle=pl.col("x").shuffle(seed=7),
        sort=pl.col("x").sort(nulls_last=True),
        sort_by=pl.col("x").sort_by("y"),
        reverse=pl.col("x").reverse(),
        rank=pl.col("x").rank(),
    )
)
```

```plain
shape: (5, 8)
┌──────┬─────┬──────────┬─────────┬──────┬─────────┬─────────┬──────┐
│ x    │ y   │ arg_sort │ shuffle │ sort │ sort_by │ reverse │ rank │
│ ---  │ --- │ ---      │ ---     │ ---  │ ---     │ ---     │ ---  │
│ i64  │ str │ u32      │ i64     │ i64  │ i64     │ i64     │ f64  │
╞══════╪═════╪══════════╪═════════╪══════╪═════════╪═════════╪══════╡
│ 1    │ D   │ 2        │ 1       │ 1    │ 3       │ 7       │ 1.0  │
│ 3    │ I   │ 0        │ null    │ 3    │ 1       │ 3       │ 2.5  │
│ null │ S   │ 1        │ 3       │ 3    │ 3       │ null    │ null │
│ 3    │ C   │ 3        │ 7       │ 7    │ 7       │ 3       │ 2.5  │
│ 7    │ O   │ 4        │ 3       │ null │ null    │ 1       │ 4.0  │
└──────┴─────┴──────────┴─────────┴──────┴─────────┴─────────┴──────┘
```



<h3 id="zOloG">**其他操作**</h3>
有一个非缩减的系列级操作不属于上述任何类别（见表 7-11）。

_表 7-11. 其他系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.rle_id()</font>**` | 将值映射为运行的 ID。 |


让我们将 `Expr.rle_id()` 应用于一个数值系列：

```python
(
    pl.DataFrame({"x": [33, 33, 27, 33, 60, 60, 60, 33, 60]})
    .with_columns(
        rle_id=pl.col("x").rle_id(),
    )
)
```

```plain
shape: (9, 2)
┌─────┬────────┐
│ x   │ rle_id │
│ --- │ ---    │
│ i64 │ u32    │
╞═════╪════════╡
│ 33  │ 0      │
│ 33  │ 0      │
│ 27  │ 1      │
│ 33  │ 2      │
│ 60  │ 3      │
│ 60  │ 3      │
│ 60  │ 3      │
│ 33  │ 4      │
│ 60  │ 5      │
└─────┴────────┘
```





<h2 id="B5aB1">**将 Series 归纳为一个值的 Series 级操作**</h2>
我们继续讨论系列级（Series-wise) 操作，但在本节中，我们关注的是将系列中的所有值归纳为一个值的操作。例如，用于计算平均值的 `Expr.mean()` 方法和用于计算缺失值数量的 `Expr.null_count()` 方法。

在接下来的四个小节中，我们将探讨通过统计、计数和其他方法使用量化器将系列归纳为一个值的操作。

---

**<font style="color:#1DC0C9;">重复值</font>**

如果使用的操作将系列归纳为一个值，并且你保留了任何原始列，则计算出的值会重复。例如，这里系列的平均值被重复了四次：

```python
(
    pl.DataFrame({"x": [1, 3, 3, 7]})
    .with_columns(
        mean=pl.col("x").mean(),
    )
)
```

```plain
shape: (4, 2)
┌─────┬──────┐
│ x   │ mean │
│ --- │ ---  │
│ i64 │ f64  │
╞═════╪══════╡
│ 1   │ 3.5  │
│ 3   │ 3.5  │
│ 3   │ 3.5  │
│ 7   │ 3.5  │
└─────┴──────┘
```



由于归纳操作通常在聚合上下文中使用，因此这并不是真正的问题。例如，当你按组计算平均值时：

```python
(
    pl.DataFrame({
        "cluster": ["a", "a", "b", "b", "b"],
        "x": [1, 3, 3, 3, 7]
    })
    .group_by("cluster")
    .agg(
        mean=pl.col("x").mean(),
    )
)
```

```plain
shape: (2, 2)
┌─────────┬──────┐
│ cluster │ mean │
│ ---     │ ---  │
│ str     │ f64  │
├─────────┼──────┤
│ b       │ 5.0  │
│ a       │ 2.0  │
└─────────┴──────┘
```

在本章的剩余部分，我们将使用 `df.select()` 方法来排除原始列，从而避免重复的值。

---

****

<h3 id="KAxt9">**量词操作**</h3>
使用量化器(quantifiers) 可以将多个布尔值归纳为一个值。Polars 通过 `Expr.all()` 和 `Expr.any()` 支持全称量化和存在量化（参见表 7-12）。

**表 7-12. 使用量化器归纳为一个值的系列级操作**

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.all(...)</font>**` | 返回列中的所有值是否都为 True。 |
| `**<font style="color:#1DC0C9;">Expr.any(...)</font>**` | 返回列中的任何值是否为 True。 |


两种方法都接受一个参数 `ignore_nulls`，指示是否应忽略缺失值。以下代码片段将 `Expr.all()` 和 `Expr.any()` 应用于三个布尔列 `x`、`y` 和 `z`：

```python
(
    pl.DataFrame({
        "x": [True, False, False],
        "y": [True, True, True],
        "z": [False, False, False],
    })
    .select(
        pl.all().all().name.suffix("_all"),
        pl.all().any().name.suffix("_any"),
    )
)
```

```plain
shape: (1, 6)
┌───────┬───────┬───────┬───────┬───────┬───────┐
│ x_all │ y_all │ z_all │ x_any │ y_any │ z_any │
│ ---   │ ---   │ ---   │ ---   │ ---   │ ---   │
│ bool  │ bool  │ bool  │ bool  │ bool  │ bool  │
╞═══════╪═══════╪═══════╪═══════╪═══════╪═══════╡
│ false │ true  │ false │ true  │ true  │ false │
└───────┴───────┴───────┴───────┴───────┴───────┘
```



<h3 id="j3iey">**计算统计量的操作**</h3>
Polars 支持许多方法来计算数值系列的各种统计量（参见表 7-13）。

_表 7-13. 通过计算统计量将系列归纳为一个元素的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.entropy(...)</font>**` | 计算熵值。 |
| `**<font style="color:#1DC0C9;">Expr.kurtosis(...)</font>**` | 计算数据集的峰度（Fisher 或 Pearson）。 |
| `**<font style="color:#1DC0C9;">Expr.max()</font>**` | 获取最大值。 |
| `**<font style="color:#1DC0C9;">Expr.mean()</font>**` | 获取平均值。 |
| `**<font style="color:#1DC0C9;">Expr.median()</font>**` | 使用线性插值获取中位数。 |
| `**<font style="color:#1DC0C9;">Expr.min()</font>**` | 获取最小值。 |
| `**<font style="color:#1DC0C9;">Expr.nan_max()</font>**` | 获取最大值，但传播/毒化遇到的 NaN 值。 |
| `**<font style="color:#1DC0C9;">Expr.nan_min()</font>**` | 获取最小值，但传播/毒化遇到的 NaN 值。 |
| `**<font style="color:#1DC0C9;">Expr.product()</font>**` | 计算表达式的乘积。 |
| `**<font style="color:#1DC0C9;">Expr.quantile(...)</font>**` | 获取分位数值。 |
| `**<font style="color:#1DC0C9;">Expr.skew(...)</font>**` | 计算数据集的样本偏度。 |
| `**<font style="color:#1DC0C9;">Expr.std(...)</font>**` | 获取标准差。 |
| `**<font style="color:#1DC0C9;">Expr.sum()</font>**` | 获取和值。 |
| `**<font style="color:#1DC0C9;">Expr.var(...)</font>**` | 获取方差。 |


在以下代码片段中，我们将 `Expr.max()`、`Expr.mean()`、`Expr.quantile()`、`Expr.skew()`、`Expr.std()`、`Expr.sum()` 和 `Expr.var()` 方法应用于一百万个值。这些值来自均值为 5、标准差为 3 的正态分布。

```python
samples = rng.normal(loc=5, scale=3, size=1_000_000)

(
    pl.DataFrame({"x": samples})
    .select(
        max=pl.col("x").max(),
        mean=pl.col("x").mean(),
        quantile=pl.col("x").quantile(quantile=0.95),
        skew=pl.col("x").skew(),
        std=pl.col("x").std(),
        sum=pl.col("x").sum(),
        var=pl.col("x").var(),
    )
)
```

```plain
shape: (1, 7)
┌───────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ max       │ mean     │ quantile │ skew     │ std      │ sum      │ var      │
│ ---       │ ---      │ ---      │ ---      │ ---      │ ---      │ ---      │
│ f64       │ f64      │ f64      │ f64      │ f64      │ f64      │ f64      │
╞═══════════╪══════════╪══════════╪══════════╪══════════╪══════════╪══════════╡
│ 20.752443 │ 4.994978 │ 9.931565 │ 0.003245 │ 2.999926 │ 4.9950e6 │ 8.999558 │
└───────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
```



其他方法如 `Expr.entropy()`、`Expr.kurtosis()`、`Expr.median()`、`Expr.min()`、`Expr.nan_max()`、`Expr.nan_min()` 和 `Expr.product()` 的工作方式类似。



<h3 id="zh3i5">**计数操作**</h3>
Polars 提供了几种用于计数的方法（参见表 7-14）。

_表 7-14. 通过计数将系列归纳为一个元素的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.approx_n_unique()</font>**` | 近似计数唯一值的数量。 |
| `**<font style="color:#1DC0C9;">Expr.count()</font>**` | 计数该表达式中的值的数量。 |
| `**<font style="color:#1DC0C9;">Expr.len()</font>**` | 计数该表达式中的值的数量。 |
| `**<font style="color:#1DC0C9;">Expr.n_unique()</font>**` | 计数唯一值的数量。 |
| `**<font style="color:#1DC0C9;">Expr.null_count()</font>**` | 计数空值的数量。 |


为了演示这些方法，让我们生成 1,729 个随机整数，范围在 0 到 10,000 之间，并将其中一个值设为缺失值：

```python
samples = pl.Series(rng.integers(low=0, high=10_000, size=1_729))
samples[403] = None  ①
df_ints = (
    pl.DataFrame({"x": samples})
    .with_row_index()  ②
)
df_ints.slice(400, 6)  ③
```

```plain
shape: (6, 2)
┌───────┬──────┐
│ index │ x    │
│ ---   │ ---  │
│ u32   │ i64  │
╞═══════╪══════╡
│ 400   │ 807  │
│ 401   │ 8634 │
│ 402   │ 2109 │
│ 403   │ null │
│ 404   │ 1740 │
│ 405   │ 3333 │
└───────┴──────┘
```

① 第403个元素被设为缺失值。

②`DataFrame` 方法 `df.with_row_index()` 将行索引添加为第一列。

③ 我们使用 `DataFrame` 方法 `df.slice()` 来显示行的子集。



让我们将这五个方法应用于列 `x`：

```python
df_ints.select(
    approx_n_unique=pl.col("x").approx_n_unique(),
    count=pl.col("x").count(),
    len=pl.col("x").len(),
    n_unique=pl.col("x").n_unique(),
    null_count=pl.col("x").null_count(),
)
```

```plain
shape: (1, 5)
┌─────────────────┬───────┬──────┬──────────┬────────────┐
│ approx_n_unique │ count │ len  │ n_unique │ null_count │
│ ---             │ ---   │ ---  │ ---      │ ---        │
│ u32             │ u32   │ u32  │ u32      │ u32        │
╞═════════════════╪═══════╪══════╪══════════╪════════════╡
│ 1572            │ 1728  │ 1729 │ 1575     │ 1          │
└─────────────────┴───────┴──────┴──────────┴────────────┘
```



<h3 id="iPQQz">**其他操作**</h3>
有八个系列级操作可以归纳为一个值，这些操作不属于任何上述类别（参见表 7-15）。

_表 7-15. 几种其他将系列归纳为一个元素的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.arg_max()</font>**` | 获取最大值的索引。 |
| `**<font style="color:#1DC0C9;">Expr.arg_min()</font>**` | 获取最小值的索引。 |
| `**<font style="color:#1DC0C9;">Expr.first()</font>**` | 获取第一个值。 |
| `**<font style="color:#1DC0C9;">Expr.get(...)</font>**` | 按索引返回一个单一值。 |
| `**<font style="color:#1DC0C9;">Expr.implode()</font>**` | 将值聚合为一个列表。 |
| `**<font style="color:#1DC0C9;">Expr.last()</font>**` | 获取最后一个值。 |
| `**<font style="color:#1DC0C9;">Expr.lower_bound()</font>**` | 计算下界。 |
| `**<font style="color:#1DC0C9;">Expr.upper_bound()</font>**` | 计算上界。 |


下面我们将 `Expr.arg_min()`、`Expr.first()`、`Expr.get()`、`Expr.implode()`、`Expr.last()` 和 `Expr.upper_bound()` 方法应用于上一节中的相同值。方法 `Expr.arg_max()` 类似于 `Expr.arg_min()`，`Expr.lower_bound()` 类似于 `Expr.upper_bound()`。

```python
df_ints.select(
    arg_min=pl.col("x").arg_min(),
    first=pl.col("x").first(),
    get=pl.col("x").get(403),  ①
    implode=pl.col("x").implode(),
    last=pl.col("x").last(),
    upper_bound=pl.col("x").upper_bound(),
)
```

```plain
shape: (1, 6)
┌─────────┬───────┬──────┬───────────────────┬──────┬─────────────────────┐
│ arg_min │ first │ get  │ implode           │ last │ upper_bound         │
│ ---     │ ---   │ ---  │ ---               │ ---  │ ---                 │
│ u32     │ i64   │ i64  │ list[i64]         │ i64  │ i64                 │
╞═════════╪═══════╪══════╪═══════════════════╪══════╪═════════════════════╡
│ 0       │ 0     │ null │ [0, 7245, … 3723] │ 3723 │ 9223372036854775807 │
└─────────┴───────┴──────┴───────────────────┴──────┴─────────────────────┘
```

① 结果为 `null`，因为在上一节中我们将第403个元素设为缺失值。



<h2 id="OOTVu">**将 Series 归纳为一个或多个值的系列级操作**</h2>
除了将系列(Series)归纳为一个值的系列级操作外，还有一些将其归纳为一个或多个值。输出系列的实际长度取决于这些值。

在接下来的四个小节中，我们将讨论基于唯一值、选择、删除缺失值等操作，将系列归纳为一个或多个值。



<h3 id="mMhdg">**与唯一值相关的操作**</h3>
表 7-16 列出了与唯一值相关的四种方法。

__

_表 7-16. 几种基于唯一值将系列归纳为一个或多个元素的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.arg_unique()</font>**` | 获取第一个唯一值的索引。 |
| `**<font style="color:#1DC0C9;">Expr.unique(...)</font>**` | 获取此表达式的唯一值。 |
| `**<font style="color:#1DC0C9;">Expr.unique_counts()</font>**` | 返回唯一值的计数，按出现顺序。 |
| `**<font style="color:#1DC0C9;">Expr.value_counts(...)</font>**` | 统计唯一值的出现次数。 |


让我们将这四种方法应用于一组字符串：

```python
(
    pl.DataFrame({"x": ["A", "C", "D", "C"]})
    .select(
        arg_unique=pl.col("x").arg_unique(),
        unique=pl.col("x").unique(maintain_order=True),  ①
        unique_counts=pl.col("x").unique_counts(),
        value_counts=pl.col("x").value_counts(),  ②
    )
)
```

```plain
shape: (3, 4)
┌────────────┬────────┬───────────────┬──────────────┐
│ arg_unique │ unique │ unique_counts │ value_counts │
│ ---        │ ---    │ ---           │ ---          │
│ u32        │ str    │ u32           │ struct[2]    │
╞════════════╪════════╪═══════════════╪══════════════╡
│ 0          │ A      │ 1             │ {"C",2}      │
│ 1          │ C      │ 2             │ {"D",1}      │
│ 2          │ D      │ 1             │ {"A",1}      │
└────────────┴────────┴───────────────┴──────────────┘
```

① 保持值的顺序在计算上更为密集。

②`Expr.value_counts()` 的结果是数据类型 `pl.Struct`，它是 `Expr.unique()` 和 `Expr.unique_counts()` 的组合，尽管不一定按相同顺序排列。



<h3 id="uvEej">**选择操作**</h3>
表 7-17 列出了几种基于位置或值选择特定元素的方法。

_表 7-17. 通过选择将系列归纳为一个或多个元素的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.bottom_k(...)</font>**` | 返回 k 个最小元素。 |
| `**<font style="color:#1DC0C9;">Expr.head(...)</font>**` | 获取前 n 行。 |
| `**<font style="color:#1DC0C9;">Expr.limit(...)</font>**` | 获取前 n 行（`Expr.head()` 的别名）。 |
| `**<font style="color:#1DC0C9;">Expr.sample(...)</font>**` | 从该表达式中取样。 |
| `**<font style="color:#1DC0C9;">Expr.slice(...)</font>**` | 获取该表达式的一个切片。 |
| `**<font style="color:#1DC0C9;">Expr.tail(...)</font>**` | 获取最后 n 行。 |
| `**<font style="color:#1DC0C9;">Expr.gather(...)</font>**` | 按索引获取值。 |
| `**<font style="color:#1DC0C9;">Expr.gather_every(...)</font>**` | 获取系列中的每 n 个值，并作为新系列返回。 |
| `**<font style="color:#1DC0C9;">Expr.top_k(...)</font>**` | 返回 k 个最大元素。 |


以下代码片段将方法 `Expr.bottom_k()`、`Expr.head()`、`Expr.sample()`、`Expr.slice()`、`Expr.gather()`、`Expr.gather_every()` 和 `Expr.top_k()` 应用于之前生成的样本。

`Expr.limit()` 方法是 `Expr.head()` 的别名。`Expr.tail()` 的工作方式与 `Expr.head()` 类似，只不过它从底部开始。

```python
df_ints.select(
    bottom_k=pl.col("x").bottom_k(7),  ①
    head=pl.col("x").head(7),
    sample=pl.col("x").sample(7),
    slice=pl.col("x").slice(400, 7),
    gather=pl.col("x").gather([1, 1, 2, 3, 5, 8, 13]),
    gather_every=pl.col("x").gather_every(247),  ②
    top_k=pl.col("x").top_k(7),
)
```

```plain
shape: (7, 7)
┌──────────┬──────┬────────┬───────┬────────┬──────────────┬───────┐
│ bottom_k │ head │ sample │ slice │ gather │ gather_every │ top_k │
│ ---      │ ---  │ ---    │ ---   │ ---    │ ---          │ ---   │
│ i64      │ i64  │ i64    │ i64   │ i64    │ i64          │ i64   │
╞══════════╪══════╪════════╪═══════╪════════╪══════════════╪═══════╡
│ null     │ 0    │ 6871   │ 807   │ 7245   │ 0            │ 9998  │
│ 0        │ 7245 │ 2202   │ 8634  │ 7245   │ 8680         │ 9988  │
│ 1        │ 5227 │ 7328   │ 2109  │ 5227   │ 8483         │ 9988  │
│ 6        │ 2747 │ 1648   │ null  │ 2747   │ 8358         │ 9986  │
│ 7        │ 9816 │ 5761   │ 1740  │ 2657   │ 1805         │ 9985  │
│ 10       │ 2657 │ 9315   │ 3333  │ 5393   │ 3638         │ 9979  │
│ 21       │ 4578 │ 8370   │ 788   │ 8203   │ 5843         │ 9975  │
└──────────┴──────┴────────┴───────┴────────┴──────────────┴───────┘
```

① 注意`nulls` 是在前面的。

② 必须匹配高度为7，否则你会收到一个错误提示说长度不匹配。在此示例中，从长度为1,729的系列中每247个值取一个值，将得到7个值。



<h3 id="s0NMi">**删除缺失值的操作**</h3>
表 7-18 列出了两种用于删除缺失值的方法：`Expr.drop_nans()` 和 `Expr.drop_nulls()`。

_表 7-18. 几种通过删除缺失值将系列归纳为一个或多个元素的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.drop_nans()</font>**` | 删除浮点型 NaN 值。 |
| `**<font style="color:#1DC0C9;">Expr.drop_nulls()</font>**` | 删除所有 null 值。 |




以下是如何应用这两种方法的示例：

```python
x = [None, 1, 2, 3, np.NaN]
(
    pl.DataFrame({"x": x})
    .select(
        drop_nans=pl.col("x").drop_nans(),
        drop_nulls=pl.col("x").drop_nulls()
    )
)
```

```plain
shape: (4, 2)
┌───────────┬────────────┐
│ drop_nans │ drop_nulls │
│ ---       │ ---        │
│ f64       │ f64        │
╞═══════════╪════════════╡
│ null      │ 1.0        │
│ 1.0       │ 2.0        │
│ 2.0       │ 3.0        │
│ 3.0       │ NaN        │
└───────────┴────────────┘
```



<h3 id="O38GC">**其他操作**</h3>
有六个系列级操作可以归纳为一个或多个值，这些操作不属于任何上述类别（参见表 7-19）。

_表 7-19. 几种其他将系列归纳为一个或多个元素的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.arg_true()</font>**` | 返回表达式结果为 `True` 的索引。 |
| `**<font style="color:#1DC0C9;">Expr.flatten()</font>**` | 将列表或字符串列展平。 |
| `**<font style="color:#1DC0C9;">Expr.mode()</font>**` | 计算出现次数最多的值。 |
| `**<font style="color:#1DC0C9;">Expr.reshape(...)</font>**` | 将表达式重塑为一个平系列或一个列表系列。 |
| `**<font style="color:#1DC0C9;">Expr.rle()</font>**` | 获取相同值连续出现的长度。 |
| `**<font style="color:#1DC0C9;">Expr.search_sorted(...)</font>**` | 查找元素应插入以保持顺序的索引。 |


下面我们将方法 `Expr.arg_true()`、`Expr.mode()`、`Expr.reshape()`、`Expr.rle()` 和 `Expr.search_sorted()` 应用于一个无序的整数系列。我们将单独演示这些方法，因为它们构造的系列具有不同的长度。

首先，`Expr.arg_true()` 方法可以按如下方式应用：

```python
numbers = [33, 33, 27, 33, 60, 60, 60, 33, 60]
(
    pl.DataFrame({"x": numbers})
    .select(
        arg_true=(pl.col("x") >= 60).arg_true(), ①
    )
)
```

```plain
shape: (4, 1)
┌──────────┐
│ arg_true │
│ ---      │
│ u32      │
╞══════════╡
│ 4        │
│ 5        │
│ 6        │
│ 8        │
└──────────┘
```

① 我们首先使用大于等于运算符 (`>=`) 来获取一个布尔系列。你将在下一章中学习更多关于此和其他比较运算符的内容。



其次，`Expr.mode()` 方法可以按如下方式应用：

```python
(
    pl.DataFrame({"x": numbers})
    .select(
        mode=pl.col("x").mode(),
    )
)
```

```plain
shape: (2, 1)
┌──────┐
│ mode │
│ ---  │
│ i64  │
╞══════╡
│ 60   │
│ 33   │
└──────┘
```



再次，`Expr.reshape()`可以按如下方式应用：

```python
(
    pl.DataFrame({"x": numbers})
    .select(
        reshape=pl.col("x").reshape((3, 3)),  ①
    )
)
```

```plain
shape: (3, 1)
┌──────────────┐
│ reshape      │
│ ---          │
│ list[i64]    │
╞══════════════╡
│ [33, 33, 27] │
│ [33, 60, 60] │
│ [60, 33, 60] │
└──────────────┘
```

① 元素的总数需要保持不变。例如，不可能将其重塑为 5 行，其中最后一行是一个包含单个元素的 `pl.List`。



又次，`Expr.rle()` 方法可以按如下方式应用：

```python
(
    pl.DataFrame({"x": numbers})
    .select(
        rle=pl.col("x").rle(),  ①
    )
)
```

```plain
shape: (6, 1)
┌───────────┐
│ rle       │
│ ---       │
│ struct[2] │
╞═══════════╡
│ {2,33}    │
│ {1,27}    │
│ {1,33}    │
│ {3,60}    │
│ {1,33}    │
│ {1,60}    │
└───────────┘
```

① 与本章前面讨论的 `Expr.rle_id()` 进行比较。



最后，`Expr.search_sorted()` 方法可以按如下方式应用：

```python
(
    pl.DataFrame({"x": numbers})
    .select(
        rle=pl.col("x").sort().search_sorted(42),
    )
)
```

①`Expr.search_sorted()` 方法在已排序的系列中最有用。



<h2 id="h58x5">**扩展系列的系列级操作**</h2>
只有两个操作可以扩展系列的长度（参见表 7-20）。

_表 7-20. 两个扩展系列的系列级操作_

| 方法 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">Expr.explode()</font>**` | 拆解列表表达式。 |
| `**<font style="color:#1DC0C9;">Expr.extend_constant(...)</font>**` | 使用常量值扩展系列。 |




下面，我们使用 `Expr.explode()` 方法将列表系列转换为常规的平系列：

```python
(
    pl.DataFrame({
        "x": [["a", "b"], ["c", "d"]],
    })
    .select(
        explode=pl.col("x").explode()
    )
)
```

```plain
shape: (4, 1)
┌─────────┐
│ explode │
│ ---     │
│ str     │
╞═════════╡
│ a       │
│ b       │
│ c       │
│ d       │
└─────────┘
```



我们在本章开头展示了 `Expr.extend_constant()` 方法。



<h2 id="BUAbf">**总结**</h2>
在本章中，你学习了许多不同的方法来通过额外的操作继续表达式。这些操作是根据它们构造的系列长度进行组织的。在下一章中，你将学习如何组合表达式。

---

1. 从技术上讲，方法 `Expr.qcut()` 不是逐元素操作，因为分位数是基于整个系列的。在这种情况下，我们认为最好将它与其近亲 `Expr.cut()` 放在一起。

---

