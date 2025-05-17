

正如你在前几章中所看到的，Polars的API已经相当广泛，涵盖了许多功能。然而，有时你可能需要通过自定义功能来扩展Polars。这可能是因为你有一个内置函数无法覆盖的特定使用场景，或者你希望优化代码的性能。在本章中，我们将探讨如何在Python和Rust中使用自定义函数和表达式来扩展Polars。我们将介绍如何注册自定义命名空间、创建自定义Rust插件，以及在插件中使用Rust的crate。

<h2 id="jKmi5">Python中的用户自定义函数</h2>
Polars提供了一系列丰富的表达式，使你可以执行各种操作。然而，有时你需要执行的操作不在现有表达式的范围内，或者需要通过外部包来完成。为此，Polars允许使用用户自定义函数（UDFs）。可以使用以下Polars函数来实现这一点：

+ `map_elements`：对列的每个元素应用一个Python函数。
+ `map_batches`：对一个Series或一组Series应用一个Python函数。
+ `map_groups`：在GroupBy上下文中，对每个分组应用一个Python函数。

让我们深入了解如何使用这些函数将自定义Python函数应用于你的数据。

`map_elements`函数允许你对列中的每个元素应用Python函数，而无需了解列中其他元素的情况。

以下是对包含评论的DataFrame文本进行情感分析的示例：

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

在这个示例中，我们使用`map_elements`函数将`analyze_sentiment`函数应用于“reviews”列中的每个元素。结果值的范围从-1.0（非常消极）到1.0（非常积极），0.0表示中性。



---

**<font style="color:#01B2BC;">关于低效映射的警告</font>**

当你在Polars中使用Python函数时，需要了解其速度不会像Polars的原生函数那样快。Polars通常在Rust中运行其操作，但当必须应用自定义Python函数时，会出现以下几种情况：

+ 函数执行较慢的Python字节码，而不是较快的Rust字节码。
+ Python函数受到全局解释器锁（GIL）的限制，意味着它无法并行运行。这对于速度尤其有害，尤其是在`group_by`操作中，聚合函数通常会为每个分组并行调用。

将Python的lambda表达式或自定义函数映射到Polars数据时，应作为最后的手段。当Polars发出`PolarsInefficientMapWarning`警告时，说明可能存在使用原生Polars表达式的替代方法。只有在查阅了Polars文档后，确认没有原生表达式或表达式组合可以满足需求时，才应考虑使用Python函数。

在下面的示例中，通过将一个简单函数映射到一列，可以看到`PolarsInefficientMapWarning`的出现。

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

这些建议通常是找到更高效实现逻辑的良好起点！

---

`@lru_cache`装饰器

Python中`functools`模块的`@lru_cache`装饰器是一个优化计算密集型函数的实用工具。通过缓存函数调用的结果，它可以显著减少执行时间，特别是在函数被反复用相同参数调用的情况下。这在将函数映射到包含重复值的DataFrame列时尤其有用。`@lru_cache`会存储函数调用的结果，当函数再次用相同参数调用时，它会从缓存中检索结果，而不是重新计算。

可以为`@lru_cache`装饰器提供一个`maxsize`参数，用于确定缓存结果的数量。默认情况下，这个值设置为128个缓存条目，但你可以根据数据的大小设置更高的值以防止缓存未命中。当缓存满时，`@lru_cache`会丢弃最少最近使用的条目。如果希望存储所有结果，可以将`maxsize`设置为`None`，但这会导致内存占用较高。当不再需要缓存时，可以使用`cache_clear()`方法清除缓存。让我们将这个装饰器应用于之前的`map_elements`中使用的余弦相似度函数：

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

`map_batches`函数允许你对一个Series或一组Series应用Python函数。这在需要了解列中其他元素的信息时，或者需要同时对多个列应用函数时非常有用。`map_batches`具有以下参数：

+ `function`：要应用于Series的函数。
+ `return_dtype`：函数返回的Series的数据类型。
+ `is_elementwise`：函数是否是逐元素的。如果是，可以在流处理引擎中运行，但可能会返回不正确的`group_by`结果。
+ `agg_list`：在`group-by`上下文中，将表达式的值聚合成一个列表，然后再应用函数。函数将只对一组列表调用一次，而不是对每个分组分别调用。

下面的示例演示了如何使用`map_batches`函数对“feature1”和“feature2”列应用softmax归一化函数。softmax归一化函数将一组数字转换为加起来等于100%的概率。

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

最后，`map_groups`函数允许你在`GroupBy`上下文中对每个分组应用Python函数。



假设你有一个包含不同地点温度数据的DataFrame，其中美国的温度是华氏度，而欧洲的温度是摄氏度。如果在你的分析中，只有温度的变化才是相关的，那么可以在每个分组内对特征进行缩放，以使它们具有可比性：

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

最后，如果你需要对`GroupBy`上下文中的各个分组进行细粒度的控制，也可以对这些分组进行迭代。这在需要对每个分组应用不同的自定义函数，或逐个检查分组时非常有用。迭代分组时，会返回一个包含分组标识符（如果只有一个标识符，则为单个标识符）和该分组对应的DataFrame的元组。

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

总之，Polars提供了`map_elements`、`map_batches`和`map_groups`等函数，可以将自定义的Python函数应用于数据。虽然这些用户自定义函数可以进行广泛的定制，但与原生Polars表达式相比，可能会存在性能缺陷。如果仍然需要使用Python函数，但输入经常相同，可以使用`@lru_cache`装饰器来优化重复计算。通过理解和利用这些工具，可以根据具体需求定制数据转换，同时保持最佳性能。

<h2 id="R2Aqh">注册自定义命名空间</h2>
Polars已经有许多内置的命名空间，例如`pl.col(…).str.…()`中的字符串表达式命名空间，但你也可以注册自己的自定义命名空间。这样可以将自定义函数和表达式注册到新的命名空间，从而为用户创建更直观和用户友好的API。

要创建自定义命名空间，需要创建一个用装饰器装饰的Python类，装饰器的级别对应于要注册的命名空间级别。这些级别包括：

| 上下文 | 装饰器 | 使用示例 |
| --- | --- | --- |
| Expression | `**<font style="color:#E746A4;">@pl.api.register_expr_namespace("…")</font>**` | `pl.col(…).<namespace>.<function>` |
| DataFrame | `**<font style="color:#E746A4;">@pl.api.register_dataframe_namespace("…")</font>**` | `df.<namespace>.<function>` |
| LazyFrame | `**<font style="color:#E746A4;">@pl.api.register_lazy_frame_namespace("…")</font>**` | `lf.<namespace>.<function>` |
| Series | `**<font style="color:#E746A4;">@pl.api.register_series_namespace("…")</font>**` | `col.<namespace>.<function>` |


注册自定义命名空间的方法在所有级别都是相同的。创建一个用相应装饰器装饰的类，并在该类中定义要注册的函数。例如，下面的代码片段展示了如何为`pl.col(…).celsius.…()`表达式注册一个名为`celsius`的自定义命名空间：

```python
@pl.api.register_expr_namespace("celsius")  ①
class Celsius:
    def __init__(self, expr: pl.Expr):  ②
        self._expr = expr

def to_fahrenheit(self) -> pl.Expr:  ③
    return (self._expr * 9 / 5) + 32

def to_kelvin(self) -> pl.Expr:
    return self._expr + 273.15
```

1. 这一行代码将自定义的表达式命名空间注册为`celsius`。
2. 类的构造函数接受一个表达式作为输入。这使得你可以在自定义函数中使用传递给自定义命名空间的表达式。
3. `to_fahrenheit`函数接收传递给自定义命名空间的表达式，并返回一个将温度转换为华氏度的新表达式。

然后，你可以像这样在代码中使用这个自定义命名空间：

```python
import polars as pl

df = pl.DataFrame({
    "celsius": [0, 10, 20, 30, 40]
})

df.with_columns(pl.col("celsius").celsius.to_fahrenheit().alias("fahrenheit"))
```

```plain
shape: (5, 2)
┌─────────┬────────────┐
│ celsius │ fahrenheit │
│ ---     │ ---        │
│ i64     │ f64        │
╞═════════╪════════════╡
│ 0       │ 32.0       │
│ 10      │ 50.0       │
│ 20      │ 68.0       │
│ 30      │ 86.0       │
│ 40      │ 104.0      │
└─────────┴────────────┘
```

这个代码片段创建了一个包含`celsius`列的DataFrame，并使用自定义命名空间`celsius`将该列中的值转换为华氏度。

---

**<font style="color:#C99103;">自定义命名空间不支持自动补全和类型提示</font>**

由于自定义命名空间不是Polars库的一部分，而是在运行时创建的，它们无法被类型提示和自动补全识别。这意味着在使用自定义命名空间时，笔记本和IDE中不会显示任何建议或类型提示。

---

<h2 id="prYr6">Rust中的Polars插件</h2>
如果你想让Polars的性能达到极致，可以用Rust编写自定义插件。这允许Polars引擎在运行时动态链接你的自定义函数和表达式。由于插件可以利用Rust带来的优化、并行性和性能，其运行速度几乎与Polars的原生函数和表达式一样快！因此，这是我们扩展Polars新功能的首选方式。

为了探索这种方法的工作原理，我们将创建一个基本的自定义函数，将Series中的所有值替换为“Hello, world!”。

<h3 id="A6sdf">前提条件</h3>
由于你需要用Rust编写表达式，首先需要安装Rust。可以按照Rust官方网站上的说明进行安装。安装完成后，可以运行以下命令来检查是否安装成功：

```bash
$ rustc --version
```

如果你看到一个版本号，就说明安装成功了！

<h3 id="WtSUP">插件项目的结构</h3>
一个自定义表达式由一个包含一些关键文件的独立项目文件夹组成：

```plain
/
├── src
│   ├── expression.rs
│   └── lib.rs
├── plugin_name
│   └── __init__.py
├── Cargo.toml
```

+ `src/expression.rs`：包含自定义表达式的Rust代码。
+ `src/lib.rs`：定义自定义表达式为库的Rust代码。
+ `plugin_name/__init__.py`：包含将函数注册到Polars引擎的Python代码。
+ `Cargo.toml`：包含Rust项目的元数据，例如名称、版本和依赖项。

<h3 id="EU8gZ">插件示例</h3>
让我们逐步了解这些文件的内容，以实现一个将所有值替换为“Hello, world!”的函数。第一个文件`src/expression.rs`包含自定义表达式的Rust代码：

```rust
src/expression.rs
```

```rust
use polars::prelude::*;  1
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=String)] 2
fn hello_world(inputs: &[Series]) -> PolarsResult<Series> {  3
    /// This function takes a Series as input, and returns a new Series of the
    /// same length with all values set as "Hello, world!"
    let length = inputs[0].len();  4
    let result: Vec<String> = vec!["Hello, world!".to_string(); length];  5
    Ok(Series::new("hello_world", result))  6
}
```

1. 这些行导入了Polars和PyO3 Polars的Rust库。
2. 这行代码类似于Python的装饰器，允许将Rust函数注册为返回字符串数据类型的Polars函数，使Polars引擎可以针对这个输出进行优化。
3. 该函数的输入是一个Series的切片，返回的是一个`PolarsResult`类型的Series。`PolarsResult`是一种可以包含值或错误的类型。
4. 这行代码获取传入Series的长度。
5. 这行代码创建一个具有相同长度的向量，其中每个元素都设置为“Hello, world!”。
6. 这行代码返回一个新Series，名称为“hello_world”，内容为该字符串向量。



接下来是定义自定义表达式为库的文件：

`_<font style="color:rgb(61, 59, 73);background-color:rgb(238, 242, 246);">src/lib.rs</font>_`

```rust
mod expressions;  /1
```

1. 这行代码告诉Rust在模块中包含`expressions.rs`文件。



`_<font style="background-color:rgb(238, 242, 246) !important;">plugin_name/__init__.py</font>_`

```python
from pathlib import Path

import polars as pl
from polars.plugins import register_plugin_function
from polars.type_aliases import IntoExpr


def hello_world(expr: IntoExpr) -> pl.Expr:  ①
    return register_plugin_function(  ②
        plugin_path=Path(__file__).parent,  ③
        function_name="hello_world",  ④
        args=expr,  ⑤
        is_elementwise=True, ⑥
    )
```

1. 接收一个`IntoExpr`类型的变量，该变量传递给Rust插件，并返回注册到Polars的表达式。`IntoExpr`表示一种类型，可以是或可以转换为Polars表达式。例如，一个字符串值可以转换为`pl.col("column_name")`表达式。
2. 将插件注册为Polars的一个函数。
3. 指定Rust插件所在的文件路径。
4. 要注册的Rust函数的名称。
5. 将表达式传递给Rust插件，并且可以向插件传递多个参数。
6. 指定函数是逐元素操作的，即对Series中的每个元素独立操作。这一信息使Polars能够触发快速路径算法和优化。

最后是包含Rust项目元数据的文件：

`_<font style="background-color:rgb(238, 242, 246) !important;">Cargo.toml</font>_`

```rust
[package]
name = "hello_world_plugin"
version = "0.1.0"
edition = "2021"

[lib]
name = "hello_world_func"
crate-type = ["cdylib"]

[dependencies]
polars = { version = "*" }
pyo3 = { version = "*", features = ["extension-module"] }
pyo3-polars = { version = "*", features = ["derive"] }
serde = { version = "*", features = ["derive"] }
```

这个文件包含了Rust项目的元数据。它指定了项目的名称、版本、版本控制（edition）以及依赖项，同时还定义了库的名称和类型。[lib]部分的`name`字段应与Rust项目文件夹的名称匹配（在本例中为`hello_world_func`）。

<h3 id="wIUzN">编译插件</h3>
为了在Python代码中使用这个基本插件，可以通过运行以下命令来编译Rust代码：

```bash
$ maturin develop --release
```



---

`**<font style="color:#01B2BC;">--release</font>**`**<font style="color:#01B2BC;">标志</font>**

`--release`标志会优化编译后的代码以提高性能，但会增加编译时间。如果要对插件进行性能测试，始终应该使用`--release`标志。而在开发过程中，如果只是快速测试代码，可以省略该标志。

---

<h3 id="TFBaP">性能基准测试</h3>
现在代码已经编译完成，可以通过运行以下Python代码粗略地对自定义表达式的性能进行基准测试，以了解其带来的性能差异：

```python
import polars as pl
from hello_world_func import hello_world  ①
import time

df = pl.DataFrame(
    {
        "a": ["1", "2", "3", "4"] * 100_000,
    }
)

times = []
for i in range(10):
    t0 = time.time()
    out = df.with_columns(pl.col("a").str.replace_all(r".*", "Hello, world!"))
    t1 = time.time()
    times.append(t1 - t0)
print("Polars native string replace:        ", sum(times) / len(times))


times = []
for i in range(10):
    t0 = time.time()
    out = df.with_columns(hello_world("a"))  ②
    t1 = time.time()
    times.append(t1 - t0)
print("Our custom made Hello world replace: ", sum(times) / len(times))
```

```plain
Polars native string replace:         0.13438236713409424
Our custom made Hello world replace:  0.0869610071182251
```

1. 这一行导入了自定义函数。导入函数的模块名称应与Rust `[lib]` 的名称相同。
2. 这一行使用自定义函数将“a”列中的所有值替换为“Hello, world!”。

正如你所见，自定义表达式比原生Polars表达式快35%！

<h3 id="qfP9Q">注册参数</h3>
现在你已经基本了解了如何创建自定义表达式，接下来让我们探讨不同的使用方法。

<h3 id="lu5CB">使用多个`args`和`kwargs`作为输入</h3>
可以将多个参数传递给Rust插件，以创建同时操作多个列的自定义函数。可以通过在`register_plugin_function`函数的`args`参数中传递一个列表来实现这一点。

```python
def args_func(arg1: IntoExpr, arg2: IntoExpr) -> pl.Expr:
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="args_func",
        args=[arg1, arg2],
    )
```

此外，你还可以通过在`register_plugin_function`函数的`kwargs`参数中传递一个字典，将关键字参数传递给Rust插件，如下所示：

```python
def kwargs_func(
    expr: IntoExpr,
    float_arg: float,
    integer_arg: int,
    string_arg: str,
    boolean_arg: bool,
) -> pl.Expr:
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="kwargs_func",
        args=expr,
        kwargs={
            "float_arg": float_arg,
            "integer_arg": integer_arg,
            "string_arg": string_arg,
            "boolean_arg": boolean_arg,
        },
    )
```

确保在Rust端标明预期的参数。在那里，你需要定义一个包含预期参数的Rust结构体（struct），并使用来自`serde`库的`Deserialize`特性为其添加Rust装饰器`derive`。`crate`是Rust中的一个包，类似于Python中的包或其他语言中的库。

```rust
/// Provide your own kwargs struct with the proper schema and accept that type
/// in your plugin expression.
#[derive(Deserialize)]
pub struct MyKwargs {
    float_arg: f64,
    integer_arg: i64,
    string_arg: String,
    boolean_arg: bool,
}

/// If you want to accept `kwargs`, define a `kwargs` argument
/// on the second position in you plugin. You can provide any custom struct that
/// is deserializable with the pickle protocol (on the Rust side). 1
#[polars_expr(output_type=String)]
fn append_kwargs(input: &[Series], kwargs: MyKwargs) -> PolarsResult<Series> {
    let input = &input[0];
    let input = input.cast(&DataType::String)?;
    let ca = input.str().unwrap();

    Ok(ca
       .apply_to_buffer(|val, buf| {
           write!(
               buf,
               "{}-{}-{}-{}-{}",
               val,
               kwargs.float_arg,
               kwargs.integer_arg,
               kwargs.string_arg,
               kwargs.boolean_arg
           )
           .unwrap()
       })
       .into_series())
}
```

1. `pickle`协议描述了用于序列化和反序列化Python对象结构的二进制协议。这意味着Rust端可以对Python对象结构进行反序列化，并在Rust代码中使用它。反序列化是指将序列化的数据转换回可用的格式。

****

**其他注册参数**

你可以提供额外的信息，以便Polars对插件的运行方式进行优化。例如：

+ `is_elementwise`  
自定义表达式的最基本用法是对Series逐元素应用函数。这允许高度的并行性和快速路径算法，是使用自定义表达式的最有效方式。在Python代码中使用`is_elementwise=True`标志来指定函数是逐元素的。

---

**<font style="color:#C99103;">警告</font>**  
如果`is_elementwise`标志使用不正确，Polars不会抛出错误，但函数行为将不可预测。这在`group-by`和窗口操作时尤为明显。如果函数不是逐元素的，但被标记为逐元素，函数可能会忽略分组和窗口操作并返回错误结果。反之，如果函数是逐元素的但未标记为逐元素，结果也会不正确。

---

+ `changes_length`  
表示插件是否更改Series的长度。例如，在应用`unique`、`filter`和`explode`等操作时，结果的长度与输入不同。如果函数更改Series的长度，应设置`changes_length=True`，默认情况下为`False`。
+ `returns_scalar`  
如果函数返回包含单个元素的列表，设置`returns_scalar=True`将展开列表并返回单个元素。例如，如果有一个求和函数对列表`[1,2,3]`求和，通常会返回`[6]`，但设置`returns_scalar=True`后会直接返回`6`。
+ `cast_to_supertype`  
如果插件接受多种类型的输入参数，可以设置`cast_to_supertype=True`以将所有输入转换为相同的类型。这有助于防止类型错误，并确保函数正确运行。例如，如果函数接受`Int64`和`Float64`作为输入，设置`cast_to_supertype=True`会在运行函数之前将`Int64`转换为`Float64`。
+ `input_wildcard_expansion`  
一些表达式代表多个列（例如`pl.col(*)`）。如果设置`input_wildcard_expansion=True`，表达式将展开为输入列的列表。
+ `pass_name_to_apply`  
如果设置为`True`，则在`group-by`操作中传递给函数的Series会保留其名称。如果你的自定义插件需要列名，可以设置`pass_name_to_apply=True`。默认情况下此选项关闭，因为每个分组都需要额外的堆分配。

<h3 id="ZMcZr">使用Rust Crate</h3>
Rust插件的一大优势是可以在自定义表达式中使用任何Rust crate。这允许你在自定义表达式中使用任何与Polars引擎兼容的Rust库。以下示例展示了如何创建`point`命名空间，以计算一个点是否位于由一组点构成的多边形内。

<h3 id="K5gGf">用例：地理空间分析</h3>
综合以上知识，你将创建自定义的`point`命名空间，用于使用Rust插件计算一个点是否位于多边形内。这是地理空间分析中的常见用例，展示了如何在自定义表达式中使用`geo` Rust crate。首先，将`geo` crate添加到`Cargo.toml`文件中。然后，编写Rust代码来计算一个点是否位于多边形内。接下来，可以将此代码供Python使用，并创建一个自定义命名空间来实现此功能。

<h4 id="ELhMl">添加`geo` Crate</h4>
要将`geo` crate添加到Rust项目中，将其添加到`Cargo.toml`文件的`[dependencies]`部分：

```toml
[package]
name = "coordinates_plugin"
version = "0.1.0"
edition = "2021"

[lib]
name = "coordinates_plugin_py"
crate-type = ["cdylib"]

[dependencies]
polars = { version = "*" }
pyo3-polars = { version = "*", features = ["derive"] }
geo = "0.28.0"
```

在这里，你创建了一个名为`coordinates_plugin`的新包，并将`geo` crate添加到依赖项中。它指向`coordinates_plugin_py`库，并指定这是一个`cdylib`类型的库。由于已经将`geo` crate添加到依赖项中，在构建代码时将自动下载和编译。现在，你可以在Rust代码中使用这个新增的crate。

<h4 id="nxxh6">Rust代码：`expressions.rs`</h4>
Rust代码需要完成以下几个步骤：接受输入并将其解析为一个点和一个多边形，检查该点是否位于多边形内，并返回一个布尔值。接下来我们将逐步讲解这个过程，如果你之前没有使用过Rust，代码可能看起来有些复杂，但别担心，我们会一步一步带你了解。首先，来看一下`expressions.rs`文件的内容：

```rust
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use geo::{Contains, Point, Polygon, coord};
use polars::series::amortized_iter::AmortSeries;


fn extract_point(point_opt: Option<AmortSeries>) -> Option<Point> {
    point_opt.and_then(|point| {
        let ca = point.as_ref().f64().ok()?;
        Some(Point::new(ca.get(0)?, ca.get(1)?))
    })
}

fn extract_polygon(polygon_opt: Option<AmortSeries>) -> Option<Polygon> {
    polygon_opt.and_then(|polygon| {
        let lst = polygon.as_ref().list().ok()?;
        let coords: Option<Vec<_>> = lst
            .amortized_iter()
            .map(|coord| {
                let coord_binding = coord?;
                let ca = coord_binding.as_ref().f64().ok()?;
                Some(coord! { x: ca.get(0)?, y: ca.get(1)? })
            })
            .collect();
        coords.map(|c| Polygon::new(c.into(), vec![]))
    })
}

fn point_in_polygon_check(point_opt: Option<AmortSeries>, polygon_opt: Option<AmortSeries>) -> Option<bool> {
    let point = extract_point(point_opt);
    let polygon = extract_polygon(polygon_opt);
    match (point, polygon) {
        (Some(p), Some(poly)) => Some(poly.contains(&p)),
        _ => None, // Return None if point or polygon extraction fails
    }
}

#[polars_expr(output_type=Boolean)]
fn point_in_polygon(inputs: &[Series]) -> PolarsResult<Series> {
    let point_series = inputs[0].list()?;
    let polygon_series = inputs[1].list()?;

    let out: BooleanChunked = point_series
        .amortized_iter()
        .zip(polygon_series.amortized_iter())
        .map(|(point_opt, polygon_opt)| match (point_opt, polygon_opt) {
            (Some(point), Some(polygon)) => point_in_polygon_check(Some(point), Some(polygon)),
            _ => None,
        })
        .collect();

    Ok(out.into_series())
}
```

在这里，`point_in_polygon`函数可作为Polars的插件使用。该函数使用了三个辅助函数来从输入中提取值，然后使用`geo`库的`contains`函数检查点是否位于多边形内。



接下来，我们将逐步讲解这个过程：

```rust
use polars::prelude::*; 1
use pyo3_polars::derive::polars_expr; 2
use geo::{Contains, Point, Polygon, coord}; 3
use polars::series::amortized_iter::AmortSeries; 4
```

这些是导入语句（在Rust中称为`use`声明）。

1. 导入Polars库中的常用项，如`Series`和`PolarsResult`。
2. 从PyO3 Polars库导入`polars_expr`宏，用于定义带有Python绑定的自定义表达式。
3. 从`geo` crate中导入必要的组件，以计算一个点是否位于多边形内。
4. 从Polars库中导入`AmortSeries`结构体，以更高效地遍历`Series`。

接下来的代码片段定义了用于从`Series`中提取点的辅助函数：

```rust
fn extract_point(point_opt: Option<AmortSeries>) -> Option<Point> { 
    point_opt.and_then(|point| { 
        let ca = point.as_ref().f64().ok()?; 
        Some(Point::new(ca.get(0)?, ca.get(1)?)) 
    })
}
```

1. 该函数接受一个可能包含点的`Series`作为输入，并将其返回为`geo` crate中定义的`Point`结构体。
2. 该函数使用`and_then`方法解包`Option`并对值应用一个闭包。闭包是可以捕获其定义环境中的变量的函数，可以将其看作Python中的lambda函数。`Option`是一种类型，可以有一个值，也可以没有（即为`None`）。
3. 该函数尝试将`Series`转换为`f64`类型，并返回`f64`类型的`Option`。如果转换失败，则返回`None`。
4. 如果能够成功检索到`Series`中的x和y坐标，则该函数使用这些坐标创建一个新的`Point`结构体。

接下来的代码片段定义了用于从`Series`中提取多边形的辅助函数：

```rust
fn extract_polygon(polygon_opt: Option<AmortSeries>) -> Option<Polygon> {
    polygon_opt.and_then(|polygon| {
        let lst = polygon.as_ref().list().ok()?; 1
        let coords: Option<Vec<_>> = lst
            .amortized_iter() 2
            .map(|coord| {
                let coord_binding = coord?;
                let ca = coord_binding.as_ref().f64().ok()?; 3
                Some(coord! { x: ca.get(0)?, y: ca.get(1)? })
            })
            .collect();
        coords.map(|c| Polygon::new(c.into(), vec![])) 4
    })
}
```

1. 该函数不是将输入解析为浮点数，而是将其解析为一个列表，因为输入是一个坐标列表（`list[list[f64]]`）。
2. 使用`amortized_iter`方法高效地遍历列表。
3. 读取坐标并将其转换为`Float64`类型的`Series`。
4. 当所有坐标都成功解析后，使用这些坐标创建一个新的`Polygon`结构体。

现在，你已经可以创建点和多边形，接下来要检查该点是否位于多边形内。这可以通过以下辅助函数来完成：

```rust
fn point_in_polygon_check(point_opt: Option<AmortSeries>, polygon_opt: Option<AmortSeries>) -> Option<bool> {
    let point = extract_point(point_opt); 1
    let polygon = extract_polygon(polygon_opt); 2
    match (point, polygon) { 3
        (Some(p), Some(poly)) => Some(poly.contains(&p)), 4
        _ => None, 5
    }
}
```

1. 从输入中提取点。
2. 从输入中提取多边形。
3. `match`语句是一种控制流结构，用于将一个值与一系列模式进行比较，并根据匹配的模式执行相应的代码。在此情况下，它检查点和多边形是否都成功提取。
4. 如果提取成功，则返回该点是否位于多边形内的结果。
5. 如果提取失败，则返回`None`。

最后一步是创建使用这些辅助函数的自定义表达式，以检查一个点是否位于多边形内：

```rust
#[polars_expr(output_type=Boolean)] 1
fn point_in_polygon(inputs: &[Series]) -> PolarsResult<Series> { 2
    let point_series = inputs[0].list()?; 3
    let polygon_series = inputs[1].list()?;

    let out: BooleanChunked = point_series 4
        .amortized_iter() 5
        .zip(polygon_series.amortized_iter()) 6
        .map(|(point_opt, polygon_opt)| match (point_opt, polygon_opt) {
            (Some(point), Some(polygon)) => point_in_polygon_check(Some(point), Some(polygon)), 7
            _ => None, 8
        })
        .collect();

    Ok(out.into_series()) 9
}
```

1. 使用`polars_expr`宏定义带有Python绑定的自定义表达式，并指定表达式的输出类型为布尔值。
2. 该函数接受一个`Series`切片作为输入，返回一个`PolarsResult`类型的`Series`。
3. 尝试将输入转换为列表。
4. 创建一个新的`BooleanChunked`来存储结果。
5. 高效地遍历点的`Series`。
6. 将点和多边形的`Series`配对，以便对每对点和多边形应用`point_in_polygon_check`函数。
7. 对成功提取的每对点和多边形应用`point_in_polygon_check`函数。
8. 如果点或多边形提取失败，则返回`None`。
9. 使用`Ok`类型返回结果，表示操作成功。

现在，这段Rust代码已准备好编译并在Python中使用。

在Rust项目文件夹中运行`maturin develop`命令，代码将被编译并供Python使用。如果想优化性能，可以运行`maturin develop --release`来启用所有优化，但会增加编译时间。

<h4 id="TI1zR">Python代码：`__init__.py`</h4>
现在Rust代码已经准备好，你可以创建一个Python文件，将自定义表达式注册到Polars中。为此，需要在`coordinates_plugin_py`文件夹中放置一个名为`__init__.py`的文件。

该文件用于在Python中注册自定义表达式，使其可以通过Polars引擎调用。

```python
from pathlib import Path

import polars as pl
from polars.plugins import register_plugin_function
from polars.type_aliases import IntoExpr


def point_in_polygon(point: IntoExpr, polygon: IntoExpr) -> pl.Expr:
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        args=[point, polygon],
        function_name="point_in_polygon",
        is_elementwise=True,
    )
```

正如之前的“hello world”示例一样，这个文件将自定义表达式注册到Polars中。不同之处在于，这里需要两个参数，因此函数接受两个`IntoExpr`参数。`args`参数是这些参数的列表，它们会被传递给Rust插件。

<h4 id="E7jnX">Python代码：使用自定义命名空间</h4>
现在Rust插件已经可用，你可以将其封装在一个自定义命名空间中。这样可以像Polars内部的组织方式一样，整理和分组相关的函数，例如`pl.col(…).str.<function>`用于字符串操作。在这个例子中，将创建`point`命名空间，其中包含一个使用Rust插件的函数。首先，注册这个自定义命名空间：

```python
import polars as pl
import coordinates_plugin_py as coord

@pl.api.register_expr_namespace("point")  1
class Point:
    def __init__(self, input_expression: pl.Expr):  2
        self._input_expression = input_expression

    def is_in_polygon(self, polygon: list[list[pl.Float64]]) -> pl.Expr:  3
        return coord.point_in_polygon(self._input_expression, polygon)
```

1. 将类注册为命名空间，使用关键字`point`。
2. `__init__`方法存储表达式将接收的输入。在这个例子中，函数将针对包含点的列调用，这意味着将接收这些点作为输入，而表示多边形的表达式将成为后续函数的输入。
3. `point`命名空间中的这个函数将调用Rust代码，以检查点是否位于由函数输入构造的多边形内。

通过一个小的测试脚本，可以检查插件是否按预期工作。可以从一个包含以下几种情况的DataFrame开始：一个应该为`True`的情况，一个应该为`False`的情况，以及一个无效的情况（应为`null`）。

```python
# Create a sample DataFrame
df = pl.DataFrame(
    {
        "point": [[5.0, 5.0], [20.0, 20.0], [20.0, 20.0]],
        "polygon": [
            [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0]],
            [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], ],
            [[0.0, None], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]],
        ],
    }
)
```

现在，剩下的唯一一步就是在你的命名空间中调用这个函数！敲响战鼓……

如果一切正常，函数调用应该会输出预期的结果。这就是如何在不影响性能的情况下，为Polars扩展特殊功能的方法。这可能是一段冒险的旅程，但绝对值得尝试！

如果你想更深入地了解如何创建这种自定义信息，我们推荐Marco Gorelli关于插件的教程。

---

**<font style="color:#01B2BC;">性能技巧</font>**

`apply_to_buffer`是一个辅助方法，用于创建一个可重用的缓冲区，用来写入字符串，从而减少堆分配并提高性能。

---

<h2 id="LfHlt">本章小结</h2>
在本章中，你学到了如何：

+ 通过`map_elements`、`map_batches`和`map_groups`将自定义Python函数应用到数据上。
+ 在Python中注册自定义命名空间。
+ 创建Rust插件并使其在Python中可用。
+ 在自定义插件中使用Rust的`crate`。
+ 为性能优化自定义插件。

下一章将深入探讨Polars的内部结构，并展示如何避免编写会降低性能的代码。

