

上一章我们重点讨论了聚合数据以创建信息总结。然而，如果数据的形状不适合执行这些聚合操作，你该怎么办呢？在本章中，我们将学习如何重塑数据，使其更适合分析。

重塑数据是数据分析过程中至关重要的一步。它涉及更改数据的维度，以使其更适合分析、提高计算性能或为可视化做准备。Polars 提供了多种函数来按需精确重塑数据，使用的函数包括`pivot`、`melt`、`transpose`、`explode`和`partition_by`。

<h2 id="ePtL0">宽格式与长格式数据框</h2>
曾有一位智者名叫哈德利·威克姆（Hadley Wickham），他是一名统计学家，因其对R编程语言及“tidyverse”的工作而闻名，“tidyverse”是R中一个用于处理和可视化数据的流行套件。与此相关的是，他在2014年撰写了一篇名为《Tidy Data》的论文，在其中他引入了宽格式（wide）和长格式（long）DataFrame的概念。这些概念至今仍被数据科学界广泛用于描述DataFrame的形状，我们在本章也将使用这些概念。

威克姆指出，DataFrame 可以以两种极端形式表示：宽格式和长格式。宽格式DataFrame有很多列但只有少量行。每行包含一个标识符的列，数据分布在多个列中。这种格式通常用于每个观察值有多个测量值的情况。宽格式数据的一个示例如下：

```python
import polars as pl

df = pl.DataFrame({
    "student": ["Alice", "Bob", "Charlie"],
    "math": [85, 78, 92],
    "science": [90, 82, 85],
    "history": [88, 80, 87]
})
df
```

```plain
shape: (3, 4)
┌─────────┬──────┬─────────┬─────────┐
│ student │ math │ science │ history │
│ ---     │ ---  │ ---     │ ---     │
│ str     │ i64  │ i64     │ i64     │
╞═════════╪══════╪═════════╪═════════╡
│ Alice   │ 85   │ 90      │ 88      │
│ Bob     │ 78   │ 82      │ 80      │
│ Charlie │ 92   │ 85      │ 87      │
└─────────┴──────┴─────────┴─────────┘
```



这个DataFrame有三列，以受试者的名字作为列名。

而宽格式DataFrame有很多列，长格式DataFrame则只有少量列但有很多行。长格式DataFrame每行只有一个变量及其对应的值。上一个示例的长格式如下所示：

```python
df = pl.DataFrame({
    "student": ["Alice", "Alice", "Alice", "Bob", "Bob", "Bob", "Charlie",
                "Charlie", "Charlie"],
    "subject": ["Math", "Science", "History", "Math", "Science", "History",
                "Math", "Science", "History"],
    "grade": [85, 90, 88, 78, 82, 80, 92, 85, 87]
})
df
```

```plain
shape: (9, 3)
┌─────────┬─────────┬───────┐
│ student │ subject │ grade │
│ ---     │ ---     │ ---   │
│ str     │ str     │ i64   │
╞═════════╪═════════╪═══════╡
│ Alice   │ Math    │ 85    │
│ Alice   │ Science │ 90    │
│ Alice   │ History │ 88    │
│ Bob     │ Math    │ 78    │
│ Bob     │ Science │ 82    │
│ Bob     │ History │ 80    │
│ Charlie │ Math    │ 92    │
│ Charlie │ Science │ 85    │
│ Charlie │ History │ 87    │
└─────────┴─────────┴───────┘
```



这种格式化的DataFrame每行只包含一个观测值。

所使用的格式对内存使用和计算性能的影响是显著的。由于Polars使用列存储格式，长格式DataFrame在内存使用和计算性能方面往往更有效。

<h2 id="MMJKI">转换为宽格式数据框</h2>
如果你想从长格式转换为宽格式DataFrame，可以使用`pivot`函数。`pivot`函数接受以下参数：

+ `**index**`: 用作行标识符的列。
+ `**columns**`: 包含要作为列名的列。
+ `**values**`: 包含将出现在单元格中的值的列。
+ `**aggregate_function**`: 如果单个单元格中有多个值，用于聚合这些值的函数。如果为空，则当单元格有多个值时会抛出错误。
+ `**maintain_order**`: 对分组键排序以确保结果可预测。
+ `**sort_columns**`: 按列名对转置后的值和列进行排序。默认按DataFrame中的出现顺序排序。
+ `**separator**`: 生成的列名中使用的分隔符字符串。

我们通过一个示例来解释。你需要存储一组学生的成绩。随着成绩一个接一个地传来，你将它们存储在一个长格式DataFrame中：

```python
import polars as pl

df = pl.DataFrame({
    "student": ["Alice", "Alice", "Alice", "Bob", "Bob", "Bob", "Charlie",
                "Charlie", "Charlie"],
    "subject": ["Math", "Science", "History", "Math", "Science", "History",
                "Math", "Science", "History"],
    "grade": [85, 90, 88, 78, 82, 80, 92, 85, 87]
})

df
```

```plain
shape: (9, 3)
┌─────────┬─────────┬───────┐
│ student │ subject │ grade │
│ ---     │ ---     │ ---   │
│ str     │ str     │ i64   │
╞═════════╪═════════╪═══════╡
│ Alice   │ Math    │ 85    │
│ Alice   │ Science │ 90    │
│ Alice   │ History │ 88    │
│ Bob     │ Math    │ 78    │
│ Bob     │ Science │ 82    │
│ Bob     │ History │ 80    │
│ Charlie │ Math    │ 92    │
│ Charlie │ Science │ 85    │
│ Charlie │ History │ 87    │
└─────────┴─────────┴───────┘
```



现在到了给学生发成绩单的时候，你想为每个学生创建一行。你可以通过在`subject`列上进行透视来实现：

```python
df.pivot(index="student", columns="subject", values="grade")
```

```plain
shape: (3, 4)
┌─────────┬──────┬─────────┬─────────┐
│ student │ Math │ Science │ History │
│ ---     │ ---  │ ---     │ ---     │
│ str     │ i64  │ i64     │ i64     │
╞═════════╪══════╪═════════╪═════════╡
│ Alice   │ 85   │ 90      │ 88      │
│ Bob     │ 78   │ 82      │ 80      │
│ Charlie │ 92   │ 85      │ 87      │
└─────────┴──────┴─────────┴─────────┘
```

  
你可以看到我们将学生的名字作为索引，科目作为列，从而转为宽格式DataFrame！

实际上，学生不只获得一个成绩，而是多个成绩。这意味着你需要聚合这些成绩！幸运的是，`pivot`函数可以为我们处理这种情况。

默认情况下，`pivot`函数不会进行聚合，如果有多个值则会抛出错误。在我们的示例中，这没有发生，因为所有值都是唯一的。你可以通过传递`aggregate_function`参数及所需的聚合函数来更改这一点。你可以从以下聚合函数中选择：`min`、`max`、`first`、`last`、`sum`、`mean`、`median`和`len`。在我们的示例中，若要计算平均成绩，可以使用`mean`聚合函数。首先我们更新DataFrame，使每个学生有多个成绩：

```python
df = pl.DataFrame({
    "student": ["Alice", "Alice", "Alice", "Alice", "Alice", "Alice",
                "Bob", "Bob", "Bob", "Bob", "Bob", "Bob"],
    "subject": ["Math", "Math", "Math", "Science", "Science", "Science",
                "Math", "Math", "Math", "Science", "Science", "Science"],
    "grade": [85, 88, 85, 60, 66, 63,
              51, 79, 62, 82, 85, 82]
})

df
```

```plain
shape: (12, 3)
┌─────────┬─────────┬───────┐
│ student │ subject │ grade │
│ ---     │ ---     │ ---   │
│ str     │ str     │ i64   │
╞═════════╪═════════╪═══════╡
│ Alice   │ Math    │ 85    │
│ Alice   │ Math    │ 88    │
│ Alice   │ Math    │ 85    │
│ Alice   │ Science │ 60    │
│ Alice   │ Science │ 66    │
│ …       │ …       │ …     │
│ Bob     │ Math    │ 79    │
│ Bob     │ Math    │ 62    │
│ Bob     │ Science │ 82    │
│ Bob     │ Science │ 85    │
│ Bob     │ Science │ 82    │
└─────────┴─────────┴───────┘
```



现在你可以透视DataFrame来计算每个学生的平均成绩：

```python
df.pivot(
    index="student",
    columns="subject",
    values="grade",
    aggregate_function="mean"
)
```

```plain
shape: (2, 3)
┌─────────┬──────┬─────────┐
│ student │ Math │ Science │
│ ---     │ ---  │ ---     │
│ str     │ f64  │ f64     │
╞═════════╪══════╪═════════╡
│ Alice   │ 86.0 │ 63.0    │
│ Bob     │ 64.0 │ 83.0    │
└─────────┴──────┴─────────┘
```



除了这组标准的聚合函数，你还可以通过表达式传递一个自定义聚合函数。通过创建一个可针对列表元素运行的表达式，比如`pl.col(…).list.eval(<your_expression>)`，你可以利用扩展的灵活性。例如，你可以计算最大值和最小值之间的差异，显示学生成绩的稳定性：

```python
df.pivot(
    index="student",
    columns="subject",
    values="grade",
    aggregate_function=pl.element().max() - pl.element().min()
)
```

```plain
shape: (2, 3)
┌─────────┬──────┬─────────┐
│ student │ Math │ Science │
│ ---     │ ---  │ ---     │
│ str     │ i64  │ i64     │
╞═════════╪══════╪═════════╡
│ Alice   │ 3    │ 6       │
│ Bob     │ 28   │ 3       │
└─────────┴──────┴─────────┘
```



你可以看到Bob在数学中的最大和最小成绩差异比Alice大得多。在我们的用例中，你可以据此联系Bob的导师，确保他一切正常，因为他似乎在一次考试中失误了！

现在这比把沙发搬上楼梯简单多了！



<h2 id="pqH4G">使用`melt`转换为长格式数据框</h2>
如果你想将宽格式转换为长格式DataFrame，或者称为“反透视”，可以使用`melt`函数。`melt`函数接受以下参数：

+ `**id_vars**`：用作行标识符的列。这些列将在结果DataFrame中保持为列。
+ `**value_vars**`：要熔化的列。如果未指定，则使用所有未在`id_vars`中设置的列。
+ `**variable_name**`：包含被熔化列名称的结果列的名称。
+ `**value_name**`：包含被熔化列的值的结果列的名称。

让我们通过一个示例来说明。我们使用上节中成绩单的数据集。每个学生都有一行，包含他们的数学、科学和历史成绩。

```python
df = pl.DataFrame({
    "student": ["Alice", "Bob", "Charlie"],
    "math": [85, 78, 92],
    "science": [90, 82, 85],
    "history": [88, 80, 87]
})
df
```

```plain
shape: (3, 4)
┌─────────┬──────┬─────────┬─────────┐
│ student │ math │ science │ history │
│ ---     │ ---  │ ---     │ ---     │
│ str     │ i64  │ i64     │ i64     │
╞═════════╪══════╪═════════╪═════════╡
│ Alice   │ 85   │ 90      │ 88      │
│ Bob     │ 78   │ 82      │ 80      │
│ Charlie │ 92   │ 85      │ 87      │
└─────────┴──────┴─────────┴─────────┘
```



你可以将这个DataFrame熔化为一个长格式DataFrame，每个学生每个科目占一行：

```python
df.melt(
    id_vars=["student"],
    value_vars=["math", "science", "history"],
    variable_name="subject",
    value_name="grade"
)
```

```plain
shape: (9, 3)
┌─────────┬─────────┬───────┐
│ student │ subject │ grade │
│ ---     │ ---     │ ---   │
│ str     │ str     │ i64   │
╞═════════╪═════════╪═══════╡
│ Alice   │ math    │ 85    │
│ Bob     │ math    │ 78    │
│ Charlie │ math    │ 92    │
│ Alice   │ science │ 90    │
│ Bob     │ science │ 82    │
│ Charlie │ science │ 85    │
│ Alice   │ history │ 88    │
│ Bob     │ history │ 80    │
│ Charlie │ history │ 87    │
└─────────┴─────────┴───────┘
```



在这个例子中，识别结果框架行的方式是通过`student`列，该列包含学生的名字。所有包含将在返回框架中的值的列是包含科目（数学、科学和历史）的列。我们将包含这些科目的列命名为`variable_name`，称为`subject`列，而`value_name`将存储在`grade`列中。

```python
df = pl.DataFrame({
    "student": ["Alice", "Bob", "Charlie", "Alice", "Bob", "Charlie"],
    "class": ["Math101", "Math101", "Math101", "Math102", "Math102", "Math102"],
    "age": [20, 21, 22, 20, 21, 22],
    "semester": ["Fall", "Fall", "Fall", "Spring", "Spring", "Spring"],
    "math": [85, 78, 92, 88, 79, 95],
    "science": [90, 82, 85, 92, 81, 87],
    "history": [88, 80, 87, 85, 82, 89]
})
df
```

```plain
shape: (6, 7)
┌─────────┬─────────┬─────┬──────────┬──────┬─────────┬─────────┐
│ student │ class   │ age │ semester │ math │ science │ history │
│ ---     │ ---     │ --- │ ---      │ ---  │ ---     │ ---     │
│ str     │ str     │ i64 │ str      │ i64  │ i64     │ i64     │
╞═════════╪═════════╪═════╪══════════╪══════╪═════════╪═════════╡
│ Alice   │ Math101 │ 20  │ Fall     │ 85   │ 90      │ 88      │
│ Bob     │ Math101 │ 21  │ Fall     │ 78   │ 82      │ 80      │
│ Charlie │ Math101 │ 22  │ Fall     │ 92   │ 85      │ 87      │
│ Alice   │ Math102 │ 20  │ Spring   │ 88   │ 92      │ 85      │
│ Bob     │ Math102 │ 21  │ Spring   │ 79   │ 81      │ 82      │
│ Charlie │ Math102 │ 22  │ Spring   │ 95   │ 87      │ 89      │
└─────────┴─────────┴─────┴──────────┴──────┴─────────┴─────────┘
```

```python
df.melt(
    id_vars=["student", "class", "age", "semester"],
    value_vars=["math", "science", "history"],
    variable_name="subject",
    value_name="grade"
)
```

```plain
shape: (18, 6)
┌─────────┬─────────┬─────┬──────────┬─────────┬───────┐
│ student │ class   │ age │ semester │ subject │ grade │
│ ---     │ ---     │ --- │ ---      │ ---     │ ---   │
│ str     │ str     │ i64 │ str      │ str     │ i64   │
╞═════════╪═════════╪═════╪══════════╪═════════╪═══════╡
│ Alice   │ Math101 │ 20  │ Fall     │ math    │ 85    │
│ Bob     │ Math101 │ 21  │ Fall     │ math    │ 78    │
│ Charlie │ Math101 │ 22  │ Fall     │ math    │ 92    │
│ Alice   │ Math102 │ 20  │ Spring   │ math    │ 88    │
│ Bob     │ Math102 │ 21  │ Spring   │ math    │ 79    │
│ Charlie │ Math102 │ 22  │ Spring   │ math    │ 95    │
│ Alice   │ Math101 │ 20  │ Fall     │ science │ 90    │
│ Bob     │ Math101 │ 21  │ Fall     │ science │ 82    │
│ Charlie │ Math101 │ 22  │ Fall     │ science │ 85    │
│ Alice   │ Math102 │ 20  │ Spring   │ science │ 92    │
│ Bob     │ Math102 │ 21  │ Spring   │ science │ 81    │
│ Charlie │ Math102 │ 22  │ Spring   │ science │ 87    │
│ Alice   │ Math101 │ 20  │ Fall     │ history │ 88    │
│ Bob     │ Math101 │ 21  │ Fall     │ history │ 80    │
│ Charlie │ Math101 │ 22  │ Fall     │ history │ 87    │
│ Alice   │ Math102 │ 20  │ Spring   │ history │ 85    │
│ Bob     │ Math102 │ 21  │ Spring   │ history │ 82    │
│ Charlie │ Math102 │ 22  │ Spring   │ history │ 89    │
└─────────┴─────────┴─────┴──────────┴─────────┴───────┘
```

<h2 id="bnW4N">转置</h2>
如果你想对所有列进行对角线翻转，而不保留一些列作为标识符，可以使用`transpose`函数。`transpose`函数仅适用于DataFrame，并接受以下参数：

+ `**include_header**`：是否将列名设置为结果DataFrame的第一列。
+ `**header_name**`：如果`include_header`设置为`True`，这将是包含原始列名的列的名称，默认值为`column`。
+ `**column_names**`：你可以传递一个列名列表（或其他可迭代对象）作为结果DataFrame中的列名。

是时候来个例子。让我们使用上节中的DataFrame：

```python
df = pl.DataFrame({
    "student": ["Alice", "Bob", "Charlie"],
    "math": [85, 78, 92],
    "science": [90, 82, 85],
    "history": [88, 80, 87]
})
df
```

```plain
shape: (3, 4)
┌─────────┬──────┬─────────┬─────────┐
│ student │ math │ science │ history │
│ ---     │ ---  │ ---     │ ---     │
│ str     │ i64  │ i64     │ i64     │
╞═════════╪══════╪═════════╪═════════╡
│ Alice   │ 85   │ 90      │ 88      │
│ Bob     │ 78   │ 82      │ 80      │
│ Charlie │ 92   │ 85      │ 87      │
└─────────┴──────┴─────────┴─────────┘
```



现在让我们对这个框架进行对角线翻转：

```python
df.transpose(
    include_header=True,
    header_name="original_headers",
    column_names=(f"report_{count}" for count in range(1, len(df.columns) + 1))
)
```

```plain
shape: (4, 4)
┌──────────────────┬──────────┬──────────┬──────────┐
│ original_headers │ report_1 │ report_2 │ report_3 │
│ ---              │ ---      │ ---      │ ---      │
│ str              │ str      │ str      │ str      │
╞══════════════════╪══════════╪══════════╪══════════╡
│ student          │ Alice    │ Bob      │ Charlie  │
│ math             │ 85       │ 78       │ 92       │
│ science          │ 90       │ 82       │ 85       │
│ history          │ 88       │ 80       │ 87       │
└──────────────────┴──────────┴──────────┴──────────┘
```

所有列现在都变成了行，原始的列名存储在`original_headers`列中！



---

**<font style="color:#01B2BC;">Python中的生成器</font>**

生成器是一种特殊类型的函数，它返回一个可迭代的序列。可以通过多种方式定义生成器，例如使用`yield`关键字的函数或生成器表达式。在上面的示例中，我们使用了生成器表达式来创建一系列字符串。这是通过在括号内使用`for`循环实现的，结果是一个列名列表，Polars 的 `transpose` 可以用它来进行转置。

---

<h2 id="erVEZ">"数据爆炸"(`explode`)</h2>
当你的列中有列表或数组时，它既不是我们之前讨论的宽格式，也不是长格式。如果你想将这些嵌套的值展开为长格式，可以使用`explode`函数。此函数不会“爆炸”，而是安全地为嵌套列中的每个值创建一行，同时复制其他列的值。`explode`的唯一参数是它应该解包为单独行的列。继续使用学生的例子，让我们列出某一科目的分数。

```python
df = pl.DataFrame({
    "student": ["Alice", "Bob", "Charlie"],
    "math": [[85, 90, 88], [78, 82, 80], [92, 85, 87]]
})
df
```

```plain
shape: (3, 2)
┌─────────┬──────────────┐
│ student │ math         │
│ ---     │ ---          │
│ str     │ list[i64]    │
╞═════════╪══════════════╡
│ Alice   │ [85, 90, 88] │
│ Bob     │ [78, 82, 80] │
│ Charlie │ [92, 85, 87] │
└─────────┴──────────────┘
```



为了将这个DataFrame转换为长格式，我们可以对`math`列应用`explode`：

```python
df.explode("math")
```

```plain
shape: (9, 2)
┌─────────┬──────┐
│ student │ math │
│ ---     │ ---  │
│ str     │ i64  │
╞═════════╪══════╡
│ Alice   │ 85   │
│ Alice   │ 90   │
│ Alice   │ 88   │
│ Bob     │ 78   │
│ Bob     │ 82   │
│ Bob     │ 80   │
│ Charlie │ 92   │
│ Charlie │ 85   │
│ Charlie │ 87   │
└─────────┴──────┘
```



如果有多个列，也可以这样做：

```python
df = pl.DataFrame({
    "student": ["Alice", "Bob", "Charlie"],
    "math": [[85, 90, 88], [78, 82, 80], [92, 85, 87]],
    "science": [[85, 90, 88], [78, 82], [92, 85, 87]],
    "history": [[85, 90, 88], [78, 82], [92, 85, 87]],
})
df
```

```plain
shape: (3, 4)
┌─────────┬──────────────┬──────────────┬──────────────┐
│ student │ math         │ science      │ history      │
│ ---     │ ---          │ ---          │ ---          │
│ str     │ list[i64]    │ list[i64]    │ list[i64]    │
╞═════════╪══════════════╪══════════════╪══════════════╡
│ Alice   │ [85, 90, 88] │ [85, 90, 88] │ [85, 90, 88] │
│ Bob     │ [78, 82, 80] │ [78, 82]     │ [78, 82]     │
│ Charlie │ [92, 85, 87] │ [92, 85, 87] │ [92, 85, 87] │
└─────────┴──────────────┴──────────────┴──────────────┘
```



为了将这个数据框转换为长格式，可以对 `math`列使用 `explode`方法：

```python
df.explode("math", "science", "history")
```

```plain
ShapeError: exploded columns must have matching element counts
```

**<font style="color:rgb(61, 59, 73);"></font>**

请注意，上例中列表中的值的顺序很重要！排列整齐的项目会出现在结果中的同一行。我们在第11章讨论过列表的排序问题。此外，`explode`的列必须生成相同数量的结果行，否则会引发`ShapeError`：

```python
df = pl.DataFrame({
    "id": [1,2],
    "value1": [["a", "b"], ["c"]],
    "value2": [["a"], ["b"]],
})
df.explode("value1", "value2")
```

```plain
ShapeError: exploded columns must have matching element counts
```



`explode`甚至可以处理嵌套列表：

```python
df = pl.DataFrame({
    "id": [1,2],
    "nested_value": [["a", "b"], [["c"], ["d", "e"]]],
}, strict=False)
df
```

```plain
shape: (2, 2)
┌─────┬─────────────────────┐
│ id  │ nested_value        │
│ --- │ ---                 │
│ i64 │ list[list[str]]     │
╞═════╪═════════════════════╡
│ 1   │ [["a"], ["b"]]      │
│ 2   │ [["c"], ["d", "e"]] │
└─────┴─────────────────────┘
```



注意，对于嵌套结构，它一次只能展开一层：

```python
df.explode("nested_value")
```

```plain
shape: (4, 2)
┌─────┬──────────────┐
│ id  │ nested_value │
│ --- │ ---          │
│ i64 │ list[str]    │
╞═════╪══════════════╡
│ 1   │ ["a"]        │
│ 1   │ ["b"]        │
│ 2   │ ["c"]        │
│ 2   │ ["d", "e"]   │
└─────┴──────────────┘
```



如果你想获取字符串值，你需要调用它两次：

```python
df.explode("nested_value").explode("nested_value")
```

```plain
shape: (5, 2)
┌─────┬──────────────┐
│ id  │ nested_value │
│ --- │ ---          │
│ i64 │ str          │
╞═════╪══════════════╡
│ 1   │ a            │
│ 1   │ b            │
│ 2   │ c            │
│ 2   │ d            │
│ 2   │ e            │
└─────┴──────────────┘
```



<h2 id="YCDZw">将数据框分区为多个数据框</h2>
我们之前在第12章讨论了`group_by`操作。你可以使用类似的函数将DataFrame拆分为多个分区。通过使用`partition_by`，你可以根据给定的列对DataFrame进行分组，并将这些组作为单独的DataFrame返回：`partition_by`接受以下参数：

+ `**by**` 和 `**more_by**`: 用于分组的列。
+ `**maintain_order**`: 确保分组的顺序是确定的。
+ `**include_key**`: 返回一组带有分组键的元组列表，而不是DataFrame列表。
+ `**as_dict**`: 返回分组键作为字典键的字典。

让我们用一些虚构的不同地区的销售数据来创建一个示例：

```python
df = pl.DataFrame({
    "OrderID": [1, 2, 3, 4, 5, 6],
    "Product": ["A", "B", "A", "C", "B", "A"],
    "Quantity": [10, 5, 8, 7, 3, 12],
    "Region": ["North", "South", "North", "West", "South", "West"]
})
```



现在你可以按`Region`列对DataFrame进行分区：

```python
df.partition_by("Region")
```

```plain
[shape: (2, 4)
 ┌─────────┬─────────┬──────────┬────────┐
 │ OrderID │ Product │ Quantity │ Region │
 │ ---     │ ---     │ ---      │ ---    │
 │ i64     │ str     │ i64      │ str    │
 ╞═════════╪═════════╪══════════╪════════╡
 │ 1       │ A       │ 10       │ North  │
 │ 3       │ A       │ 8        │ North  │
 └─────────┴─────────┴──────────┴────────┘,
 shape: (2, 4)
 ┌─────────┬─────────┬──────────┬────────┐
 │ OrderID │ Product │ Quantity │ Region │
 │ ---     │ ---     │ ---      │ ---    │
 │ i64     │ str     │ i64      │ str    │
 ╞═════════╪═════════╪══════════╪════════╡
 │ 2       │ B       │ 5        │ South  │
 │ 5       │ B       │ 3        │ South  │
 └─────────┴─────────┴──────────┴────────┘,
 shape: (2, 4)
 ┌─────────┬─────────┬──────────┬────────┐
 │ OrderID │ Product │ Quantity │ Region │
 │ ---     │ ---     │ ---      │ ---    │
 │ i64     │ str     │ i64      │ str    │
 ╞═════════╪═════════╪══════════╪════════╡
 │ 4       │ C       │ 7        │ West   │
 │ 6       │ A       │ 12       │ West   │
 └─────────┴─────────┴──────────┴────────┘]
```



如果你想删除正在分区的列，可以将`include_key`设置为`False`：

```python
df.partition_by("Region", include_key=False)
```

```plain
[shape: (2, 3)
 ┌─────────┬─────────┬──────────┐
 │ OrderID │ Product │ Quantity │
 │ ---     │ ---     │ ---      │
 │ i64     │ str     │ i64      │
 ╞═════════╪═════════╪══════════╡
 │ 1       │ A       │ 10       │
 │ 3       │ A       │ 8        │
 └─────────┴─────────┴──────────┘,
 shape: (2, 3)
 ┌─────────┬─────────┬──────────┐
 │ OrderID │ Product │ Quantity │
 │ ---     │ ---     │ ---      │
 │ i64     │ str     │ i64      │
 ╞═════════╪═════════╪══════════╡
 │ 2       │ B       │ 5        │
 │ 5       │ B       │ 3        │
 └─────────┴─────────┴──────────┘,
 shape: (2, 3)
 ┌─────────┬─────────┬──────────┐
 │ OrderID │ Product │ Quantity │
 │ ---     │ ---     │ ---      │
 │ i64     │ str     │ i64      │
 ╞═════════╪═════════╪══════════╡
 │ 4       │ C       │ 7        │
 │ 6       │ A       │ 12       │
 └─────────┴─────────┴──────────┘]
```



最后，如果你想将结果作为字典返回，使用分组键的元组作为键，DataFrame作为值，可以将`as_dict`参数设置为`True`：

```python
dfs = df.partition_by(["Region"], as_dict=True)
dfs
```

```plain
{('North',): shape: (2, 4)
 ┌─────────┬─────────┬──────────┬────────┐
 │ OrderID │ Product │ Quantity │ Region │
 │ ---     │ ---     │ ---      │ ---    │
 │ i64     │ str     │ i64      │ str    │
 ╞═════════╪═════════╪══════════╪════════╡
 │ 1       │ A       │ 10       │ North  │
 │ 3       │ A       │ 8        │ North  │
 └─────────┴─────────┴──────────┴────────┘,
 ('South',): shape: (2, 4)
 ┌─────────┬─────────┬──────────┬────────┐
 │ OrderID │ Product │ Quantity │ Region │
 │ ---     │ ---     │ ---      │ ---    │
 │ i64     │ str     │ i64      │ str    │
 ╞═════════╪═════════╪══════════╪════════╡
 │ 2       │ B       │ 5        │ South  │
 │ 5       │ B       │ 3        │ South  │
 └─────────┴─────────┴──────────┴────────┘,
 ('West',): shape: (2, 4)
 ┌─────────┬─────────┬──────────┬────────┐
 │ OrderID │ Product │ Quantity │ Region │
 │ ---     │ ---     │ ---      │ ---    │
 │ i64     │ str     │ i64      │ str    │
 ╞═════════╪═════════╪══════════╪════════╡
 │ 4       │ C       │ 7        │ West   │
 │ 6       │ A       │ 12       │ West   │
 └─────────┴─────────┴──────────┴────────┘}
```



然后你可以通过访问字典中想要的键来获取DataFrame：

```python
dfs[("North",)]
```

```plain
shape: (2, 4)
┌─────────┬─────────┬──────────┬────────┐
│ OrderID │ Product │ Quantity │ Region │
│ ---     │ ---     │ ---      │ ---    │
│ i64     │ str     │ i64      │ str    │
╞═════════╪═════════╪══════════╪════════╡
│ 1       │ A       │ 10       │ North  │
│ 3       │ A       │ 8        │ North  │
└─────────┴─────────┴──────────┴────────┘
```



这就是如何将你的DataFrame分区为多个DataFrame的方法！

<h2 id="NQD99">本章小结</h2>
在本章中，你学习了如何重塑数据。

我们讨论了数据的宽格式和长格式。

我们向你展示了如何将数据从长格式透视到宽格式。

我们向你展示了如何将数据从宽格式熔化为长格式。

我们向你展示了如何转置数据，按对角线翻转DataFrame。

我们向你展示了如何将嵌套值展开为长格式。

我们向你展示了如何将DataFrame分区为多个DataFrame。

现在你已经准备好像专家一样重塑你的数据，接下来你可以为数据可视化做好准备，我们将在下一章讨论！

