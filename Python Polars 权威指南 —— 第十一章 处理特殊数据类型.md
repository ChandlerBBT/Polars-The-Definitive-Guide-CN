

在 **第3章** 中，我们讨论了 Polars 支持的基本数据类型以及它们如何用于 DataFrame 中存储信息。然而，某些数据类型需要特别关注。它们有些有特殊的操作可以执行，有些则针对特定的使用场景进行了优化。在 Polars 中，这些特殊数据类型在表达式中有自己的命名空间，这意味着你可以通过 `Expr` 命名空间访问它们的方法和属性。

Polars 中的特殊数据类型有 `String`、`Categorical` 和 `Enum`；时间数据类型有 `Date`、`DateTime`、`Time` 和 `Duration`；嵌套数据类型有 `Array`、`List` 和 `Struct`。本章将深入探讨这些特殊数据类型及其操作。



<h2 id="EL8gK">字符串</h2>
`string` 是一种用于表示文本的数据类型，由一系列字符、数字或符号组成。字符串的一个挑战在于其长度不固定。例如，整数是固定长度的：你可以通过将整数的大小加到当前内存地址上来计算下一个整数的内存地址。而字符串则不行。字符串的长度是未知的，因此不能仅根据数据缓冲区预测下一个字符串的内存地址。这意味着字符串必须与整数不同地存储：在数据缓冲区中是连续存储的。**连续内存** 是指一个长的内存块，所有的值都存储在一排中。

视图布局存储了字符串值的几个属性：

+ 字节 0 到 3 存储字符串的长度。
+ 字节 4 到 7 存储字符串前 4 个字节的副本。这允许“快速路径”或优化，因为这 4 个字节通常包含快速比较所需的信息。
+ 字节 8 到 11 存储字符串所在的数据缓冲区的索引。
+ 字节 12 到 15 存储偏移量：即字符串在数据缓冲区中的起始位置。

通过这些信息，你可以从数据缓冲区中检索字符串，而无需在内存中查找！



Polars 对长度小于 12 个字节的字符串还有另一种优化。在这种情况下，字符串存储在视图布局本身中，而不是数据缓冲区中。这称为**内嵌**。当字符串长度不超过 12 字节时，字符串可以存储在紧随其后的 12 字节内。这避免了 Polars 在数据缓冲区中分配内存和查找，这两者都是代价高昂的操作。

**图 11-1** 说明了这种存储方式。Aerosmith 可以内嵌，因此不在数据缓冲区中。而 "Toots and the Maytals" 存储在数据缓冲区中，从 "The Velvet UnderGround" 的第 22 个位置开始（第一个位置是 0）。

![图11-1 内存存储长短字符串存储的示意图](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728825812501-55eb22f5-c401-46a2-afa4-cacdc1213c89.png)

<h3 id="TBUEj">方法</h3>
现在你已经知道字符串如何在物理内存中存储，让我们来看看有哪些操作可用于它们。

<h4 id="JKVMf">转换(Conversion)</h4>
**表11-1** 中的方法允许你将字符串与不同的数据类型或格式之间进行转换。

__

_表 11-1：字符串数据类型的转换方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">pl.Expr.str.decode(…)</font>**` | 使用提供的编码解码一个值。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.encode(…)</font>**` | 使用提供的编码编码一个值。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.json_decode(…)</font>**` | 将字符串值解析为 JSON。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.json_extract(…)</font>**` | 将字符串值解析为 JSON。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.json_path_match(…)</font>**` | 使用提供的 JSONPath 表达式提取 JSON 字符串的第一个匹配项。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.strptime(…)</font>**` | 将 `String` 列转换为 `Date`/`Datetime`/`Time` 列。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.to_date(…)</font>**` | 将 `String` 列转换为 `Date` 列。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.to_datetime(…)</font>**` | 将 `String` 列转换为 `Datetime` 列。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.to_decimal(…)</font>**` | 将 `String` 列转换为 `Decimal` 列。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.to_integer(…)</font>**` | 将 `String` 列转换为带有基数$ ^{\text{a}} $ 的 `Int64` 列。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.to_time(…)</font>**` | 将 `String` 列转换为 `Time` 列。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.parse_int(…)</font>**` | 使用基数从字符串中解析整数。 |


> a. 基数（Radix）指的是一个数字系统的基数，指定了它使用的位数。在将字符串转换为整数的上下文中，基数决定了如何解释字符串中的符号。如果不知道基数，转换可能会模棱两可。例如，“101” 可能代表不同的值，具体取决于它是十进制（101）、二进制（5）还是十六进制（272）。默认基数是十进制（10）。
>



<h4 id="feuDu">描述和查询方法</h4>
**表11-2** 中的方法可以返回列中字符串值的属性，或允许你查询某些模式。

__

_表 11-2：字符串数据类型的描述方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">pl.Expr.str.contains(…)</font>**` | 检查 `Series` 中的字符串是否包含匹配正则表达式的子字符串。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.find(…)</font>**` | 返回 `Series` 字符串中匹配模式的第一个子字符串的索引。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.len_bytes()</font>**` | 返回每个字符串的长度（以字节数表示）。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.len_chars()</font>**` | 返回每个字符串的长度（以字符数表示）。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.lengths()</font>**` | 返回每个字符串的字节数。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.n_chars()</font>**` | 返回每个字符串的长度（以字符数表示）。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.starts_with(…)</font>**` | 检查字符串值是否以某个子字符串开头。 |




<h4 id="JHX0b">操纵(<font style="color:rgb(61, 59, 73);">Manipulation)</font></h4>
**表11-3** 中的方法允许你操作列中的字符串值。

__

_表 11-3：字符串数据类型的操作方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">pl.Expr.str.concat(…)</font>**` | 垂直连接列中的字符串值为单个字符串值。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.contains_any(…)</font>**` | 使用 `aho-corasick` 算法查找匹配项。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.count_match(…)</font>**` | 计算所有连续的非重叠正则表达式匹配项。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.count_matches(…)</font>**` | 计算所有连续的非重叠正则表达式匹配项。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.ends_with(…)</font>**` | 检查字符串值是否以某个子字符串结尾。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.explode()</font>**` | 返回一个列，其中每个字符串字符占一行。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.extract(…)</font>**` | 提取提供的模式中的目标捕获组。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.extract_all(…)</font>**` | 提取给定正则表达式模式的所有匹配项。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.extract_groups(…)</font>**` | 提取给定正则表达式模式的所有捕获组。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.ljust(…)</font>**` | 返回左对齐的字符串，长度为 `length`。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.lstrip(…)</font>**` | 移除前导字符。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.pad_end(…)</font>**` | 在字符串末尾填充字符，直到达到给定的 `length`。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.pad_start(…)</font>**` | 在字符串开头填充字符，直到达到给定的 `length`。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.replace(…)</font>**` | 将第一个匹配的正则表达式/文本子字符串替换为新字符串值。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.replace_all(…)</font>**` | 将所有匹配的正则表达式/文本子字符串替换为新字符串值。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.replace_many(…)</font>**` | 使用 `aho-corasick` 算法替换多个匹配项。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.reverse(…)</font>**` | 反转字符串值的顺序。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.rjust(…)</font>**` | 返回右对齐的字符串，长度为 `length`。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.rstrip(…)</font>**` | 移除尾随字符。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.slice(…)</font>**` | 创建字符串值的子切片。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.split(…)</font>**` | 按子字符串拆分字符串。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.split_exact(…)</font>**` | 按子字符串拆分字符串，使用 `n` 次拆分。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.splitn(…)</font>**` | 按子字符串拆分字符串，最多返回 `n` 个项。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.strip_chars(…)</font>**` | 移除前导和尾随字符。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.strip_chars_start(…)</font>**` | 移除前导字符。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.strip_chars_end(…)</font>**` | 移除尾随字符。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.strip_prefix(…)</font>**` | 移除前缀。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.strip_suffix(…)</font>**` | 移除后缀。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.to_lowercase()</font>**` | 将字符串修改为其小写等效形式。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.to_titlecase()</font>**` | 将字符串修改为其标题大小写等效形式。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.to_uppercase()</font>**` | 将字符串修改为其大写等效形式。 |
| `**<font style="color:#01B2BC;">pl.Expr.str.zfill(…)</font>**` | 用零填充字符串的开头，直到达到给定长度。 |




<h3 id="sEaIp">例子</h3>
让我们深入研究一些例子。首先创建案例数据：

```python
import polars as pl

df = pl.DataFrame({
    "raw_text": [
        "  Data Science is amazing ",
        "Data_analysis > Data entry",
        " Python&Polars; Fast",
    ]
})
print(df)
```

```plain
shape: (3, 1)
┌────────────────────────────┐
│ raw_text                   │
│ ---                        │
│ str                        │
╞════════════════════════════╡
│   Data Science is amazing  │
│ Data_analysis > Data entry │
│  Python&Polars; Fast       │
└────────────────────────────┘
```

这个示例 DataFrame 展示了一些在 Polars 中可用的字符串操作。首先从清理字符串开始：

```python
df = df.with_columns(
    pl.col("raw_text")
    .str.strip_chars()  ①
    .str.to_lowercase()  ②
    .str.replace_all("_", " ")  ③
    .alias("processed_text")  ④
)
print(df)
```

```plain
shape: (3, 2)
┌────────────────────────────┬────────────────────────────┐
│ raw_text                   │ processed_text             │
│ ---                        │ ---                        │
│ str                        │ str                        │
╞════════════════════════════╪════════════════════════════╡
│   Data Science is amazing  │ data science is amazing    │
│ Data_analysis > Data entry │ data analysis > data entry │
│  Python&Polars; Fast       │ python&polars; fast        │
└────────────────────────────┴────────────────────────────┘
```

①`strip_chars()` 方法移除字符串中的前导和尾随字符。由于你没有提供任何要移除的字符，它默认移除空白字符。

② 将所有字符转换为小写可以使数据处理更容易，因为这使得数据不区分大小写。

③ 在处理文件名或 URL 时，可能需要将所有下划线替换为空格。

④ 创建一个包含处理后文本的新列，并为其命名。



现在你有了可以处理的干净数据，让我们开始操作和选择它。一个常见的操作是对字符串进行切片和分割：

```python
print(
    df.with_columns(
        pl.col("processed_text")
        .str.slice(0, 5)  ①
        .alias("first_5_chars"),
        pl.col("processed_text")
        .str.split(" ")  ②
        .list.get(0)  ③
        .alias("first_word"),
        pl.col("processed_text")
        .str.split(" ")
        .list.get(1)  ④
        .alias("second_word"),
    )
)
```

```plain
shape: (3, 5)
┌────────────────┬───────────────┬───────────────┬───────────────┬─────────────┐
│ raw_text       │ processed_tex │ first_5_chars │ first_word    │ second_word │
│ ---            │ t             │ ---           │ ---           │ ---         │
│ str            │ ---           │ str           │ str           │ str         │
│                │ str           │               │               │             │
╞════════════════╪═══════════════╪═══════════════╪═══════════════╪═════════════╡
│ Data Science   │ data science  │ data          │ data          │ science     │
│ is am…         │ is amaz…      │               │               │             │
│ Data_analysis  │ data analysis │ data          │ data          │ analysis    │
│ > Data…        │ > data…       │               │               │             │
│ Python&Polars; │ python&polars │ pytho         │ python&polars │ fast        │
│ Fast           │ ; fast        │               │ ;             │             │
└────────────────┴───────────────┴───────────────┴───────────────┴─────────────┘
```

① 从 `processed_text` 列的字符串值中，切取前 5 个字符，并将其保存在 `first_5_chars` 列中。

② 在 `processed_text` 列中按空格分割字符串。这将创建一个字符串列表，其长度为字符串中空格数量 + 1。

③ 从该字符串列表中获取第一个元素，这将是第一个单词。

④ 从该字符串列表中获取第二个元素。



你还可以对字符串进行查询，以获取一些信息，如下所示：

```python
print(
    df.with_columns(
        pl.col("processed_text")
        .str.len_chars()  ①
        .alias("amount_of_chars"),
        pl.col("processed_text")
        .str.len_bytes()  ②
        .alias("amount_of_bytes"),
        pl.col("processed_text")
        .str.count_matches("a")  ③
        .alias("count_a"),
    )
)
```

```plain
shape: (3, 5)
┌─────────────────┬────────────────┬────────────────┬────────────────┬─────────┐
│ raw_text        │ processed_text │ amount_of_char │ amount_of_byte │ count_a │
│ ---             │ ---            │ s              │ s              │ ---     │
│ str             │ str            │ ---            │ ---            │ u32     │
│                 │                │ u32            │ u32            │         │
╞═════════════════╪════════════════╪════════════════╪════════════════╪═════════╡
│ Data Science is │ data science   │ 23             │ 23             │ 4       │
│ amazing         │ is amazing     │                │                │         │
│ Data_analysis > │ data analysis  │ 26             │ 26             │ 6       │
│ Data entry      │ > data entry   │                │                │         │
│ Python&Polars;  │ python&polars; │ 19             │ 19             │ 2       │
│ Fast            │ fast           │                │                │         │
└─────────────────┴────────────────┴────────────────┴────────────────┴─────────┘
```

① 计算字符串中的字符数量。

② 计算字符串在内存中占用的字节数。

③ 计算字符串中字母 “a” 出现的次数。



---

<font style="color:#01B2BC;">提示：</font>

`len_bytes()` 的性能远远优于 `len_chars()`。`len_bytes()` 的时间复杂度为 O(1)，而 `len_chars()` 的时间复杂度为 O(n)。

O(1) 和 O(n) 是计算机科学中用于描述算法在最坏情况下的时间复杂度的符号。O(1) 意味着算法的执行时间是恒定的，与输入的大小无关。O(n) 意味着算法的执行时间与输入的大小线性相关。不同方法的时间复杂度不同，因为字节数可以直接从视图布局中获取，而字符数量则必须通过遍历数据缓冲区中的字符串来计算。

在上面的示例中，`len_chars()` 和 `len_bytes()` 的结果相同，因为你处理的是 ASCII 文本。在处理非 ASCII 文本时，字节长度和字符长度可能不同，因此你可能更愿意使用 `len_chars()`。

---

Polars 还支持正则表达式操作。正则表达式（regex）是定义搜索模式的字符序列。正则表达式主要用于字符串搜索和操作，允许你基于特定模式识别、匹配，甚至修改文本，高效处理复杂的文本任务。下面的代码示例简单地查找字符串中的所有哈希标签。

```python
df = pl.DataFrame({
    "post": ["Loving #python and #polars!", "A boomer post without a hashtag"]
})

hashtag_regex = r"#(\w+)"  ①

df.with_columns(
    pl.col("post").str.extract_all(hashtag_regex).alias("hashtags")  ②
)
```

```plain
shape: (2, 2)
┌─────────────────────────────────┬────────────────────────┐
│ post                            │ hashtags               │
│ ---                             │ ---                    │
│ str                             │ list[str]              │
╞═════════════════════════════════╪════════════════════════╡
│ Loving #python and #polars!     │ ["#python", "#polars"] │
│ A boomer post without a hashtag │ []                     │
└─────────────────────────────────┴────────────────────────┘
```

① 你定义了一个正则表达式模式，该模式匹配一个哈希标签后跟一个单词。这里的 `\w` 匹配任何单词字符。单词字符是指 a-z、A-Z、0-9，包括下划线（_）。`+` 表示前一个字符可以出现一次或多次，捕获整个单词而不仅仅是第一个字符。

② 你从 `post` 列中提取正则表达式模式的所有匹配项，并将它们保存在 `hashtags` 列中。



<h2 id="h4bjY">分类（Categoricals）</h2>
`Categorical` 数据类型可以有效地对字符串值列进行编码。对于 `String` 数据类型，所有值都是分开存储在物理内存中的，即使它们是相同的。而 `Categorical` 类型则使用了字符串缓存。

字符串缓存（string cache）是一个后台字典，它存储唯一的字符串值以及所有唯一字符串的 `UInt32` 表示形式。与其为列中的所有值存储字符串，不如使用更小的 `int` 表示形式来进行高效存储。`int` 被称为**物理表示**，而字符串则称为**词汇表示**。

如果数据列包含大量字符串值但只有少量唯一字符串值，那么这允许更高效的存储和更快的操作（因为字符串比较很昂贵）。分类值存储在两部分中：字典和索引。

让我们来探讨 `Categorical` 数据类型及其方法。首先，你将创建一个包含 `Categorical` 列的 DataFrame。此外，你还将创建一个包含其物理表示的列。

```python
df1 = pl.DataFrame(
    {"categorical_column": ["value1", "value2", "value3"]},
    schema={"categorical_column": pl.Categorical},
)

print(
    df1.with_columns(
        pl.col("categorical_column")
        .to_physical()
        .alias("categorical_column_physical")
    )
)
```

```plain
shape: (3, 2)
┌────────────────────┬───────────────────────┐
│ categorical_column │ categorical_column_p… │
│ ---                │ ---                   │
│ cat                │ u32                   │
╞════════════════════╪═══════════════════════╡
│ value1             │ 0                     │
│ value2             │ 1                     │
│ value3             │ 2                     │
└────────────────────┴───────────────────────┘
```



<h3 id="BIlSv">方法</h3>
`Categorical` 数据类型有以下两种方法：

_表 11-4：Categorical 数据类型的方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">Expr.cat.get_categories()</font>**` | 获取此数据类型中存储的类别。 |
| `**<font style="color:#01B2BC;">Expr.cat.set_ordering(…)</font>**` | 确定此分类序列应如何排序。 |


<h3 id="dQG8r">示例</h3>
列中字符串的顺序决定了 `Categorical` 和字典的样子。即使另一个 DataFrame 中的列包含相同的唯一字符串，如果顺序不同，`Categorical` 也会不同。这是因为字典的顺序不同，因此它的物理表示（`int`）也不同：

```python
df2 = pl.DataFrame(
    {"categorical_column": ["value4", "value3", "value2"]},
    schema={"categorical_column": pl.Categorical},
)

print(
    df2.with_columns(
        pl.col("categorical_column")
        .to_physical()
        .alias("categorical_column_physical")
    )
)
```

```plain
shape: (3, 2)
┌────────────────────┬───────────────────────┐
│ categorical_column │ categorical_column_p… │
│ ---                │ ---                   │
│ cat                │ u32                   │
╞════════════════════╪═══════════════════════╡
│ value4             │ 0                     │
│ value3             │ 1                     │
│ value2             │ 2                     │
└────────────────────┴───────────────────────┘
```



因此，尝试合并两个不同的 `Categorical` 时会导致 `CategoricalRemappingWarning` 警告：

```python
df1.join(df2, on="categorical_column")
```

```plain
CategoricalRemappingWarning: Local categoricals have different encodings, expens
ive re-encoding is done to perform this merge operation. Consider using a String
Cache or an Enum type if the categories are known in advance
shape: (2, 1)
┌────────────────────┐
│ categorical_column │
│ ---                │
│ cat                │
╞════════════════════╡
│ value3             │
│ value2             │
└────────────────────┘
```

要合并两个 `Categorical`，你需要通过在相同的字符串缓存下创建它们，使它们的字符串缓存匹配。你可以通过全局字符串缓存来实现。这是一种共享给所有 `Categorical` 使用的字符串缓存。通过这种方式，所有的 `Categorical` 都使用相同的字符串缓存，避免了不匹配的情况。全局字符串缓存默认是关闭的，因为对所有 `Categorical` 使用相同的字符串缓存会带来性能损失。如果字符串缓存是一个全局对象，那么在访问时需要锁定它，使线程互相等待，从而导致更长的加载时间。



下面的示例展示了如何使用 `StringCache` 上下文管理器在相同的字符串缓存下创建 `Categorical`：

```python
with pl.StringCache():
    df1 = pl.DataFrame(
        {
            "categorical_column": ["value3", "value2", "value1"],
            "other": ["a", "b", "c"],
        },
        schema={"categorical_column": pl.Categorical, "other": pl.String},
    )
    df2 = pl.DataFrame(
        {
            "categorical_column": ["value2", "value3", "value4"],
            "other": ["d", "e", "f"],
        },
        schema={"categorical_column": pl.Categorical, "other": pl.String},
    )

# 即使在全局字符串缓存的作用域之外，你现在也可以连接包含Categorical列的两个 DataFrame
df1.join(df2, on="categorical_column")
```

```plain
shape: (2, 3)
┌────────────────────┬───────┬─────────────┐
│ categorical_column │ other │ other_right │
│ ---                │ ---   │ ---         │
│ cat                │ str   │ str         │
╞════════════════════╪═══════╪═════════════╡
│ value2             │ b     │ d           │
│ value3             │ a     │ e           │
└────────────────────┴───────┴─────────────┘
```

你还可以启用全局字符串缓存，使用以下方式：

```python
pl.enable_string_cache()
```

但是请注意，这意味着全局字符串缓存将始终被使用，这相比于使用上下文管理器来说，可能不是最佳解决方案。

要检索 `Categorical`列中包含的唯一类别，进入 `Categorical` 命名空间并调用 `.get_categories()`。

```python
df2.select(pl.col("categorical_column").cat.get_categories())
```

```plain
shape: (3, 1)
┌────────────────────┐
│ categorical_column │
│ ---                │
│ str                │
╞════════════════════╡
│ value2             │
│ value3             │
│ value4             │
└────────────────────┘
```



最后一个相关属性是列在 `sort()` 中的排序方式。共有两种选项：

+ **物理排序（默认）**：使用物理（整数）表示来进行排序。
+ **词汇排序**：使用字符串值来进行排序。

在创建 `Categorical` 数据类型时，你可以设置这些选项。你可以通过将 `Categorical` 转换为另一种变体来切换排序方式。

首先，准备其中一个 DataFrame：

```python
sorting_comparison_df = (
    df2
    .select(
        pl.col("categorical_column")
        .alias("categorical_lexical")
    )
    .with_columns(
        pl.col("categorical_lexical")
        .to_physical()
        .alias("categorical_physical")
    )
)
print(sorting_comparison_df)
```

```plain
shape: (3, 2)
┌─────────────────────┬──────────────────────┐
│ categorical_lexical │ categorical_physical │
│ ---                 │ ---                  │
│ cat                 │ u32                  │
╞═════════════════════╪══════════════════════╡
│ value2              │ 1                    │
│ value3              │ 0                    │
│ value4              │ 3                    │
└─────────────────────┴──────────────────────┘
```



下面的示例中，`Categorical` 列按照物理表示排序，可以在 `categorical_physical` 列中看到：

这里则是按照词汇表示进行排序，可以在 `categorical_column` 列中看到：

```python
print(
    sorting_comparison_df
    .with_columns(
        pl.col("categorical_lexical")
        .cast(pl.Categorical("physical"))  # 默认值
    )
    .sort(by="categorical_lexical")
)
```

```plain
shape: (3, 2)
┌─────────────────────┬──────────────────────┐
│ categorical_lexical │ categorical_physical │
│ ---                 │ ---                  │
│ cat                 │ u32                  │
╞═════════════════════╪══════════════════════╡
│ value3              │ 0                    │
│ value2              │ 1                    │
│ value4              │ 3                    │
└─────────────────────┴──────────────────────┘
```



在词汇表示中进行排序，可以在 `categorical_column` 列中看到：

```python
print(
    sorting_comparison_df
    .with_columns(
        pl.col("categorical_lexical")
        .cast(pl.Categorical("lexical"))
    )
    .sort(by="categorical_lexical")
)
```

```plain
shape: (3, 2)
┌─────────────────────┬──────────────────────┐
│ categorical_lexical │ categorical_physical │
│ ---                 │ ---                  │
│ cat                 │ u32                  │
╞═════════════════════╪══════════════════════╡
│ value2              │ 1                    │
│ value3              │ 0                    │
│ value4              │ 3                    │
└─────────────────────┴──────────────────────┘
```



<h3 id="QwlaL">枚举（Enum）</h3>
如果你提前知道某列的类别，你可以使用 `Enum` 数据类型。当前这个数据类型在底层使用 `Categorical` 数据类型，但将来可能会有自己的实现。

```python
enum_dtype = pl.Enum(["Polar", "Panda", "Brown"])
enum_series = pl.Series(
    ["Polar", "Panda", "Brown", "Brown", "Polar"], dtype=enum_dtype
)

cat_series = pl.Series(
    ["Polar", "Panda", "Brown", "Brown", "Polar"], dtype=pl.Categorical
)
```

枚举是 Polars 中的一种新数据类型，在撰写本文时尚未拥有自己的命名空间。



<h2 id="sacky">时间数据（Temporal Data）</h2>
时间数据类型是处理基于时间的信息（如时间点和时间间隔）的专用格式。这些类型允许进行比较、算术和其他与时间相关的操作。

Polars 使用几种数据类型来存储时间数据，如表 11-5 所示。

__

_表 11-5：时间数据类型_

| 数据类型 | 描述 | 示例 | 存储形式 |
| --- | --- | --- | --- |
| `**<font style="color:#01B2BC;">Date</font>**` | 表示不带时间的日历日期。 | 生日 | 使用自 UNIX 纪元（1970-01-01）以来的天数存储为 `int32` |
| `**<font style="color:#01B2BC;">Datetime</font>**` | 表示带有时间的日历日期。 | 日志中的时间戳 | 使用自 UNIX 纪元以来的时间存储为 `int64`，支持纳秒、微秒、毫秒等不同单位。 |
| `**<font style="color:#01B2BC;">Duration</font>**` | 表示时间间隔，即两个时间点之间的差异，类似于 Python 中的 `timedelta`。 | 两个事件之间的时间差 | 当从 Date/Datetime 相减时产生 `int64` |
| `**<font style="color:#01B2BC;">Time</font>**` | 仅关注一天中的时间。 | 每日任务的安排 | 使用自午夜以来的纳秒存储为 `int64` |




<h3 id="t4elt">方法</h3>
时间数据命名空间提供了多种用于转换、描述和操作数据的方法。



<h4 id="YkTQr">转换</h4>
下列方法允许你将时间数据与其他数据类型或格式之间进行转换。

_表 11-6：用于转换其他数据类型的方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">Expr.dt.cast_time_unit(…)</font>**` | 将底层数据转换为另一种时间单位。 |
| `**<font style="color:#01B2BC;">Expr.dt.strftime(…)</font>**` | 将 `Date`/`Time`/`Datetime` 列转换为具有指定格式的字符串列。 |
| `**<font style="color:#01B2BC;">Expr.dt.to_string(…)</font>**` | 将 `Date`/`Time`/`Datetime` 列转换为具有指定格式的字符串列。 |


<h4 id="e2pgj">描述</h4>
以下方法可以返回时间数据的属性。

_表 11-7：用于描述时间数据的方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">Expr.dt.base_utc_offset()</font>**` | 获取与 UTC 的基础时区偏移量。 |
| `**<font style="color:#01B2BC;">Expr.dt.date()</font>**` | 从日期时间中提取日期。 |
| `**<font style="color:#01B2BC;">Expr.dt.datetime()</font>**` | 返回日期时间。 |
| `**<font style="color:#01B2BC;">Expr.dt.day()</font>**` | 从日期表示中提取天。 |
| `**<font style="color:#01B2BC;">Expr.dt.days()</font>**` | 从 Duration 类型中提取总天数。 |
| `**<font style="color:#01B2BC;">Expr.dt.dst_offset()</font>**` | 获取当前生效的附加时区偏移量（通常由于夏令时）。 |
| `**<font style="color:#01B2BC;">Expr.dt.epoch(…)</font>**` | 获取自 Unix 纪元以来的时间（指定时间单位）。 |
| `**<font style="color:#01B2BC;">Expr.dt.hour()</font>**` | 从日期时间表示中提取小时。 |
| `**<font style="color:#01B2BC;">Expr.dt.hours()</font>**` | 从 Duration 类型中提取总小时数。 |
| `**<font style="color:#01B2BC;">Expr.dt.is_leap_year()</font>**` | 判断底层日期的年份是否为闰年。 |
| `**<font style="color:#01B2BC;">Expr.dt.iso_year()</font>**` | 从日期表示中提取 ISO 年。 |
| `**<font style="color:#01B2BC;">Expr.dt.microsecond()</font>**` | 从日期时间表示中提取微秒。 |
| `**<font style="color:#01B2BC;">Expr.dt.microseconds()</font>**` | 从 Duration 类型中提取总微秒数。 |
| `**<font style="color:#01B2BC;">Expr.dt.millisecond()</font>**` | 从日期时间表示中提取毫秒。 |
| `**<font style="color:#01B2BC;">Expr.dt.milliseconds()</font>**` | 从 Duration 类型中提取总毫秒数。 |
| `**<font style="color:#01B2BC;">Expr.dt.minute()</font>**` | 从日期时间表示中提取分钟。 |
| `**<font style="color:#01B2BC;">Expr.dt.minutes()</font>**` | 从 Duration 类型中提取总分钟数。 |
| `**<font style="color:#01B2BC;">Expr.dt.month()</font>**` | 从日期表示中提取月份。 |
| `**<font style="color:#01B2BC;">Expr.dt.nanosecond()</font>**` | 从日期时间表示中提取纳秒。 |
| `**<font style="color:#01B2BC;">Expr.dt.nanoseconds()</font>**` | 从 Duration 类型中提取总纳秒数。 |
| `**<font style="color:#01B2BC;">Expr.dt.ordinal_day()</font>**` | 从日期表示中提取序号天数。 |
| `**<font style="color:#01B2BC;">Expr.dt.quarter()</font>**` | 从日期表示中提取季度。 |
| `**<font style="color:#01B2BC;">Expr.dt.second(…)</font>**` | 从日期时间表示中提取秒数。 |
| `**<font style="color:#01B2BC;">Expr.dt.seconds()</font>**` | 从 Duration 类型中提取总秒数。 |
| `**<font style="color:#01B2BC;">Expr.dt.time()</font>**` | 提取时间。 |
| `**<font style="color:#01B2BC;">Expr.dt.timestamp(…)</font>**` | 返回指定时间单位的时间戳。 |
| `**<font style="color:#01B2BC;">Expr.dt.total_days()</font>**` | 从 Duration 类型中提取总天数。 |
| `**<font style="color:#01B2BC;">Expr.dt.total_hours()</font>**` | 从 Duration 类型中提取总小时数。 |
| `**<font style="color:#01B2BC;">Expr.dt.total_microseconds()</font>**` | 从 Duration 类型中提取总微秒数。 |
| `**<font style="color:#01B2BC;">Expr.dt.total_milliseconds()</font>**` | 从 Duration 类型中提取总毫秒数。 |
| `**<font style="color:#01B2BC;">Expr.dt.total_minutes()</font>**` | 从 Duration 类型中提取总分钟数。 |
| `**<font style="color:#01B2BC;">Expr.dt.total_nanoseconds()</font>**` | 从 Duration 类型中提取总纳秒数。 |
| `**<font style="color:#01B2BC;">Expr.dt.total_seconds()</font>**` | 从 Duration 类型中提取总秒数。 |
| `**<font style="color:#01B2BC;">Expr.dt.year()</font>**` | 从日期表示中提取年份。 |


<h4 id="Ge0Om">操纵</h4>
以下方法允许你操作时间数据。

_表 11-8：用于操作时间数据的方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">Expr.dt.replace_time_zone(…)</font>**` | 替换 Datetime 表达式的时区。 |
| `**<font style="color:#01B2BC;">Expr.dt.combine(…)</font>**` | 从现有的 Date/Datetime 表达式和 Time 创建一个朴素的 Datetime。 |
| `**<font style="color:#01B2BC;">Expr.dt.month_start()</font>**` | 回滚到该月的第一天。 |
| `**<font style="color:#01B2BC;">Expr.dt.month_end()</font>**` | 滚动到该月的最后一天。 |
| `**<font style="color:#01B2BC;">Expr.dt.offset_by(…)</font>**` | 按相对时间偏移量偏移此日期。 |
| `**<font style="color:#01B2BC;">Expr.dt.round(…)</font>**` | 将 Date/Datetime 范围划分为桶。 |
| `**<font style="color:#01B2BC;">Expr.dt.truncate(…)</font>**` | 将 Date/Datetime 范围划分为桶。 |
| `**<font style="color:#01B2BC;">Expr.dt.week()</font>**` | 从日期表示中提取周数。 |
| `**<font style="color:#01B2BC;">Expr.dt.weekday()</font>**` | 从日期表示中提取星期几。 |
| `**<font style="color:#01B2BC;">Expr.dt.with_time_unit(…)</font>**` | 设置 DateTime 或 Duration 表达式的时间单位。 |
| `**<font style="color:#01B2BC;">Expr.dt.convert_time_zone(…)</font>**` | 将 DateTime 表达式转换为指定时区。 |


<h3 id="HslEb">示例</h3>
时间序列领域非常广泛，我们无法涵盖所有内容。然而，我们可以展示一些时间序列分析中常用的操作，并说明它们在 Polars 中如何处理。在接下来的示例中，你主要将处理日期，但我们展示的方法也适用于其他时间数据类型。

<h4 id="Ks2QD">从 CSV 加载</h4>
要在 Polars 中开始处理时间数据，首先需要加载它。你可以从 CSV 文件中加载时间数据。使用 `read_csv` 方法并将 `try_parse_dates` 参数设置为 True：

```python
pl.read_csv("data/all_stocks.csv", try_parse_dates=True)
```

```plain
shape: (18_476, 8)
┌────────┬────────────┬────────────┬───┬────────────┬────────────┬──────────┐
│ symbol │ date       │ open       │ … │ close      │ adj close  │ volume   │
│ ---    │ ---        │ ---        │   │ ---        │ ---        │ ---      │
│ str    │ date       │ f64        │   │ f64        │ f64        │ i64      │
╞════════╪════════════╪════════════╪═══╪════════════╪════════════╪══════════╡
│ ASML   │ 1999-01-04 │ 11.765625  │ … │ 12.140625  │ 7.535689   │ 1801867  │
│ ASML   │ 1999-01-05 │ 11.859375  │ … │ 13.96875   │ 8.670406   │ 8241600  │
│ ASML   │ 1999-01-06 │ 14.25      │ … │ 16.875     │ 10.474315  │ 16400267 │
│ ASML   │ 1999-01-07 │ 14.742188  │ … │ 16.851563  │ 10.459769  │ 17722133 │
│ ASML   │ 1999-01-08 │ 16.078125  │ … │ 15.796875  │ 9.805122   │ 10696000 │
│ …      │ …          │ …          │ … │ …          │ …          │ …        │
│ TSM    │ 2023-06-26 │ 102.019997 │ … │ 100.110001 │ 99.125954  │ 8560000  │
│ TSM    │ 2023-06-27 │ 101.150002 │ … │ 102.080002 │ 101.076591 │ 9732000  │
│ TSM    │ 2023-06-28 │ 100.5      │ … │ 100.919998 │ 99.927986  │ 8160900  │
│ TSM    │ 2023-06-29 │ 101.339996 │ … │ 100.639999 │ 99.650742  │ 7383900  │
│ TSM    │ 2023-06-30 │ 101.400002 │ … │ 100.919998 │ 99.927986  │ 11701700 │
└────────┴────────────┴────────────┴───┴────────────┴────────────┴──────────┘
```

这里可以通过列标题中的数据类型看到，`date` 列已按正确的格式读取。

<h4 id="BBtkG">字符串的转换</h4>
或者，你也可以从字符串中解析日期，如下所示：

```python
df = pl.DataFrame({
    "date_str": ["2023-12-31", "2024-02-29"]
})

df = df.with_columns(
    pl.col("date_str").str.strptime(pl.Date, "%Y-%m-%d").alias("date")
)
print(df)
```

```plain
shape: (2, 2)
┌────────────┬────────────┐
│ date_str   │ date       │
│ ---        │ ---        │
│ str        │ date       │
╞════════════╪════════════╡
│ 2023-12-31 │ 2023-12-31 │
│ 2024-02-29 │ 2024-02-29 │
└────────────┴────────────┘
```



如果你想将日期以特定格式写入字符串，可以执行以下操作：

```python
df = df.with_columns(
    pl.col("date").dt.to_string("%d-%m-%Y").alias("formatted_date")
)

print(df)
```

```plain
shape: (2, 3)
┌────────────┬────────────┬────────────────┐
│ date_str   │ date       │ formatted_date │
│ ---        │ ---        │ ---            │
│ str        │ date       │ str            │
╞════════════╪════════════╪════════════════╡
│ 2023-12-31 │ 2023-12-31 │ 31-12-2023     │
│ 2024-02-29 │ 2024-02-29 │ 29-02-2024     │
└────────────┴────────────┴────────────────┘
```

在这里，你提供给 `to_string()` 方法的格式是 `%d-%m-%Y`，这意味着日期、月份和年份用连字符分隔。格式化选项定义在 [chrono strftime文档](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) 中，Polars 使用了该文档中的定义。

<h4 id="zx712">生成范围</h4>
除了从其他来源加载数据外，你还可以直接在 Polars 中生成日期范围和日期时间范围：

```python
from datetime import date
df = pl.DataFrame(
    {
        "date": pl.date_range(
            start=date(2023,12,31),  ①
            end=date(2024,1,15),
            interval="1w",  ②
            eager=True,  ③
        ),
    }
)
print(df)
```

```plain
shape: (3, 1)
┌────────────┐
│ date       │
│ ---        │
│ date       │
╞════════════╡
│ 2023-12-31 │
│ 2024-01-07 │
│ 2024-01-14 │
└────────────┘
```

① 对于 `start` 和 `end` 参数，你可以使用 Python 标准库中的 `datetime.date` 类型。

②`interval` 参数可以设置为表示间隔的字符串：例如，“1w” 表示一周，“1d” 表示一天，“1h” 表示一小时，等等。

③ 将 `eager` 参数设置为 True 以返回范围为 `Series` 对象，或设置为 False 以返回表达式对象。由于我们这里使用的是 `DataFrame` 构造器，所以不能使用表达式，因为这会导致 `TypeError`: `DataFrame` 构造器不支持传递表达式对象。

<h4 id="h1E5J">时区</h4>
处理时间数据时最令人头疼的事情之一就是时区，特别是夏令时（Daylight Saving Time）。因此，在时间序列分析中经常使用世界协调时（UTC），因为它是一个通用的固定时区。然后你可以从 UTC 转换为任何你想要的时区。

在下一个示例中，我们有一个数据集使用的是 UTC 时区，我们想将其转换为阿姆斯特丹时区：即中欧时间（CEST）。

```python
df = pl.DataFrame(  ①
    {
        "utc_mixed_offset_data": [
            "2021-03-27T00:00:00+0100",
            "2021-03-28T00:00:00+0100",
            "2021-03-29T00:00:00+0200",
            "2021-03-30T00:00:00+0200",
        ]
    }
)
df = (
    df.with_columns(
        pl.col("utc_mixed_offset_data")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%z")  ②
        .alias("parsed_data")
    ).with_columns(
        pl.col("parsed_data")
        .dt.convert_time_zone("Europe/Amsterdam")  ③
        .alias("converted_data")
    )
)
print(df)
```

```plain
shape: (4, 3)
┌──────────────────────────┬─────────────────────────┬──────────────────────────┐
│ utc_mixed_offset_data    │ parsed_data             │ converted_data           │
│ ---                      │ ---                     │ ---                      │
│ str                      │ datetime[μs, UTC]       │ datetime[μs,             │
│                          │                         │ Europe/Amsterdam]        │
╞══════════════════════════╪═════════════════════════╪══════════════════════════╡
│ 2021-03-27T00:00:00+0100 │ 2021-03-26 23:00:00 UTC │ 2021-03-27 00:00:00 CET  │
│ 2021-03-28T00:00:00+0100 │ 2021-03-27 23:00:00 UTC │ 2021-03-28 00:00:00 CET  │
│ 2021-03-29T00:00:00+0200 │ 2021-03-28 22:00:00 UTC │ 2021-03-29 00:00:00 CEST │
│ 2021-03-30T00:00:00+0200 │ 2021-03-29 22:00:00 UTC │ 2021-03-30 00:00:00 CEST │
└──────────────────────────┴─────────────────────────┴──────────────────────────┘
```

① 我们创建了一个 DataFrame，其中的某列包含带有不同偏移量的日期字符串。

② 我们使用 `str.to_datetime()` 方法将这些字符串解析为日期时间。格式字符串中的 `%z` 用于解析时区偏移量。

③ 我们使用 `dt.convert_time_zone()` 方法将解析后的日期时间转换为阿姆斯特丹时区。

在生成的 DataFrame 中，可以看到日期已转换为阿姆斯特丹时区。偏移量已根据中欧标准时间（CET）和中欧夏令时间（CEST）进行解析。

在第 12 章中，我们将向你展示如何使用窗口函数、动态分组操作等对时间数据进行汇总和聚合！



<h2 id="KbRi9">列表(Listing)</h2>
在单个列中存储数据点集合有三种方式：使用 `Array`、`List` 或 `Struct`。

`List` 类型可以包含长度不同但数据类型相同的列表。

---

`pl.List` != `list`

Polars 的 `List` 只包含相同数据类型的值，与 Python 的 `list` 不同，后者可以包含不同数据类型的值。在 Polars 中可以使用 `Object` 类型来存储 Python 的 `list`，但不推荐这样做，因为内容将是序列化 Python 数据的二进制对象。这意味着没有特殊的列表操作，也没有通常适用于 Polars 数据类型的优化空间，且所有操作必须在 Python 中完成，这比在 Rust 中运行要慢得多。

---

`List` 类型在内存中的实现与 Arrow 的可变大小列表布局（Variable Size List Layout）相同。类似于 `String` 类型，它有一个连续的数据缓冲区和一个偏移缓冲区，指向数据缓冲区中值的内存位置。

<h3 id="lIIKR">方法</h3>
_表 11-9：List 类型的方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">Expr.list.all()</font>**` | 判断列表中的所有布尔值是否都为 `True`。 |
| `**<font style="color:#01B2BC;">Expr.list.any()</font>**` | 判断列表中是否有任意一个布尔值为 `True`。 |
| `**<font style="color:#01B2BC;">Expr.list.drop_nulls()</font>**` | 删除列表中的所有 `null` 值。 |
| `**<font style="color:#01B2BC;">Expr.list.arg_max()</font>**` | 获取每个子列表中的最大值的索引。 |
| `**<font style="color:#01B2BC;">Expr.list.arg_min()</font>**` | 获取每个子列表中的最小值的索引。 |
| `**<font style="color:#01B2BC;">Expr.list.concat(…)</font>**` | 以线性时间连接 `Series` 中的数组。 |
| `**<font style="color:#01B2BC;">Expr.list.contains(…)</font>**` | 检查子列表是否包含给定的项。 |
| `**<font style="color:#01B2BC;">Expr.list.count_match(…)</font>**` | 计算元素中生成的值出现的次数。 |
| `**<font style="color:#01B2BC;">Expr.list.count_matches(…)</font>**` | 计算元素中生成的值出现的次数。 |
| `**<font style="color:#01B2BC;">Expr.list.diff(…)</font>**` | 计算每个子列表中移位项的第一个离散差值。 |
| `**<font style="color:#01B2BC;">Expr.list.eval(…)</font>**` | 对列表中的元素运行任意 Polars 表达式。 |
| `**<font style="color:#01B2BC;">Expr.list.explode()</font>**` | 返回一个列，其中每个列表元素占一行。 |
| `**<font style="color:#01B2BC;">Expr.list.first()</font>**` | 获取每个子列表的第一个值。 |
| `**<font style="color:#01B2BC;">Expr.list.gather(…)</font>**` | 通过多个索引获取子列表。 |
| `**<font style="color:#01B2BC;">Expr.list.get(…)</font>**` | 根据子列表中的索引获取值。 |
| `**<font style="color:#01B2BC;">Expr.list.head(…)</font>**` | 对每个子列表切取前 `n` 个值。 |
| `**<font style="color:#01B2BC;">Expr.list.join(…)</font>**` | 连接子列表中的所有字符串项，并在它们之间放置分隔符。 |
| `**<font style="color:#01B2BC;">Expr.list.last()</font>**` | 获取每个子列表的最后一个值。 |
| `**<font style="color:#01B2BC;">Expr.list.len()</font>**` | 返回每个列表中的元素数量。 |
| `**<font style="color:#01B2BC;">Expr.list.lengths()</font>**` | 返回每个列表中的元素数量。 |
| `**<font style="color:#01B2BC;">Expr.list.max()</font>**` | 计算数组中列表的最大值。 |
| `**<font style="color:#01B2BC;">Expr.list.mean()</font>**` | 计算数组中列表的平均值。 |
| `**<font style="color:#01B2BC;">Expr.list.min()</font>**` | 计算数组中列表的最小值。 |
| `**<font style="color:#01B2BC;">Expr.list.reverse()</font>**` | 反转列表中的数组。 |
| `**<font style="color:#01B2BC;">Expr.list.sample(…)</font>**` | 从该列表中采样。 |
| `**<font style="color:#01B2BC;">Expr.list.set_difference(…)</font>**` | 计算该列表中的元素与其他列表中的元素的差集。 |
| `**<font style="color:#01B2BC;">Expr.list.set_intersection(…)</font>**` | 计算该列表中的元素与其他列表中的元素的交集。 |
| `**<font style="color:#01B2BC;">Expr.list.set_symmetric_difference(…)</font>**` | 计算该列表中的元素与其他列表中的元素的对称差集。 |
| `**<font style="color:#01B2BC;">Expr.list.set_union(…)</font>**` | 计算该列表中的元素与其他列表中的元素的并集。 |
| `**<font style="color:#01B2BC;">Expr.list.shift(…)</font>**` | 根据给定的索引数量移位列表值。 |
| `**<font style="color:#01B2BC;">Expr.list.slice(…)</font>**` | 对每个子列表进行切片。 |
| `**<font style="color:#01B2BC;">Expr.list.sort(…)</font>**` | 对该列中的列表进行排序。 |
| `**<font style="color:#01B2BC;">Expr.list.sum()</font>**` | 求数组中所有列表的和。 |
| `**<font style="color:#01B2BC;">Expr.list.tail(…)</font>**` | 对每个子列表切取最后 `n` 个值。 |
| `**<font style="color:#01B2BC;">Expr.list.take(…)</font>**` | 根据多个索引获取子列表。 |
| `**<font style="color:#01B2BC;">Expr.list.to_array(…)</font>**` | 将 `List` 列转换为具有相同内部数据类型的 `Array` 列。 |
| `**<font style="color:#01B2BC;">Expr.list.to_struct(…)</font>**` | 将 `List` 类型的 `Series` 转换为 `Struct` 类型的 `Series`。 |
| `**<font style="color:#01B2BC;">Expr.list.unique(…)</font>**` | 获取列表中的唯一值/不同值。 |


<h3 id="c5ygb">示例</h3>
让我们展示一些你可以与 `List` 类型一起使用的方法。

你可以使用 `all` 和 `any` 方法来判断列表中所有或任意布尔值是否为 `True`：

```python
bool_df = pl.DataFrame({
    "values": [[True, True], [False, False, True], [False]]
})
print(
    bool_df
    .with_columns(
        pl.col("values")
        .list.all()
        .alias("all values"),
        pl.col("values")
        .list.any()
        .alias("any values")
    )
)
```

```plain
shape: (3, 3)
┌──────────────────────┬────────────┬────────────┐
│ values               │ all values │ any values │
│ ---                  │ ---        │ ---        │
│ list[bool]           │ bool       │ bool       │
╞══════════════════════╪════════════╪════════════╡
│ [true, true]         │ true       │ true       │
│ [false, false, true] │ false      │ true       │
│ [false]              │ false      │ false      │
└──────────────────────┴────────────┴────────────┘
```



一个与 `any()` 和 `all()` 方法很好结合的强大方法是 `eval` 方法。此方法允许你对列表的元素运行任何 Polars 表达式。在下面的示例中，我们将使用 `eval` 方法将列表元素乘以 10：

1. `element()` 方法用于访问列表的元素。
2. 由于 `parallel` 参数设置为 `True`，`eval()` 方法将并行运行表达式。默认情况下此选项是关闭的，但如果你运行的表达式支持并行处理，它可以显著加速计算。
3. 为了确保 `with_columns()` 中的并行处理，对新创建列的任何进一步修改都需要单独的 `with_columns()` 调用。
4. `all()` 方法用于判断列表中的所有布尔值是否为 `True`。

你可以使用 `explode` 方法将列表展开为单独的行：

```python
df = pl.DataFrame({
    "values": [[10, 20], [30, 40, 50], [60]]
})
print(
    df
    .with_columns(
        pl.col("values")
        .list.eval(
            pl.element() > 40,  ①
            parallel=True,  ②
        )
        .alias("values > 40")
    )
    .with_columns(  ③
        pl.col("values > 40")
        .list.all()  ④
        .alias("all values > 40")
    )
)
```

```plain
shape: (3, 3)
┌──────────────┬──────────────────────┬─────────────────┐
│ values       │ values > 40          │ all values > 40 │
│ ---          │ ---                  │ ---             │
│ list[i64]    │ list[bool]           │ bool            │
╞══════════════╪══════════════════════╪═════════════════╡
│ [10, 20]     │ [false, false]       │ false           │
│ [30, 40, 50] │ [false, false, true] │ false           │
│ [60]         │ [true]               │ true            │
└──────────────┴──────────────────────┴─────────────────┘
```

①`element()` 方法用于访问列表中的元素。

② 由于 `parallel` 参数设置为 True，`eval()` 方法将并行运行表达式。默认情况下此选项是关闭的，但如果你运行的表达式支持并行处理，它可以显著加速计算。

③ 为了确保 `with_columns()` 中的并行处理，对新创建列的任何进一步修改需要单独的 `with_columns()` 调用。

④`all()` 方法用于判断列表中的所有布尔值是否为 True。



你可以使用 `explode` 方法将列表解包为单独的行：

```python
df.explode("values")
```

```plain
shape: (6, 1)
┌────────┐
│ values │
│ ---    │
│ i64    │
╞════════╡
│ 10     │
│ 20     │
│ 30     │
│ 40     │
│ 50     │
│ 60     │
└────────┘
```



<h2 id="ezYMw">数组(Array)</h2>
`Array` 类型可以包含长度固定且数据类型相同的数组。它类似于 Numpy 的 `ndarray` 类型。`Array` 类型在内存中由 Arrow 的固定大小列表布局（Fixed Size List Layout）实现。对于这种类型，数据缓冲区也是连续的，就像 `List` 一样，但由于长度是固定的，因此不需要偏移缓冲区。这使得这种类型在内存使用和性能上更加高效，因为需要查找的数据较少。

<h3 id="JFgOC">方法</h3>
`Array` 类型有多种方法用于转换、描述和操作数据。

_表 11-10：Array 类型的方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">Expr.arr.max()</font>**` | 计算子数组的最大值。 |
| `**<font style="color:#01B2BC;">Expr.arr.min()</font>**` | 计算子数组的最小值。 |
| `**<font style="color:#01B2BC;">Expr.arr.median()</font>**` | 计算子数组的中位数。 |
| `**<font style="color:#01B2BC;">Expr.arr.sum()</font>**` | 计算子数组的和。 |
| `**<font style="color:#01B2BC;">Expr.arr.std(…)</font>**` | 计算子数组的标准差。 |
| `**<font style="color:#01B2BC;">Expr.arr.to_list()</font>**` | 将 `Array` 列转换为具有相同内部数据类型的 `List` 列。 |
| `**<font style="color:#01B2BC;">Expr.arr.unique(…)</font>**` | 获取数组中的唯一值/不同值。 |
| `**<font style="color:#01B2BC;">Expr.arr.var(…)</font>**` | 计算子数组的方差。 |
| `**<font style="color:#01B2BC;">Expr.arr.all()</font>**` | 判断每个子数组中的所有布尔值是否为 `True`。 |
| `**<font style="color:#01B2BC;">Expr.arr.any()</font>**` | 判断每个子数组中的任意布尔值是否为 `True`。 |
| `**<font style="color:#01B2BC;">Expr.arr.sort(…)</font>**` | 对该列中的数组进行排序。 |
| `**<font style="color:#01B2BC;">Expr.arr.reverse()</font>**` | 反转该列中的数组。 |
| `**<font style="color:#01B2BC;">Expr.arr.arg_min()</font>**` | 获取每个子数组中的最小值的索引。 |
| `**<font style="color:#01B2BC;">Expr.arr.arg_max()</font>**` | 获取每个子数组中的最大值的索引。 |
| `**<font style="color:#01B2BC;">Expr.arr.get(…)</font>**` | 根据子数组中的索引获取值。 |
| `**<font style="color:#01B2BC;">Expr.arr.first()</font>**` | 获取每个子数组的第一个值。 |
| `**<font style="color:#01B2BC;">Expr.arr.last()</font>**` | 获取每个子数组的最后一个值。 |
| `**<font style="color:#01B2BC;">Expr.arr.join(…)</font>**` | 连接子数组中的所有字符串项，并在它们之间放置分隔符。 |
| `**<font style="color:#01B2BC;">Expr.arr.explode()</font>**` | 返回一个列，其中每个数组元素占一行。 |
| `**<font style="color:#01B2BC;">Expr.arr.contains(…)</font>**` | 检查子数组是否包含给定的项。 |
| `**<font style="color:#01B2BC;">Expr.arr.count_matches(…)</font>**` | 计算元素中生成的值出现的次数。 |
| `**<font style="color:#01B2BC;">Expr.arr.to_struct(…)</font>**` | 将 `Array` 类型的 `Series` 转换为 `Struct` 类型的 `Series`。 |
| `**<font style="color:#01B2BC;">Expr.arr.shift(…)</font>**` | 根据给定的索引数量移位数组值。 |


<h3 id="WcJI5">示例</h3>
为了展示 `Array` 类型，创建一个包含 `Array` 列的 DataFrame。在下面的示例中，你将创建一个包含 `Array` 列的 DataFrame，其中的数组由表示不同地点温度的整数组成：

```python
df = pl.DataFrame([
    pl.Series(
        "location",
        ["Paris", "Amsterdam", "Barcelona"],
        dtype=pl.String
    ),
    pl.Series(
        "temperatures",
        [
            [23, 27, 21, 22, 24, 23, 22],
            [17, 19, 15, 22, 18, 20, 21],
            [30, 32, 28, 29, 34, 33, 31]
        ],
        dtype=pl.Array(pl.Int64, width=7),
    ),
])
print(df)
```

```plain
shape: (3, 2)
┌───────────┬────────────────┐
│ location  │ temperatures   │
│ ---       │ ---            │
│ str       │ array[i64, 7]  │
╞═══════════╪════════════════╡
│ Paris     │ [23, 27, … 22] │
│ Amsterdam │ [17, 19, … 21] │
│ Barcelona │ [30, 32, … 31] │
└───────────┴────────────────┘
```



一些适用于数组类型的方法包括 `median`（中位数）、`max`（最大值）和 `arg_max`（最大值的索引）。

```python
print(
    df
    .with_columns(
        pl.col("temperatures")
        .arr.median()
        .alias("median"),
        pl.col("temperatures")
        .arr.max()
        .alias("max"),
        pl.col("temperatures")
        .arr.arg_max()
        .alias("warmest_weekday")
    )
)
```

```plain
shape: (3, 5)
┌───────────┬────────────────┬────────┬─────┬─────────────────┐
│ location  │ temperatures   │ median │ max │ warmest_weekday │
│ ---       │ ---            │ ---    │ --- │ ---             │
│ str       │ array[i64, 7]  │ f64    │ i64 │ u32             │
╞═══════════╪════════════════╪════════╪═════╪═════════════════╡
│ Paris     │ [23, 27, … 22] │ 23.0   │ 27  │ 1               │
│ Amsterdam │ [17, 19, … 21] │ 19.0   │ 22  │ 3               │
│ Barcelona │ [30, 32, … 31] │ 31.0   │ 34  │ 4               │
└───────────┴────────────────┴────────┴─────┴─────────────────┘
```

在生成的 DataFrame 中，你可以看到 `median` 列包含每个地点的中位数温度，`max` 列包含每个地点的最高温度，`warmest_weekday` 列包含每个地点的最高温度对应的星期索引。



<h2 id="Xd4nV">结构体（Structs）</h2>
结构体是一种用于在单列中存储多个列的嵌套类型。在行级别上，它可以看作是一个字典。键是列名，称为字段（fields），值是该行中字段的值。结构体数据类型是 Polars 中处理多列的惯用方式。Polars 基于表达式运行，而根据定义，一个表达式只能接受一列作为输入并输出一列（`Fn(Series) -> Series`）。

通过将多个列封装在结构体中，你仍然可以执行多列操作，同时保持表达式范式的完整性。将多列转换为结构体不会复制数据，而是允许新的结构体类型指向内存中现有的数据缓冲区，从而确保高效的内存使用。

<h3 id="gX2Fk">方法</h3>
`Struct` 类型具有以下方法：

_表 11-11：结构体类型的方法_

| 函数或方法 | 描述 |
| --- | --- |
| `**<font style="color:#01B2BC;">Expr.struct.field(…)</font>**` | 将结构体字段检索为一个新的 `Series`。 |
| `**<font style="color:#01B2BC;">Expr.struct.json_encode()</font>**` | 将此结构体转换为包含 JSON 值的字符串列。 |
| `**<font style="color:#01B2BC;">Expr.struct.rename_fields(…)</font>**` | 重命名结构体的字段。 |


<h3 id="Ci2r0">示例</h3>
要操作结构体，首先需要创建它们。有许多方法可以返回结构体，或者你可以通过使用字典构造一个 DataFrame 来创建它们：

```python
df = pl.DataFrame({
    "struct_column": [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
        {"a": 5, "b": 6},
    ]
})
print(df)
```

```plain
shape: (3, 1)
┌───────────────┐
│ struct_column │
│ ---           │
│ struct[2]     │
╞═══════════════╡
│ {1,2}         │
│ {3,4}         │
│ {5,6}         │
└───────────────┘
```



你可以使用 `field` 方法从结构体中检索值：

```python
df.select(pl.col("struct_column").struct.field("a"))
```

```plain
shape: (3, 1)
┌─────┐
│ a   │
│ --- │
│ i64 │
╞═════╡
│ 1   │
│ 3   │
│ 5   │
└─────┘
```



要返回多个列，可以使用 `unnest` 方法。请注意，`unnest` 方法不属于结构体命名空间，而应在 DataFrame/LazyFrame/Series 级别调用。

```python
df = df.unnest("struct_column")
print(df)
```

```plain
shape: (3, 2)
┌─────┬─────┐
│ a   │ b   │
│ --- │ --- │
│ i64 │ i64 │
╞═════╪═════╡
│ 1   │ 2   │
│ 3   │ 4   │
│ 5   │ 6   │
└─────┴─────┘
```



如果你想做相反的操作，将多个列组合起来，可以将它们转换为结构体：

```python
df.select(
    "a",
    "b",
    pl.struct(
        pl.col("a"),
        pl.col("b")
    ).alias("struct_column"),
)
```

```plain
shape: (3, 3)
┌─────┬─────┬───────────────┐
│ a   │ b   │ struct_column │
│ --- │ --- │ ---           │
│ i64 │ i64 │ struct[2]     │
╞═════╪═════╪═══════════════╡
│ 1   │ 2   │ {1,2}         │
│ 3   │ 4   │ {3,4}         │
│ 5   │ 6   │ {5,6}         │
└─────┴─────┴───────────────┘
```



一个常见的返回结构体的函数是 `value_counts()`。此函数用于统计列中唯一值的出现次数。由于表达式只能返回一个 `Series`，`value_counts()` 返回一个带有两个字段的结构体列：被统计的原始列名和计数。

首先，创建一个包含结构体列的 DataFrame：

```python
df = pl.DataFrame({
    "fruit": ["cherry", "apple", "banana", "banana", "apple", "banana"],
})
print(df)
```

```plain
shape: (6, 1)
┌────────┐
│ fruit  │
│ ---    │
│ str    │
╞════════╡
│ cherry │
│ apple  │
│ banana │
│ banana │
│ apple  │
│ banana │
└────────┘
```



你可以使用 `value_counts()` 计算 `fruit` 列中每个唯一元素的出现次数：

```python
print(
    df
    .select(
        pl.col("fruit")
        .value_counts(sort=True)
    )
)
```

```plain
shape: (3, 1)
┌──────────────┐
│ fruit        │
│ ---          │
│ struct[2]    │
╞══════════════╡
│ {"banana",3} │
│ {"apple",2}  │
│ {"cherry",1} │
└──────────────┘
```



在生成的 DataFrame 中，你可以看到 `value_counts` 方法被调用时，`sort` 参数设置为 `True`，这意味着值将按计数降序排列。

然后你可以将这些结构体展开为独立的列：

```python
print(
    df.select(
        pl.col("fruit")
        .value_counts(sort=True)
    )
    .unnest("fruit")
)
```

```plain
shape: (3, 2)
┌────────┬───────┐
│ fruit  │ count │
│ ---    │ ---   │
│ str    │ u32   │
╞════════╪═══════╡
│ banana │ 3     │
│ apple  │ 2     │
│ cherry │ 1     │
└────────┴───────┘
```

在生成的 DataFrame 中，你可以看到 `value_counts` 方法已被展开为独立的列。

<h2 id="L7OVS">本章小结</h2>
本章介绍了 Polars 中具有自己命名空间的特殊数据类型以及如何使用它们。你学习了：

+ 字符串类型，重点是通过优化的内存布局来优化可变长度。
+ `Categoricals` 和 `Enums` 的内存高效方式，用于处理重复的字符串数据。
+ 如何使用时间数据类型 `Date`、`DateTime`、`Time` 和 `Duration` 处理基于时间的信息挑战。
+ 如何使用嵌套数据类型（如 `List`、`Array` 和 `Struct`）在单列中存储数据点集合。

通过这些数据类型，你可以使用 Polars 提供的丰富方法来处理各种类型的数据。在下一章中，你将学习如何汇总和聚合数据。

