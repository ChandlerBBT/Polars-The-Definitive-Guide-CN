

现在你已经了解了一些基本概念，例如数据类型和不同的 API，是时候学习如何与外部数据源交互了。这包括从文件和数据库中读取数据到 Polars 中。我们还将介绍如何将结果写入文件和数据库。在本章结束时，你将能够开始使用自己的数据进行操作。我们鼓励你尽快使用自己的数据，因为这不仅会使学习 Polars 变得更加有趣，还会更加高效。

由于外部数据可能来自各种途径，Polars 拥有超过30个与读取数据相关的函数，这些函数接受许多参数。在本章中全面涵盖每个函数和每个参数是非常具有挑战性且乏味的，所以这正是[官方 API 文档](https://docs.pola.rs/py-polars/html/reference/)的作用所在。相反，我们将重点关注最有可能遇到的格式和情况。

在本章中，你将学习如何：

+ 读取和写入多种格式的数据，包括 CSV、Excel 和 Parquet
+ 使用通配符高效处理多个文件
+ 正确读取缺失值
+ 处理字符编码
+ 急切和惰性地读取数据

我们将使用几个额外的包：

+ `xlsx2csv`：用于读取 Excel 电子表格
+ `chardet`：用于确定文件的字符编码
+ `connectorx`：用于连接数据库
+ `pyarrow`：用于读取 PyArrow 数据集

第二章介绍了如何安装这些包。

为了演示如何处理各种数据格式，本章使用了许多数据集。获取相应文件的说明在第二章中。我们假设你将文件保存在 `data` 子目录中。

和往常一样，我们先导入 Polars：

```python
import polars as pl
```



<h2 id="bZI2K">读取 CSV 文件</h2>
我们将从逗号分隔值（CSV）文件格式开始，这可能是编程、数据分析和科学研究中最常见的文件格式。尽管它非常普遍，但并不是没有缺陷。当你拿到一个 `.csv` 扩展名的文件时，你并不知道里面是什么：

+ 分隔符是逗号、制表符、分号还是其他符号？
+ 字符编码是 UTF-8、ASCII 还是其他？
+ 是否有包含列名的标题行？有几行？
+ 缺失值如何表示？
+ 值是否被正确引用？

Polars 可以处理所有这些情况，但有时可能需要反复尝试。

设想一下，我们有一个简单的 CSV 文件，比如 `data/penguins.csv`。在我们立即开始将数据加载到 Polars 之前，让我们先使用命令行工具 `cat` 来查看文件的原始内容（请注意输出已被截断）：

```bash
$ cat data/penguins.csv
```

```plain
"rowid","species","island","bill_length_mm","bill_depth_mm","flipper_length_mm","body_mass_g","sex","year"
"1","Adelie","Torgersen",39.1,18.7,181,3750,"male",2007
"2","Adelie","Torgersen",39.5,17.4,186,3800,"female",2007
"3","Adelie","Torgersen",40.3,18,195,3250,"female",2007
"4","Adelie","Torgersen",NA,NA,NA,NA,NA,2007
... 剩余340行
```

乍一看，这个 CSV 文件看起来确实很简单。第一行是标题，分隔符是逗号，这与 Polars 的默认值匹配。此外，字符编码兼容 UTF-8（稍后会详细介绍）。这使我们有足够的信心将数据集读取到 Polars 的 DataFrame 中：

```python
penguins = pl.read_csv("data/penguins.csv")
penguins
```

```plain
shape: (344, 9)
┌───────┬───────────┬───────────┬───┬─────────────┬────────┬──────┐
│ rowid │ species   │ island    │ … │ body_mass_g │ sex    │ year │
│ ---   │ ---       │ ---       │   │ ---         │ ---    │ ---  │
│ i64   │ str       │ str       │   │ str         │ str    │ i64  │
╞═══════╪═══════════╪═══════════╪═══╪═════════════╪════════╪══════╡
│ 1     │ Adelie    │ Torgersen │ … │ 3750        │ male   │ 2007 │
│ 2     │ Adelie    │ Torgersen │ … │ 3800        │ female │ 2007 │
│ 3     │ Adelie    │ Torgersen │ … │ 3250        │ female │ 2007 │
│ 4     │ Adelie    │ Torgersen │ … │ NA          │ NA     │ 2007 │
│ 5     │ Adelie    │ Torgersen │ … │ 3450        │ female │ 2007 │
│ …     │ …         │ …         │ … │ …           │ …      │ …    │
│ 340   │ Chinstrap │ Dream     │ … │ 4000        │ male   │ 2009 │
│ 341   │ Chinstrap │ Dream     │ … │ 3400        │ female │ 2009 │
│ 342   │ Chinstrap │ Dream     │ … │ 3775        │ male   │ 2009 │
│ 343   │ Chinstrap │ Dream     │ … │ 4100        │ male   │ 2009 │
│ 344   │ Chinstrap │ Dream     │ … │ 3775        │ female │ 2009 │
└───────┴───────────┴───────────┴───┴─────────────┴────────┴──────┘
```



看起来这个 CSV 文件已经被正确读取了，除了一个问题：“NA” 值没有被识别为缺失值。我们将在下一节中修复这个问题。

如果你的 CSV 文件不同，或许可以参考表 5-1 中列出的参数来帮助你解决问题。

**表 5-1. **`pl.read_csv()`** 函数的常用参数**

| 参数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">source</font>**` | 文件或类文件对象的路径。 |
| `**<font style="color:#1DC0C9;">has_header</font>**` | 指示数据集的第一行是否为标题。 |
| `**<font style="color:#1DC0C9;">columns</font>**` | 选择的列。接受列索引列表（从 0 开始）或列名称列表。 |
| `**<font style="color:#1DC0C9;">separator</font>**` | 用作文件中分隔符的单字节字符。 |
| `**<font style="color:#1DC0C9;">skip_rows</font>**` | 在读取数据之前跳过的行数。 |
| `**<font style="color:#1DC0C9;">null_values</font>**` | 作为空值处理的值。 |
| `**<font style="color:#1DC0C9;">encoding</font>**` | 默认：`utf8`。`utf8-lossy` 表示无效的 UTF-8 值将用 � 字符替换。在使用其他编码时，输入首先在内存中用 Python 解码。 |




<h2 id="VsF7A">正确解析缺失值</h2>
数据集中有缺失值是很常见的。对于像 CSV 这样的纯文本格式来说，没有标准的方法来表示这些缺失值。我们常见的缺失值表示方式包括 NULL、Nil、None、NA、N/A、NaN、999999，甚至是空字符串。

默认情况下，Polars 只将空字符串解释为缺失值。任何其他表示方式都需要通过字符串（或字符串列表）明确传递给 `null_values` 参数。让我们来修复 `data/penguins.csv` 中的这些缺失值：

```python
penguins = pl.read_csv("data/penguins.csv", null_values="NA")
penguins
```

```plain
shape: (344, 9)
┌───────┬───────────┬───────────┬───┬─────────────┬────────┬──────┐
│ rowid │ species   │ island    │ … │ body_mass_g │ sex    │ year │
│ ---   │ ---       │ ---       │   │ ---         │ ---    │ ---  │
│ i64   │ str       │ str       │   │ i64         │ str    │ i64  │
╞═══════╪═══════════╪═══════════╪═══╪═════════════╪════════╪══════╡
│ 1     │ Adelie    │ Torgersen │ … │ 3750        │ male   │ 2007 │
│ 2     │ Adelie    │ Torgersen │ … │ 3800        │ female │ 2007 │
│ 3     │ Adelie    │ Torgersen │ … │ 3250        │ female │ 2007 │
│ 4     │ Adelie    │ Torgersen │ … │ null        │ null   │ 2007 │
│ 5     │ Adelie    │ Torgersen │ … │ 3450        │ female │ 2007 │
│ …     │ …         │ …         │ … │ …           │ …      │ …    │
│ 340   │ Chinstrap │ Dream     │ … │ 4000        │ male   │ 2009 │
│ 341   │ Chinstrap │ Dream     │ … │ 3400        │ female │ 2009 │
│ 342   │ Chinstrap │ Dream     │ … │ 3775        │ male   │ 2009 │
│ 343   │ Chinstrap │ Dream     │ … │ 4100        │ male   │ 2009 │
│ 344   │ Chinstrap │ Dream     │ … │ 3775        │ female │ 2009 │
└───────┴───────────┴───────────┴───┴─────────────┴────────┴──────┘
```



---

**<font style="color:#1DC0C9;">注意</font>**

当 DataFrames 以 ASCII 格式呈现时（例如在本书中），所有字符串都将不带引号显示。这意味着你无法通过视觉检查来确认缺失值是否被正确解释。

当你使用 Jupyter Notebook 时，你将获得 DataFrame 的 HTML 渲染。在这里，缺失值显示为没有引号的 "null"，而常规字符串则带有引号显示。

---

如果你不确定所有缺失值是否被正确解析，可以通过 `null_count()` 方法以编程方式对它们进行计数：

```python
(
    penguins
    .null_count()
    .transpose(include_header=True, column_names=["null_count"])
)
```

结果表明：

```plain
shape: (9, 2)
┌───────────────────┬────────────┐
│ column            │ null_count │
│ ---               │ ---        │
│ str               │ u32        │
╞═══════════════════╪════════════╡
│ rowid             │ 0          │
│ species           │ 0          │
│ island            │ 0          │
│ bill_length_mm    │ 2          │
│ bill_depth_mm     │ 2          │
│ flipper_length_mm │ 2          │
│ body_mass_g       │ 2          │
│ sex               │ 11         │
│ year              │ 0          │
└───────────────────┴────────────┘
```

① 我们转置了输出，以便更好地概览所有计数。



<h2 id="gCUeN">**读取非 UTF-8 编码的文件**</h2>
每个文本文件都有特定的**字符编码**。字符编码是一种将集合中的各个字符分配唯一代码的系统，从而使得这些字符能够被计算机表示和处理。

Polars 假设 CSV 文件是以 UTF-8 编码的，UTF-8 是一种广泛使用的编码标准。UTF-8 能够表示 Unicode 标准中的任何字符，涵盖了多种语言的字符，包括现代和历史语言，以及各种符号。

如果你尝试读取一个不是 UTF-8 编码的 CSV 文件，你可能会遇到错误，就像我们在读取 `data/directors.csv` 时看到的那样：

```python
pl.read_csv("data/directors.csv")
```

```plain
ComputeError: could not parse `����` as dtype `str` at column 'name' (column num
ber 1)

The current offset in the file is 19 bytes.

You might want to try:
- increasing `infer_schema_length` (e.g. `infer_schema_length=10000`),
- specifying correct dtype with the `dtypes` argument
- setting `ignore_errors` to `True`,
- adding `����` to the `null_values` list.

Original error: ```invalid utf-8 sequence```
```

显然，`data/directors.csv` 并不是 UTF-8 编码的。

如果你开始猜测文件的编码，你可能会用错编码，虽然这不会让 Polars 报错，但文件中的字节可能还是会被错误地解释。如果你不熟悉该语言，可能很难发现问题。

现在假设你被告知文件包含一些亚洲名字。你猜测文件可能包含一些中文名字，最好的猜测是尝试使用中文字符常见的编码：

```python
pl.read_csv("data/directors.csv", encoding="EUC-CN")
```

```plain
shape: (4, 3)
┌───────────┬──────┬──────────┐
│ name      │ born │ country  │
│ ---       │ ---  │ ---      │
│ str       │ i64  │ str      │
╞═══════════╪══════╪══════════╡
│ 考侯      │ 1930 │ 泣塑     │
│ Verhoeven │ 1938 │ オランダ │
│ 弟宏      │ 1942 │ 泣塑     │
│ Tarantino │ 1963 │ 势柜     │
└───────────┴──────┴──────────┘
```



这成功了吗？还是没有？当你通过翻译（例如，使用你喜欢的搜索引擎）将第一个国家从中文翻译成英文时，它显示为“哭泣的塑料”。什么？我们从未听说过这样的国家！

与其猜测编码，不如让 **chardet** 包来检测。下面的函数返回给定文件的编码。让我们将这个函数应用于我们的 CSV 文件：

```python
import chardet

def detect_encoding(filename: str) -> str:
    """返回文件的最可能的字符编码。"""
    
    with open(filename, "rb") as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        return result["encoding"]

detect_encoding("data/directors.csv")
```

```plain
'EUC-JP'
```

于是，**chardet** 检测到了一种不同的编码——一种常用于日文字符的编码。让我们试试用 Polars 读取文件并使用 "EUC-JP" 编码：

```python
pl.read_csv("data/directors.csv", encoding="EUC-JP")
```

```plain
shape: (4, 3)
┌───────────┬──────┬──────────┐
│ name      │ born │ country  │
│ ---       │ ---  │ ---      │
│ str       │ i64  │ str      │
╞═══════════╪══════╪══════════╡
│ 深作      │ 1930 │ 日本     │
│ Verhoeven │ 1938 │ オランダ │
│ 宮崎      │ 1942 │ 日本     │
│ Tarantino │ 1963 │ 米国     │
└───────────┴──────┴──────────┘
```

现在这是正确的。相信我们，我们检查过了。

结论：你最好不要猜测文件的编码。这不仅适用于CSV文件，也适用于所有基于文本的文件，包括JSON、XML和HTML。

<h2 id="TQ5XK">**读取Excel电子表格**</h2>
尽管CSV在数据密集型、编程和分析场景中很常见，但Excel电子表格在商业环境中更为常见，这些场景通常涉及手动数据检查、数据输入和基本分析。

Excel文件可以包含复杂的数据、标记、公式和图表。虽然这些特性在商业应用中很有用，但它们会妨碍将电子表格读取到Polars中。理想情况下，电子表格应仅包含像CSV文件一样的矩形数据。

要将Excel电子表格读取为DataFrame，Polars使用**xlsx2csv**包。（如何安装此包的说明见第2章。）让我们读取文件_data/top2000-2023.xlsx_，这是荷兰年度广播节目Top2000的电子表格。它包含2023年该广播站听众投票的最受欢迎的2000首歌曲。

```python
songs = pl.read_excel("data/top2000-2023.xlsx")
songs
```

```plain
shape: (2_001, 4)
| positie | titel                 | artiest        | jaar |
| ---     | ---                   | ---            | ---  |
| i64     | str                   | str            | i64  |
|---------|-----------------------|----------------|------|
| null    | null                  | null           | null |
| 1       | Bohemian Rhapsody     | Queen          | 1975 |
| 2       | Roller Coaster        | Danny Vera     | 2019 |
| 3       | Hotel California      | Eagles         | 1977 |
| 4       | Piano Man             | Billy Joel     | 1974 |
| …       | …                     | …              | …    |
| 1996    | Charlie Brown         | Coldplay       | 2011 |
| 1997    | Beast Of Burden       | Bette Midler   | 1984 |
| 1998    | It Was A Very Good Y… | Frank Sinatra  | 1968 |
| 1999    | Hou Van Mij           | 3JS            | 2008 |
| 2000    | Drivers License       | Olivia Rodrigo | 2021 |
```

① 荷兰语列名翻译为位置（position）、标题（title）、艺术家（artist）和年份（year）。（趣味小知识：荷兰语是继弗里斯语之后与英语最接近的亲属语言。）



我们这个电子表格只有一个小缺陷：表头跨越了两行。（注意，第一行只有缺失值。）可以通过以下方式修复：

```python
songs_fixed = pl.read_excel(
    "data/top2000-2023.xlsx", read_options={"skip_rows_after_header": 1}
)
```

```plain
shape: (2_000, 4)
┌─────────┬───────────────────────┬────────────────┬──────┐
│ positie │ titel                 │ artiest        │ jaar │
│ ---     │ ---                   │ ---            │ ---  │
│ i64     │ str                   │ str            │ i64  │
╞═════════╪═══════════════════════╪════════════════╪══════╡
│ 1       │ Bohemian Rhapsody     │ Queen          │ 1975 │
│ 2       │ Roller Coaster        │ Danny Vera     │ 2019 │
│ 3       │ Hotel California      │ Eagles         │ 1977 │
│ 4       │ Piano Man             │ Billy Joel     │ 1974 │
│ 5       │ Fix You               │ Coldplay       │ 2005 │
│ …       │ …                     │ …              │ …    │
│ 1996    │ Charlie Brown         │ Coldplay       │ 2011 │
│ 1997    │ Beast Of Burden       │ Bette Midler   │ 1984 │
│ 1998    │ It Was A Very Good Y… │ Frank Sinatra  │ 1968 │
│ 1999    │ Hou Van Mij           │ 3JS            │ 2008 │
│ 2000    │ Drivers License       │ Olivia Rodrigo │ 2021 │
└─────────┴───────────────────────┴────────────────┴──────┘
```



我们传递给 `pl.read_excel()` 的附加参数是一个字典，其中的参数将传递给 `pl.read_csv()`。这是因为在底层，Excel电子表格首先会转换为CSV文件。表5-2列出了其他常用的参数。

表5-2. `pl.read_excel()`函数的常用参数

| 参数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">source</font>**` | 文件路径或类文件对象。 |
| `**<font style="color:#1DC0C9;">sheet_id</font>**` | 要转换的工作表编号（0表示所有工作表）。如果未指定 `sheet_name` 或 `sheet_id`，则默认值为1。 |
| `**<font style="color:#1DC0C9;">sheet_name</font>**` | 要转换的工作表名称。不能与 `sheet_id` 一起使用。 |
| `**<font style="color:#1DC0C9;">xlsx2csv_options</font>**` | 传递给 `xlsx2csv.Xlsx2csv()` 的额外选项，例如：`{"skip_empty_lines": True}` |
| `**<font style="color:#1DC0C9;">read_csv_options</font>**` | 传递给 `pl.read_csv()` 的额外选项，用于解析 `xlsx2csv.Xlsx2csv().convert()` 返回的CSV文件。 |


Polars只支持带有 `.xlsx` 扩展名的Excel电子表格。如果发现 `pl.read_excel()` 无法处理你的电子表格文件，建议你尝试使用Pandas的函数 `pd.read_excel()`。除了 `.xlsx`，该函数还支持 `.xls`、`.xlsm`、`.xlsb`、`.odf`、`.ods` 和 `.odt` 等格式。在本章后面的 “其他文件格式” 部分，我们将解释如何将Pandas的DataFrame转换为Polars的DataFrame。



<h2 id="Bzs1B">处理多个文件</h2>
如果你的数据分布在多个文件中，并且这些文件都具有相同的格式和架构，你可能能够一次性读取所有文件。

例如，让我们考虑三家公司：ASML Holding N.V. (ASML)、NVIDIA Corporation (NVDA) 和台湾半导体制造公司 (TSMC) 的每日股票信息。数据分布在多个CSV文件中，每家公司每年一个文件。文件命名遵循以下模式：`data/stock/<symbol>_<year>.csv`。例如：`data/stock/nvda/2010.csv` 和 `data/stock/asml/2022.csv`。

由于这些文件具有相同的格式和架构，我们可以使用 **通配符模式**（_globbing pattern_）。通配符模式可以包含特殊字符，例如星号 (`*`)、问号 (`?`) 或方括号 (`[]`)，它们充当通配符。星号匹配字符串中的零个或多个字符，而问号匹配正好一个字符。例如，模式 `*.csv` 将匹配以 `.csv` 结尾的任何文件，模式 `file?.csv` 将匹配 `file1.csv` 或 `fileA.csv`，但不会匹配 `file12.csv`。要匹配特定集合或范围内的某个字符，你可以使用方括号。例如，`file-[a,b].csv` 匹配 `file-a.csv` 和 `file-b.csv`。模式 `file-[0-9].csv` 匹配 `file-0.csv`、`file-1.csv`、`file-2.csv` 直到 `file-9.csv`。

要读取2010年到2019年间的NVIDIA股票数据，可以使用以下模式：

```python
pl.read_csv("data/stock/nvda/201[0-9].csv")
```

```plain
shape: (2_516, 8)
┌────────┬────────────┬───────────┬───┬───────────┬───────────┬──────────┐
│ symbol │ date       │ open      │ … │ close     │ adj close │ volume   │
│ ---    │ ---        │ ---       │   │ ---       │ ---       │ ---      │
│ str    │ str        │ f64       │   │ f64       │ f64       │ i64      │
╞════════╪════════════╪═══════════╪═══╪═══════════╪═══════════╪══════════╡
│ NVDA   │ 2010-01-04 │ 4.6275    │ … │ 4.6225    │ 4.24115   │ 80020400 │
│ NVDA   │ 2010-01-05 │ 4.605     │ … │ 4.69      │ 4.303082  │ 72864800 │
│ NVDA   │ 2010-01-06 │ 4.6875    │ … │ 4.72      │ 4.330608  │ 64916800 │
│ NVDA   │ 2010-01-07 │ 4.695     │ … │ 4.6275    │ 4.245738  │ 54779200 │
│ NVDA   │ 2010-01-08 │ 4.59      │ … │ 4.6375    │ 4.254913  │ 47816800 │
│ …      │ …          │ …         │ … │ …         │ …         │ …        │
│ NVDA   │ 2019-12-24 │ 59.549999 │ … │ 59.654999 │ 59.432919 │ 13886400 │
│ NVDA   │ 2019-12-26 │ 59.689999 │ … │ 59.797501 │ 59.574883 │ 18285200 │
│ NVDA   │ 2019-12-27 │ 59.950001 │ … │ 59.217499 │ 58.997044 │ 25464400 │
│ NVDA   │ 2019-12-30 │ 58.997501 │ … │ 58.080002 │ 57.863789 │ 25805600 │
│ NVDA   │ 2019-12-31 │ 57.724998 │ … │ 58.825001 │ 58.606007 │ 23100400 │
└────────┴────────────┴───────────┴───┴───────────┴───────────┴──────────┘
```



要读取`data/stock`目录下的所有CSV文件，使用两个星号（`**`），因为它们位于不同的子目录中：

```python
all_stocks = pl.read_csv("data/stock/**/*.csv")
all_stocks
```

```plain
shape: (18_476, 8)
┌────────┬────────────┬────────────┬───┬────────────┬────────────┬──────────┐
│ symbol │ date       │ open       │ … │ close      │ adj close  │ volume   │
│ ---    │ ---        │ ---        │   │ ---        │ ---        │ ---      │
│ str    │ str        │ f64        │   │ f64        │ f64        │ i64      │
╞════════╪════════════╪════════════╪═══╪════════════╪════════════╪══════════╡
│ ASML   │ 1999-01-04 │ 11.765625  │ … │ 12.140625  │ 7.5722     │ 1801867  │
│ ASML   │ 1999-01-05 │ 11.859375  │ … │ 13.96875   │ 8.712416   │ 8241600  │
│ ASML   │ 1999-01-06 │ 14.25      │ … │ 16.875     │ 10.525064  │ 16400267 │
│ ASML   │ 1999-01-07 │ 14.742188  │ … │ 16.851563  │ 10.510445  │ 17722133 │
│ ASML   │ 1999-01-08 │ 16.078125  │ … │ 15.796875  │ 9.852628   │ 10696000 │
│ …      │ …          │ …          │ … │ …          │ …          │ …        │
│ TSM    │ 2023-06-26 │ 102.019997 │ … │ 100.110001 │ 100.110001 │ 8560000  │
│ TSM    │ 2023-06-27 │ 101.150002 │ … │ 102.080002 │ 102.080002 │ 9732000  │
│ TSM    │ 2023-06-28 │ 100.5      │ … │ 100.919998 │ 100.919998 │ 8160900  │
│ TSM    │ 2023-06-29 │ 101.339996 │ … │ 100.639999 │ 100.639999 │ 7383900  │
│ TSM    │ 2023-06-30 │ 101.400002 │ … │ 100.919998 │ 100.919998 │ 11701700 │
└────────┴────────────┴────────────┴───┴────────────┴────────────┴──────────┘
```



如果无法通过模式匹配表达要读取的文件，您可以使用手动方法：

1. 构造一个文件名列表以读取。
2. 使用适当的Polars函数读取这些文件（例如，`pl.read_csv()`）。
3. 使用`pl.concat()`函数合并Polars数据框。

以下是一个示例，我们从闰年读取所有ASML的股票数据：

```python
import calendar

filenames = [
    f"data/stock/asml/{year}.csv"
    for year in range(1999, 2024)
    if calendar.isleap(year)
]

filenames
```

输出的文件名列表：

```plain
['data/stock/asml/2000.csv',
 'data/stock/asml/2004.csv',
 'data/stock/asml/2008.csv',
 'data/stock/asml/2012.csv',
 'data/stock/asml/2016.csv',
 'data/stock/asml/2020.csv']
```

---

**将所有文件的内容合并：**

```python
pl.concat(pl.read_csv(f) for f in filenames)
```

```plain
shape: (1_512, 8)
┌────────┬────────────┬────────────┬───┬────────────┬────────────┬─────────┐
│ symbol │ date       │ open       │ … │ close      │ adj close  │ volume  │
│ ---    │ ---        │ ---        │   │ ---        │ ---        │ ---     │
│ str    │ str        │ f64        │   │ f64        │ f64        │ i64     │
╞════════╪════════════╪════════════╪═══╪════════════╪════════════╪═════════╡
│ ASML   │ 2000-01-03 │ 43.875     │ … │ 43.640625  │ 27.218985  │ 1121600 │
│ ASML   │ 2000-01-04 │ 41.953125  │ … │ 40.734375  │ 25.406338  │ 968800  │
│ ASML   │ 2000-01-05 │ 39.28125   │ … │ 39.609375  │ 24.704666  │ 1458133 │
│ ASML   │ 2000-01-06 │ 36.75      │ … │ 37.171875  │ 23.184378  │ 3517867 │
│ ASML   │ 2000-01-07 │ 36.867188  │ … │ 38.015625  │ 23.710632  │ 1631200 │
│ …      │ …          │ …          │ … │ …          │ …          │ …       │
│ ASML   │ 2020-12-24 │ 478.950012 │ … │ 483.089996 │ 471.932404 │ 271900  │
│ ASML   │ 2020-12-28 │ 487.140015 │ … │ 480.23999  │ 469.148193 │ 449300  │
│ ASML   │ 2020-12-29 │ 489.450012 │ … │ 484.01001  │ 472.831177 │ 377200  │
│ ASML   │ 2020-12-30 │ 488.130005 │ … │ 489.910004 │ 478.594879 │ 381900  │
│ ASML   │ 2020-12-31 │ 490.0      │ … │ 487.720001 │ 476.455444 │ 312700  │
└────────┴────────────┴────────────┴───┴────────────┴────────────┴─────────┘
```



<h2 id="T7Q9c">**读取Parquet**</h2>
Parquet格式是一种列式存储文件格式，经过优化，适用于大数据处理框架，如Apache Spark、Apache Hive，当然还有Polars。它提供了高效的压缩和编码方案，提高了性能并减少了存储空间。

与CSV和Excel等行式格式相比，Parquet在读取和写入大规模数据集时更加高效，特别是在查询特定列时。此外，Parquet支持复杂的嵌套数据结构，而CSV和Excel通常是平面的，这使得Parquet成为处理复杂数据集的更通用选择。

Parquet文件还包含数据的模式(schema)，消除了我们在读取CSV文件时遇到的那种错误。

以下是一个使用纽约市黄出租车出行数据的示例：

```python
trips = pl.read_parquet("data/taxi/yellow_tripdata_*.parquet")
trips
```

```plain
shape: (39_656_098, 19)
┌──────────┬──────────────────────┬───┬──────────────────────┬─────────────┐
│ VendorID │ tpep_pickup_datetime │ … │ congestion_surcharge │ airport_fee │
│ ---      │ ---                  │   │ ---                  │ ---         │
│ i64      │ datetime[ns]         │   │ f64                  │ f64         │
╞══════════╪══════════════════════╪═══╪══════════════════════╪═════════════╡
│ 1        │ 2022-01-01 00:35:40  │ … │ 2.5                  │ 0.0         │
│ 1        │ 2022-01-01 00:33:43  │ … │ 0.0                  │ 0.0         │
│ 2        │ 2022-01-01 00:53:21  │ … │ 0.0                  │ 0.0         │
│ 2        │ 2022-01-01 00:25:21  │ … │ 2.5                  │ 0.0         │
│ 2        │ 2022-01-01 00:36:48  │ … │ 2.5                  │ 0.0         │
│ …        │ …                    │ … │ …                    │ …           │
│ 2        │ 2022-12-31 23:46:00  │ … │ null                 │ null        │
│ 2        │ 2022-12-31 23:13:24  │ … │ null                 │ null        │
│ 2        │ 2022-12-31 23:00:49  │ … │ null                 │ null        │
│ 1        │ 2022-12-31 23:02:50  │ … │ null                 │ null        │
│ 2        │ 2022-12-31 23:00:15  │ … │ null                 │ null        │
└──────────┴──────────────────────┴───┴──────────────────────┴─────────────┘
```



---

**<font style="color:#1DC0C9;">提示</font>**  
在我们的普通笔记本电脑上，使用`pl.read_parquet()`读取近4000万行数据仅需大约5秒。

---

**表5-3**列出了一些读取Parquet文件的常用参数。



_表5-3：_`_pl.read_parquet()_`_函数的常用参数。_

| 参数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">source</font>**` | 文件路径或类似文件的对象。如果路径是目录，则该目录中的所有文件都会被读取。如果安装了`fsspec`，则它将用于打开远程文件。 |
| `**<font style="color:#1DC0C9;">columns</font>**` | 要选择的列。接受列索引列表（从零开始）或列名称列表。 |
| `**<font style="color:#1DC0C9;">n_rows</font>**` | 在读取`n_rows`行后停止读取Parquet文件。仅在`use_pyarrow=False`时有效。 |
| `**<font style="color:#1DC0C9;">use_pyarrow</font>**` | 使用`pyarrow`而不是Rust原生的Parquet读取器。`pyarrow`读取器更稳定（默认值：`False`）。 |


Parquet的速度和稳定性使其在处理数据框时成为我们认为的最佳文件格式。在本书的其余部分，你会更多地看到它的使用。



<h2 id="FT8on">**读取JSON和NDJSON**</h2>
在本节中，我们讨论如何读取JavaScript对象表示法（JSON）及其近亲换行分隔JSON（NDJSON）。

---

<h3 id="GMkMA">**JSON**</h3>
JSON是一种易于人类阅读和书写的文本格式，同时也易于机器解析和生成。与CSV和Excel不同，JSON可以包含嵌套的数据结构。这种灵活性使它成为API、NoSQL数据库和配置文件的流行选择。

让我们使用命令行工具`cat`查看`data/pokedex.json`的原始内容：

```bash
$ cat data/pokedex.json
```

```plain
{
  "pokemon": [{
    "id": 1,
    "num": "001",
    "name": "Bulbasaur",
    "img": "http://www.serebii.net/pokemongo/pokemon/001.png",
    "type": [
      "Grass",
      "Poison"
    ],
    "height": "0.71 m",
    "weight": "6.9 kg",
    "candy": "Bulbasaur Candy",
    "candy_count": 25,
    "egg": "2 km",
    "spawn_chance": 0.69,
    "avg_spawns": 69,
    "spawn_time": "20:00",
    "multipliers": [1.58],
    "weaknesses": [
      "Fire",
      "Ice",
      "Flying",
      "Psychic"
    ],
    "next_evolution": [{
      "num": "002",
      "name": "Ivysaur"
    }, {
      "num": "003",
      "name": "Venusaur"
    }]
  }, {
… with 4053 more lines
```



这个JSON文件以花括号开始和结束，意味着整个文件是一个JSON对象。这些花括号正是允许JSON具有高度嵌套结构的原因。

该对象有一个键`pokemon`，其包含一个对象列表。前33行还显示了第一个Pokemon对象，即Bulbasaur。这个对象本身也包含一些包含其他对象的键。同样，这种灵活性有很多优点，但如我们接下来将看到的，它在使用Polars读取时也带来了一些挑战。

让我们看看当我们将这个JSON文件读取到Polars数据框时会发生什么：

```python
pokedex = pl.read_json("data/pokedex.json")
pokedex
```

```plain
shape: (1, 1)
┌──────────────────────────────────────────────────────────────────────────────┐
│ pokemon                                                                      │
│ ---                                                                          │
│ list[struct[17]]                                                             │
╞══════════════════════════════════════════════════════════════════════════════╡
│ [{1,"001","Bulbasaur","http://www.serebii.net/pokemongo/pokemon/001.png",["G │
│ rass", "Poison"],"0.71 m","6.9 kg","Bulbasaur Candy","2 km",0.69,69.0,"20:0… │
└──────────────────────────────────────────────────────────────────────────────┘
```



请注意，所有内容都被读取为一个单一的值？这是因为JSON对象只有一个名为`pokemon`的键，其值是一个对象列表。Polars并没有对如何将嵌套结构展平为矩形形状做出任何假设。

幸运的是，Polars 提供了两种手动展平数据的方法：`df.explode()`，用于将列表中的每个项目转换为新的一行；`df.unnest()`，用于将对象中的每个键转换为新列。现在，让我们对Pokedex进行一定程度的展平：

```python
(
    pokedex.explode("pokemon")
    .unnest("pokemon")
    .select("id", "name", "type", "height", "weight")
)
```

```plain
shape: (151, 5)
┌─────┬────────────┬──────────────────────┬────────┬──────────┐
│ id  │ name       │ type                 │ height │ weight   │
│ --- │ ---        │ ---                  │ ---    │ ---      │
│ i64 │ str        │ list[str]            │ str    │ str      │
╞═════╪════════════╪══════════════════════╪════════╪══════════╡
│ 1   │ Bulbasaur  │ ["Grass", "Poison"]  │ 0.71 m │ 6.9 kg   │
│ 2   │ Ivysaur    │ ["Grass", "Poison"]  │ 0.99 m │ 13.0 kg  │
│ 3   │ Venusaur   │ ["Grass", "Poison"]  │ 2.01 m │ 100.0 kg │
│ 4   │ Charmander │ ["Fire"]             │ 0.61 m │ 8.5 kg   │
│ 5   │ Charmeleon │ ["Fire"]             │ 1.09 m │ 19.0 kg  │
│ …   │ …          │ …                    │ …      │ …        │
│ 147 │ Dratini    │ ["Dragon"]           │ 1.80 m │ 3.3 kg   │
│ 148 │ Dragonair  │ ["Dragon"]           │ 3.99 m │ 16.5 kg  │
│ 149 │ Dragonite  │ ["Dragon", "Flying"] │ 2.21 m │ 210.0 kg │
│ 150 │ Mewtwo     │ ["Psychic"]          │ 2.01 m │ 122.0 kg │
│ 151 │ Mew        │ ["Psychic"]          │ 0.41 m │ 4.0 kg   │
└─────┴────────────┴──────────────────────┴────────┴──────────┘
```

****

**表5-4** 列出了一些常用于读取JSON和NDJSON的参数，我们将在接下来的部分中讨论。

****

_表5-4：_`_pl.read_json()_`_和_`_pl.read_ndjson()_`_函数的常用参数。_

| 参数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">source</font>**` | 文件的路径或类似文件的对象。 |
| `**<font style="color:#1DC0C9;">schema</font>**` | DataFrame的模式可以通过几种方式声明：（1）作为 `{name: type}` 对的字典；如果类型为None，将自动推断。（2）作为列名列表；在这种情况下，类型将自动推断。（3）作为 `(name, type)` 对的列表；等同于字典形式。 |
| `**<font style="color:#1DC0C9;">schema_overrides</font>**` | 支持对一个或多个列的类型指定或覆盖；注意任何从`schema`参数推断的类型将被覆盖。对于底层数据中存在的类型，这里给出的名称将覆盖它们。 |




<h3 id="Pb2kT">**NDJSON**</h3>
NDJSON是一种方便的格式，用于存储或流式传输结构化数据，可以一次处理一条记录。它本质上是JSON对象的集合，由换行符分隔。

NDJSON数据集中每一行都是一个有效的JSON对象，但整个文件不是有效的JSON数组，因为换行符并不是JSON语法的一部分。这种格式的好处在于，它允许您轻松地向数据集中添加内容，并逐行高效地读取数据，这在流式传输场景或处理无法一次全部加载到内存中的大数据集时特别有用。NDJSON通常用于日志文件到RESTful API等场景。

我们通过监听Wikimedia API的流，并对其稍作清理，准备了`data/wikimedia.ndjson`。以下是该文件的前5行：

```bash
$ cat data/wikimedia.ndjson
```

```json
{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://en..."},
{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://en..."},
{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://en..."},
{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://en..."},
{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://en..."},
... 还有95行
```



同样，每一行都是一个单独的JSON对象。让我们来仔细查看其中的第一个：

```python
from json import loads
from pprint import pprint

with open("data/wikimedia.ndjson") as f:
    pprint(loads(f.readline()))
```

```plain
{'$schema': '/mediawiki/recentchange/1.0.0',
 'bot': False,
 'comment': '/* League champions, runners-up and play-off finalists */',
 'id': 1659529639,
 'length': {'new': 91166, 'old': 91108},
 'meta': {'domain': 'en.wikipedia.org',
          'dt': '2023-07-29T07:51:39Z',
          'id': '0416300b-980c-45bb-b0a2-c9d7a9e2b7eb',
          'offset': 4820784717,
          'partition': 0,
          'request_id': 'ea0541fb-4e72-4fc3-82f0-6c26651b2043',
          'stream': 'mediawiki.recentchange',
          'topic': 'eqiad.mediawiki.recentchange',
          'uri': 'https://en.wikipedia.org/wiki/EFL_Championship'},
 'minor': False,
 'namespace': 0,
 'notify_url': 'https://en.wikipedia.org/w/index.php?diff=1167689309&oldid=1166…
 'parsedcomment': '<span dir="auto"><span class="autocomment"><a '
                  'href="/wiki/EFL_Championship#League_champions,_runners-up_an…
                  'title="EFL Championship">→\u200eLeague champions, '
                  'runners-up and play-off finalists</a></span></span>',
 'revision': {'new': 1167689309, 'old': 1166824248},
 'server_name': 'en.wikipedia.org',
 'server_script_path': '/w',
 'server_url': 'https://en.wikipedia.org',
 'timestamp': 1690617099,
 'title': 'EFL Championship',
 'title_url': 'https://en.wikipedia.org/wiki/EFL_Championship',
 'type': 'edit',
 'user': '87.12.215.232',
 'wiki': 'enwiki'}
```



注意这个JSON对象是稍微嵌套的。三个键，即`length`、`meta`和`revision`，都有多个键和值。让我们看看Polars如何使用`pl.read_ndjson()`函数加载这些数据：

```python
wikimedia = pl.read_ndjson("data/wikimedia.ndjson")
wikimedia
```

```plain
shape: (100, 20)
┌─────────────────────┬─────────────────────┬───┬────────┬─────────────────────┐
│ $schema             │ meta                │ … │ wiki   │ parsedcomment       │
│ ---                 │ ---                 │   │ ---    │ ---                 │
│ str                 │ struct[9]           │   │ str    │ str                 │
╞═════════════════════╪═════════════════════╪═══╪════════╪═════════════════════╡
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │ <span dir="auto"><… │
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │                     │
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │ <span dir="auto"><… │
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │ Nominated for dele… │
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │ Rescuing 1 sources… │
│ …                   │ …                   │ … │ …      │ …                   │
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │ <span dir="auto"><… │
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │ Ce                  │
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │                     │
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │                     │
│ /mediawiki/recentc… │ {"https://en.wikip… │ … │ enwiki │ <span dir="auto"><… │
└─────────────────────┴─────────────────────┴───┴────────┴─────────────────────┘
```



就像我们对Pokedex所做的那样，我们可以使用`unnest()`函数将键转换为新的列：

```python
(
    wikimedia.rename({"id": "edit_id"})
    .unnest("meta")
    .select("timestamp", "title", "user", "comment")
)
```

```plain
shape: (100, 4)
┌────────────┬────────────────────┬────────────────────┬────────────────────┐
│ timestamp  │ title              │ user               │ comment            │
│ ---        │ ---                │ ---                │ ---                │
│ i64        │ str                │ str                │ str                │
╞════════════╪════════════════════╪════════════════════╪════════════════════╡
│ 1690617099 │ EFL Championship   │ 87.12.215.232      │ /* League champio… │
│ 1690617102 │ Lim Sang-choon     │ Preferwiki         │                    │
│ 1690617104 │ Higher             │ Ss112              │ /* Albums */ add   │
│ 1690617104 │ International Pok… │ Piotrus            │ Nominated for del… │
│ 1690617105 │ Abdul Hamid Khan … │ InternetArchiveBo… │ Rescuing 1 source… │
│ …          │ …                  │ …                  │ …                  │
│ 1690617238 │ Havering Resident… │ MRSC               │ /* 2018 election … │
│ 1690617235 │ Olha Kharlan       │ 2603:7000:2101:AA… │ Ce                 │
│ 1690617238 │ Mukim Kota Batu    │ Pangalau           │                    │
│ 1690617239 │ User:IDK1213safas… │ 94.101.29.27       │                    │
│ 1690617234 │ List of bus route… │ Pedroperezhumbert… │ /* Non-TfL bus ro… │
└────────────┴────────────────────┴────────────────────┴────────────────────┘
```

注意，我们需要将`id`列重命名为`edit_id`，否则`df.unnest()`将失败，并提示有重复的列名。



<h2 id="HNi7G">**其他文件格式**</h2>
Polars还支持以下格式：Arrow IPC（Feather版本2）、Apache Avro、Delta lake表和PyArrow数据集。对于这些格式，可以分别使用`pl.read_ipc()`、`pl.read_avro()`、`pl.read_delta()`和`pl.scan_pyarrow_dataset()`函数。

如果你有一个Polars不支持的文件，那么Pandas或许可以帮上忙。Pandas已经存在超过13年，所以它支持更多的格式。你可以使用`pl.from_pandas()`将Pandas的DataFrame转换为Polars的DataFrame。以下是从HTML页面读取表格的示例：

```python
import pandas as pd

url = "https://en.wikipedia.org/wiki/List_of_Latin_abbreviations"
pl.from_pandas(pd.read_html(url)[0])
```

```plain
shape: (62, 4)
┌──────────────┬───────────────────┬────────────────────┬────────────────────┐
│ abbreviation │ Latin             │ translation        │ usage and notes    │
│ ---          │ ---               │ ---                │ ---                │
│ str          │ str               │ str                │ str                │
╞══════════════╪═══════════════════╪════════════════════╪════════════════════╡
│ A.D.         │ anno Domini       │ "in the year of t… │ Used to label or … │
│ A.I.         │ ad interim        │ "temporarily"      │ Used in business … │
│ a.m.         │ ante meridiem     │ "before midday"[1… │ Used on the twelv… │
│ ca./c.       │ circa             │ "around", "about"… │ Used with dates t… │
│ Cap.         │ capitulus         │ "chapter"          │ Used before a cha… │
│ …            │ …                 │ …                  │ …                  │
│ S.O.S.       │ si opus sit       │ "if there is need… │ A prescription in… │
│ sic          │ sic erat scriptum │ "thus it was writ… │ Often used when c… │
│ stat.        │ statim            │ "immediately"      │ Often used in med… │
│ viz.         │ videlicet         │ "namely", "to wit… │ In contradistinct… │
│ vs. v.       │ versus            │ "against"          │ Sometimes is not … │
└──────────────┴───────────────────┴────────────────────┴────────────────────┘
```



除了HTML，Pandas（而不是Polars）还支持读取Feather、定宽文本文件、HDF5、ORC、SAS、SPSS、Stata、XLS、XML、本地剪贴板以及各种电子表格格式。这些格式中的一些需要安装额外的包。例如，上面的HTML示例需要`lxml`包。更多信息请参见Pandas用户指南中的[IO工具部分](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html)。



<h2 id="Ma3Y1">**查询数据库**</h2>
Polars提供了一种便捷的方法与关系数据库交互，使用`pl.read_database()`函数。该函数允许你直接执行SQL查询并将结果检索为DataFrame。Polars支持从各种关系数据库检索数据，包括Postgres、MsSQL、MySQL、Oracle、SQLite和BigQuery。

`pl.read_database()`函数需要SQL查询和连接字符串。连接字符串允许你指定数据库的类型、位置，并在需要时提供凭证。举例来说，连接到Postgres数据库的字符串格式为：

```plain
postgres://username:password@server:port/database
```

数据库通常运行在别处（至少在一个独立的进程中），通常需要凭证。而SQLite数据库则只是一个本地文件。为了简化操作，我们将使用SQLite数据库演示如何让Polars查询数据库。这个过程对于其他类型的数据库是相同的，唯一的区别是你需要指定不同的连接字符串并可能使用不同的SQL方言。

我们使用的是Sakila数据库，这是一个由MySQL开发团队创建的示例数据库，并被Bradley Grant[移植到SQLite](https://github.com/grant/Sakila)平台。以下查询选择了10个虚拟的电影标题，并附带类别、评分和时长：

```python
pl.read_database_uri(
    query="""
    SELECT
        f.film_id,
        f.title,
        c.name AS category,
        f.rating,
        f.length / 60.0 AS length
    FROM
        film AS f,
        film_category AS fc,
        category AS c
    WHERE
        fc.film_id = f.film_id
        AND fc.category_id = c.category_id
    LIMIT 10
    """,
    uri="sqlite:::data/sakila.db",
)
```

```plain
shape: (10, 5)
┌─────────┬──────────────────┬─────────────┬────────┬──────────┐
│ film_id │ title            │ category    │ rating │ length   │
│ ---     │ ---              │ ---         │ ---    │ ---      │
│ i64     │ str              │ str         │ str    │ f64      │
╞═════════╪══════════════════╪═════════════╪════════╪══════════╡
│ 1       │ ACADEMY DINOSAUR │ Documentary │ PG     │ 1.433333 │
│ 2       │ ACE GOLDFINGER   │ Horror      │ G      │ 0.8      │
│ 3       │ ADAPTATION HOLES │ Documentary │ NC-17  │ 0.833333 │
│ 4       │ AFFAIR PREJUDICE │ Horror      │ G      │ 1.95     │
│ 5       │ AFRICAN EGG      │ Family      │ G      │ 2.166667 │
│ 6       │ AGENT TRUMAN     │ Foreign     │ PG     │ 2.816667 │
│ 7       │ AIRPLANE SIERRA  │ Comedy      │ PG-13  │ 1.033333 │
│ 8       │ AIRPORT POLLOCK  │ Horror      │ R      │ 0.9      │
│ 9       │ ALABAMA DEVIL    │ Horror      │ PG-13  │ 1.9      │
│ 10      │ ALADDIN CALENDAR │ Sports      │ NC-17  │ 1.05     │
└─────────┴──────────────────┴─────────────┴────────┴──────────┘
```



如果SQL不是你的兴趣所在，但你仍然需要从数据库中读取数据，你可以使用一个或多个`SELECT * FROM table`查询来选择所有内容，然后在Polars中继续操作。以下三条SQL查询和Polars代码与上面的单条SQL查询产生相同的结果：

```python
db = "sqlite:::data/sakila.db"
films = pl.read_database_uri("SELECT * FROM film", db)
film_categories = pl.read_database_uri("SELECT * FROM film_category", db)
categories = pl.read_database_uri("SELECT * FROM category", db)

(
    films.join(film_categories, on="film_id", suffix="_fc")
    .join(categories, on="category_id", suffix="_c")
    .select(
        "film_id",
        "title",
        pl.col("name").alias("category"),
        "rating",
        pl.col("length") / 60,
    )
    .limit(10)
)
```

```plain
shape: (10, 5)
┌─────────┬──────────────────┬─────────────┬────────┬──────────┐
│ film_id │ title            │ category    │ rating │ length   │
│ ---     │ ---              │ ---         │ ---    │ ---      │
│ i64     │ str              │ str         │ str    │ f64      │
╞═════════╪══════════════════╪═════════════╪════════╪══════════╡
│ 1       │ ACADEMY DINOSAUR │ Documentary │ PG     │ 1.433333 │
│ 2       │ ACE GOLDFINGER   │ Horror      │ G      │ 0.8      │
│ 3       │ ADAPTATION HOLES │ Documentary │ NC-17  │ 0.833333 │
│ 4       │ AFFAIR PREJUDICE │ Horror      │ G      │ 1.95     │
│ 5       │ AFRICAN EGG      │ Family      │ G      │ 2.166667 │
│ 6       │ AGENT TRUMAN     │ Foreign     │ PG     │ 2.816667 │
│ 7       │ AIRPLANE SIERRA  │ Comedy      │ PG-13  │ 1.033333 │
│ 8       │ AIRPORT POLLOCK  │ Horror      │ R      │ 0.9      │
│ 9       │ ALABAMA DEVIL    │ Horror      │ PG-13  │ 1.9      │
│ 10      │ ALADDIN CALENDAR │ Sports      │ NC-17  │ 1.05     │
└─────────┴──────────────────┴─────────────┴────────┴──────────┘
```

当你采用这种方法时，需考虑传输的数据量。为了获得更好的性能，通常让数据库尽可能多地处理工作，并仅选择你需要的列是一个好主意。



<h2 id="NsJLq">**写入数据**</h2>
Python Polars 提供了多种将数据写入文件的方法。理解每种格式的细微差别有助于你根据具体的数据需求做出明智的选择。



<h3 id="BoLqx">**CSV 格式**</h3>
最受欢迎的写入选择之一是 CSV 格式。CSV 以其广泛的识别度和与众多软件和工具的兼容性而著称。要将 DataFrame 保存为此格式，你可以使用 `df.write_csv()` 方法：

```python
all_stocks.write_csv("data/all_stocks.csv")
```

****

_表 5-5 _`_df.write_csv()_`_方法的常用参数_

| 参数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">file</font>**` | 要写入DataFrame的文件路径。如果设置为`None`（默认值），则输出将以字符串形式返回。 |
| `**<font style="color:#1DC0C9;">has_header</font>**` | 是否包含表头（默认值：`True`）。 |
| `**<font style="color:#1DC0C9;">separator</font>**` | 分隔CSV字段的字符（默认值：`,`）。 |
| `**<font style="color:#1DC0C9;">quote</font>**` | 用于引用值的字符（默认值：`"`）。 |
| `**<font style="color:#1DC0C9;">null_value</font>**` | 表示缺失值的字符串（默认值：空字符串）。 |


由于CSV是一种基于文本的格式，它易于人类读取。然而，正如我们已经看到的，它也带来了一些与编码、缺失数据和模式推断相关的挑战。



<h3 id="OSOjp">**Excel 格式**</h3>
如果你希望以许多商业用户熟悉的格式写入数据，Excel格式是一个理想选择。方法 `df.write_excel("filename.xlsx")` 实现了这一点：

```python
all_stocks.write_excel("data/all_stocks.xlsx")
```



_表 5- _`_df.write_excel()_`_方法的常用参数_

| 参数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">worksheet</font>**` | 目标工作表的名称（默认值：`Sheet1`）。 |
| `**<font style="color:#1DC0C9;">position</font>**` | Excel中的表格位置，以Excel表示法（例如："A1"）或(row, col)整数元组的形式。 |
| `**<font style="color:#1DC0C9;">table_style</font>**` | 命名的Excel表样式，例如“Table Style Medium 4”，或一个包含如下键的字典：`"style"`，`"first_column"`，`"last_column"`，`"banded_columns"`，`"banded_rows"`。 |
| `**<font style="color:#1DC0C9;">column_widths</font>**` | 一个`{列名: 整数}`字典，或设置表格列宽的单一整数（以像素单位表示）。如果给定单个整数，则所有表格列使用相同的值。 |


Excel的主要优势在于其对多工作表的支持以及将样式和公式直接融入数据的能力。然而，作为一种二进制格式，它意味着直接的人类可读性较差。此外，它也不是处理非常大数据集的最佳选择，因为性能可能会成为问题。



<h3 id="y7VHg">**Parquet 格式**</h3>
如果你的DataFrame较大，并且你需要一种高效的读写机制，那么Parquet格式是理想选择。使用`df.write_parquet("filename.parquet")`方法，你可以将数据保存为这种列式存储格式：

```python
all_stocks.write_parquet("data/all_stocks.parquet")
```



_表 5-7 _`_df.write_parquet()_`_方法的常用参数_

| 参数 | 描述 |
| --- | --- |
| `**<font style="color:#1DC0C9;">file</font>**` | 要写入DataFrame的文件路径。 |
| `**<font style="color:#1DC0C9;">compression</font>**` | 选择`zstd`以获得良好的压缩性能。选择`lz4`以实现快速压缩和解压缩。选择`snappy`以确保与旧版Parquet读取器的向后兼容性。 |
| `**<font style="color:#1DC0C9;">compression_level</font>**` | 要使用的压缩级别。较高的压缩级别意味着磁盘上的文件更小。 |


Parquet为高效设计；它对数据进行压缩以优化存储，并支持复杂的嵌套数据结构。此外，它保留了模式信息，允许一致的数据检索。然而，Parquet不像CSV或Excel那样被广泛识别，因此你可能需要特定的工具或库来读取数据。



<h3 id="v0sS6">**其他考虑事项**</h3>
Polars 还支持写入其他格式，如 Avro 和 JSON。确定合适的格式时，权衡因素包括数据的预期用途、与其他软件的兼容性、数据集的大小以及所需数据结构的复杂性是至关重要的。



<h2 id="ffZIG">**总结**</h2>
在本章中，我们探讨了Polars在读取和写入数据方面的功能。我们详细介绍了如何有效地与各种文件格式交互，从CSV和Excel到Parquet。通过结合使用通配符技术，你能够有效地处理多个文件。我们讨论了正确读取缺失值的细微差别，以及处理不同字符编码的复杂性。有了这些函数，你应该能够轻松地将即将讨论的主题和代码示例应用到你自己的数据中。 

