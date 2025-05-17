

有很大可能你已经有了Pandas的使用经验。如果没有，你可以安全地跳过本章。如果有，我们鼓励你阅读这一章；这将使后续内容更容易理解。

在本章中，我们希望确保你从Pandas向Polars的过渡尽可能顺利，因此我们将重点介绍这两个库的相似之处，更重要的是它们之间的差异。

自从Polars开始流行以来，它经常被拿来与Pandas比较。尽管这种比较往往对Polars有利，但并不是所有人都对此表示尊重。我们不能认同这种看法，因为数据科学家实际上欠Pandas很多。没有Pandas，可能就不会有Polars。我们甚至认为，可以说没有Pandas，Python也不会成为处理数据的如此受欢迎的语言。我们将尽可能客观地讨论这两个库的相似之处和不同之处。

在本章中，你将学习：

+ Pandas和Polars的共同点
+ 这两个库的语法和输出在外观上的不同之处
+ 你需要摒弃的Pandas概念
+ 如何在两个库中执行常见操作

我们不会比较Pandas和Polars的性能。

---



<h1 id="H4mrX">动物</h1>
在本章中，我们将使用一个名为`data/animals.csv`的CSV文件，其中包含十种动物及其一些属性，如栖息地、寿命和体重。虽然这个文件很小，但对于本章来说已经足够了。原始数据如下所示：

```bash
%cat data/animals.csv
```

```plain
animal,class,habitat,diet,lifespan,status,features,weight
dolphin,mammal,ocean/carnivore,40,least concern,high intelligence
duck,bird,wetlands/omnivore,8,least concern,waterproof feathers,3
elephant,mammal,savannah/herbivore,60,endangered,large ears and trunk,8000
ibis,bird,wetlands/omnivore,16,least concern,long curved bill,1
impala,mammal,savannah/herbivore,12,least concern,long curved horns,65
kudu,mammal,savannah/herbivore,15,least concern,spiral horns,250
narwhal,mammal,arctic ocean/carnivore,40,near threatened,long spiral tusk
panda,mammal,forests/herbivore,20,vulnerable,black and white coloration,100
polar bear,mammal,arctic/carnivore,25,vulnerable,thick fur and blubber,450
ray,fish,ocean/carnivore,20,,"flat disc-shaped body",90
```

原始数据很重要，因为两个库在读取它时略有不同。请注意，我们缺少独角鲸的体重（该行以逗号结束），而对于鳐鱼，它的状态值为空（用两个双引号表示）。你能猜到Pandas如何处理这两个值吗？你很快就会知道，但首先，让我们考虑一下Pandas和Polars的共同点。



<h1 id="D6yrt">相似之处</h1>
从高层次上看，Pandas和Polars有很多共同点。它们都：

+ 是用于处理结构化数据的Python库。
+ 提供DataFrame作为主要数据结构。
+ 将DataFrame定义为由（可能是多个）Series组成的结构，每个Series必须具有相同数量的值，且Series内的值必须具有相同的数据类型。
+ 支持各种数据类型，如布尔型、整数、浮点数、字符串、分类数据、日期、时间和持续时间。
+ 拥有非常大的API（即，许多函数和方法），可以对DataFrame和Series执行操作。
+ 可以从多种文件格式和数据库中读取和写入数据。
+ 可以在Jupyter笔记本中使用，并提供HTML表示。
+ 都以字母“P”开头。

---

**名字的含义**

“Pandas”这个名字与动物无关——它来源于“Panel Data”，一种三维DataFrame。自Pandas 0.20.0版本（2017年5月发布）起，Panel数据结构已被废弃，但名字沿用了下来。如今，Pandas中的两个主要数据结构是Series和DataFrame，与Polars类似。

Ritchie Vink，Polars的创建者，选择“Polars”这个名字有两个原因：第一，北极熊比熊猫更强壮；第二，Polars的结尾是“rs”，这是向Polars实现所用的Rust编程语言致敬。

---



<h1 id="DcWty">外观对比</h1>
正如你在生活中的每次初次接触一样，你的初步印象往往是基于外观的。由于你熟悉Pandas，Polars乍看之下可能会显得有些奇怪。让我们现在就解决这个问题。

同时导入Pandas和Polars，如下所示：

```python
import pandas as pd
import polars as pl
```

没错：与使用`pd`作为起点不同，你需要习惯使用`pl`。现在你已经准备好检查Polars的外观了，无论是从代码角度还是输出角度，并将其与Pandas进行比较。

<h2 id="oSicA">代码中的差异</h2>
让我们读取CSV文件`data/animals.csv`，并创建一个Pandas DataFrame，称为`animals_pd`，以及一个Polars DataFrame，称为`animals_pl`：

```python
_pd = pd.read_csv("data/animals.csv", sep=",", header=0)
_pl = pl.read_csv("data/animals.csv", separator=",", has_header=True)

print(type(animals_pd))  # Pandas的DataFrame类型
print(type(animals_pl))  # Polars的DataFrame类型
```

```python
type(animals_pd) = <class 'pandas.core.frame.DataFrame'>
type(animals_pl) = <class 'polars.dataframe.frame.DataFrame'>
```

尽管这些函数具有相同的名称`read_csv()`，但它们接受的参数不同（例如`sep`和`separator`），并且生成的类型也不同。在本章的后面，我们将探讨其他稍有不同的函数和方法。

---

**你可以做到！**  
在许多情况下，你会遇到这些小的差异，并且它们可能会使你向Polars的过渡变得更加具有挑战性。意识到这一点是很好的。同时，相信我们，当我们说一切都会好起来时，确实如此。你能行的！

---



让我们来看看Pandas和Polars如何显示它们的DataFrame和Series。

<h2 id="ASEu2">显示差异</h2>


在Jupyter Notebook中工作时，DataFrame会通过一些HTML和CSS优雅地显示出来。**图2-1**和**图2-2**展示了Pandas和Polars的DataFrame在Jupyter Notebook中的显示效果。

![Figure 2-1. A screenshot of a Pandas DataFrame in a Jupyter Notebook](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728536871899-01440113-e399-47f3-924d-8d69a61baf22.png)

![Figure 2-2. A screenshot of a Polars DataFrame in a Jupyter Notebook](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728536943399-46dbe9df-3270-4258-b13e-f81faf18b33a.png)



请注意以下两个截图之间的区别：

+ Pandas显示数据值，而Polars还显示了DataFrame的形状，即行数和列数。
+ Pandas在左侧打印了索引，编号从0到9，而Polars没有打印索引。我们将在本章后面详细讨论这一点。
+ Pandas显示列名，而Polars还显示每列的数据类型。
+ Pandas将两个缺失值显示为“NaN”，而Polars则显示`null`（缺失`weight`列的值）和两个双引号表示缺失的`status`值。
+ Pandas将每个字符串值都用双引号包裹，而Polars没有。

当你在终端中使用Python时，你会看到文本表示而不是HTML表示。本书中的示例也是如此。以下是Pandas和Polars中的文本显示效果：

```python
animals_pd
```

```plain
animal   class        habitat       diet  lifespan           status  \
0     dolphin  mammal  oceans/rivers  carnivore        40    least concern
1        duck    bird       wetlands   omnivore         8    least concern
2    elephant  mammal       savannah  herbivore        60       endangered
3        ibis    bird       wetlands   omnivore        16    least concern
4      impala  mammal       savannah  herbivore        12    least concern
5        kudu  mammal       savannah  herbivore        15    least concern
6     narwhal  mammal   arctic ocean  carnivore        40  near threatened
7       panda  mammal        forests  herbivore        20       vulnerable
8  polar bear  mammal         arctic  carnivore        25       vulnerable
9         ray    fish         oceans  carnivore        20              NaN

                     features  weight
0           high intelligence   150.0
1         waterproof feathers     3.0
2        large ears and trunk  8000.0
3           long, curved bill     1.0
4          long, curved horns    70.0
5                spiral horns   250.0
6           long, spiral tusk     NaN
7  black and white coloration   100.0
8       thick fur and blubber   720.0
9      flat, disc-shaped body    90.0
```

```python
animals_pl
```

```plain
shape: (10, 8)
┌──────────┬────────┬─────────┬─────────┬─────────┬─────────┬─────────┬────────┐
│ animal   │ class  │ habitat │ diet    │ lifespa │ status  │ feature │ weight │
│ ---      │ ---    │ ---     │ ---     │ n       │ ---     │ s       │ ---    │
│ str      │ str    │ str     │ str     │ ---     │ str     │ ---     │ i64    │
│          │        │         │         │ i64     │         │ str     │        │
╞══════════╪════════╪═════════╪═════════╪═════════╪═════════╪═════════╪════════╡
│ dolphin  │ mammal │ oceans/ │ carnivo │ 40      │ least   │ high    │ 150    │
│          │        │ rivers  │ re      │         │ concern │ intelli │        │
│          │        │         │         │         │         │ gence   │        │
│ duck     │ bird   │ wetland │ omnivor │ 8       │ least   │ waterpr │ 3      │
│          │        │ s       │ e       │         │ concern │ oof fea │        │
│          │        │         │         │         │         │ thers   │        │
│ elephant │ mammal │ savanna │ herbivo │ 60      │ endange │ large   │ 8000   │
│          │        │ h       │ re      │         │ red     │ ears    │        │
│          │        │         │         │         │         │ and     │        │
│          │        │         │         │         │         │ trunk   │        │
│ ibis     │ bird   │ wetland │ omnivor │ 16      │ least   │ long,   │ 1      │
│          │        │ s       │ e       │         │ concern │ curved  │        │
│          │        │         │         │         │         │ bill    │        │
│ impala   │ mammal │ savanna │ herbivo │ 12      │ least   │ long,   │ 70     │
│          │        │ h       │ re      │         │ concern │ curved  │        │
│          │        │         │         │         │         │ horns   │        │
│ kudu     │ mammal │ savanna │ herbivo │ 15      │ least   │ spiral  │ 250    │
│          │        │ h       │ re      │         │ concern │ horns   │        │
│ narwhal  │ mammal │ arctic  │ carnivo │ 40      │ near    │ long,   │ null   │
│          │        │ ocean   │ re      │         │ threate │ spiral  │        │
│          │        │         │         │         │ ned     │ tusk    │        │
│ panda    │ mammal │ forests │ herbivo │ 20      │ vulnera │ black   │ 100    │
│          │        │         │ re      │         │ ble     │ and     │        │
│          │        │         │         │         │         │ white   │        │
│          │        │         │         │         │         │ colo…   │        │
│ polar    │ mammal │ arctic  │ carnivo │ 25      │ vulnera │ thick   │ 720    │
│ bear     │        │         │ re      │         │ ble     │ fur and │        │
│          │        │         │         │         │         │ blubbe… │        │
│ ray      │ fish   │ oceans  │ carnivo │ 20      │         │ flat,   │ 90     │
│          │        │         │ re      │         │         │ disc-sh │        │
│          │        │         │         │         │         │ aped    │        │
│          │        │         │         │         │         │ bo…     │        │
└──────────┴────────┴─────────┴─────────┴─────────┴─────────┴─────────┴────────┘
```

**<font style="color:rgb(61, 59, 73);"></font>**

显然，两个DataFrame都太宽，无法完全适应一个屏幕。每个库使用不同的策略来解决这个问题。Pandas首先打印尽可能多的列，然后继续在下一行打印。注意，索引被打印了两次。Polars将所有列打印在一起，但会在每个列中换行，因此每一行占用多行。一些在`features`列中的长值被截断。此外，Polars还打印了DataFrame的形状和每列的数据类型。默认情况下，Polars会添加边框。在文本表示中，与HTML表示不同，Polars不会用双引号包裹字符串值。

为了完整起见，让我们看看从Pandas Series和Polars Series中提取单列作为Series时的显示效果：

```python
animals_pd["animal"]
```

```plain
0       dolphin
1          duck
2      elephant
3          ibis
4        impala
5          kudu
6       narwhal
7         panda
8    polar bear
9           ray
Name: animal, dtype: object
```

```python
animals_pl.get_column("animal")
```

```plain
shape: (10,)
Series: 'animal' [str]
[
	"dolphin"
	"duck"
	"elephant"
	"ibis"
	"impala"
	"kudu"
	"narwhal"
	"panda"
	"polar bear"
	"ray"
]
```



我们再次看到 Pandas 输出了索引。我们还看到了 Pandas 为这个系列（Series）分配的数据类型，即“object”。

Pandas 和 Polars 都没有一个针对系列（Series）的 HTML 表示形式，所以在 Jupyter Notebook 中你会看到相同的输出。

为了让接下来这一章——以及整本书——更加简洁，我们将使用文本表示形式的 DataFrame。为了让 `animals_pd` 和 `animals_pl` 这两个 DataFrame 更加适应页面，我们将移除三列。

```python
animals_pd = animals_pd.drop(columns=["habitat", "diet", "features"])
animals_pd
```

```plain
animal   class  lifespan           status  weight
0     dolphin  mammal        40    least concern   150.0
1        duck    bird         8    least concern     3.0
2    elephant  mammal        60       endangered  8000.0
3        ibis    bird        16    least concern     1.0
4      impala  mammal        12    least concern    70.0
5        kudu  mammal        15    least concern   250.0
6     narwhal  mammal        40  near threatened     NaN
7       panda  mammal        20       vulnerable   100.0
8  polar bear  mammal        25       vulnerable   720.0
9         ray    fish        20              NaN    90.0
```

```python
animals_pl = animals_pl.drop("habitat", "diet", "features")
animals_pl
```

```plain
shape: (10, 5)
┌────────────┬────────┬──────────┬─────────────────┬────────┐
│ animal     │ class  │ lifespan │ status          │ weight │
│ ---        │ ---    │ ---      │ ---             │ ---    │
│ str        │ str    │ i64      │ str             │ i64    │
╞════════════╪════════╪══════════╪═════════════════╪════════╡
│ dolphin    │ mammal │ 40       │ least concern   │ 150    │
│ duck       │ bird   │ 8        │ least concern   │ 3      │
│ elephant   │ mammal │ 60       │ endangered      │ 8000   │
│ ibis       │ bird   │ 16       │ least concern   │ 1      │
│ impala     │ mammal │ 12       │ least concern   │ 70     │
│ kudu       │ mammal │ 15       │ least concern   │ 250    │
│ narwhal    │ mammal │ 40       │ near threatened │ null   │
│ panda      │ mammal │ 20       │ vulnerable      │ 100    │
│ polar bear │ mammal │ 25       │ vulnerable      │ 720    │
│ ray        │ fish   │ 20       │                 │ 90     │
└────────────┴────────┴──────────┴─────────────────┴────────┘
```



有些外观上的差异纯粹是美学方面的，比如使用双引号和边框。这些美学部分大多可以配置。其他外观上的差异与底层概念有关，比如索引。

<h1 id="AXaAk">需要“忘记”的概念</h1>
忘记一些概念可能是从 Pandas 转向 Polars 最难的部分。这些概念影响了你如何思考 DataFrame 的工作方式，并决定了可供你使用的 API。

如果你不花时间去忘记这些概念，那么你可能会在与从头开始学习的人相比时处于劣势。在接下来的五节中，我们将讨论以下概念：索引、坐标轴、索引与切片、贪婪性、以及松散性。

<h2 id="azuQ2">索引（Index）</h2>
你可以忘记的第一件事是“索引”。Pandas 的 DataFrame 总是有索引，有时甚至有多级索引。许多 DataFrame 只是一个 RangeIndex，包括我们的 `animals_pd` DataFrame：

```python
animals_pd.index
```

```python
RangeIndex(start=0, stop=10, step=1)
```

某些方法（包括聚合）会改变索引：

```python
animals_agg_pd = animals_pd.groupby(["class", "status"])[["weight"]].mean()
animals_agg_pd
```

```plain
weight
class  status
bird   least concern       2.000000
mammal endangered       8000.000000
       least concern     156.666667
       near threatened          NaN
       vulnerable        410.000000
```

你可以验证前面的代码片段是基于 `class` 和 `status` 列创建了一个多重索引（MultiIndex）：

```python
animals_agg_pd.index
```

```plain
MultiIndex([(  'bird',   'least concern'),
            ('mammal',      'endangered'),
            ('mammal',   'least concern'),
            ('mammal', 'near threatened'),
            ('mammal',      'vulnerable')],
           names=['class', 'status'])
```



由于许多用户发现索引难以使用，他们希望去掉它。你可以通过将 `as_index=False` 传递给 `df.groupby()` 来避免这种行为，但你可能已经多次使用过 `df.reset_index()` 方法，将索引或多重索引转回列：

```python
animals_agg_pd.reset_index()
```

```plain
class           status       weight
0    bird    least concern     2.000000
1  mammal       endangered  8000.000000
2  mammal    least concern   156.666667
3  mammal  near threatened          NaN
4  mammal       vulnerable   410.000000
```



Polars 的 DataFrame 没有索引，更不用说多重索引了——它们只有列。以下是在 Polars 中执行相同聚合的输出（目前不用担心语法）：

```python
animals_pl.group_by(["class", "status"]).agg(pl.col("weight").mean())
```

```plain
shape: (6, 3)
┌────────┬─────────────────┬────────────┐
│ class  │ status          │ weight     │
│ ---    │ ---             │ ---        │
│ str    │ str             │ f64        │
╞════════╪═════════════════╪════════════╡
│ mammal │ vulnerable      │ 410.0      │
│ fish   │                 │ 90.0       │
│ mammal │ near threatened │ null       │
│ mammal │ least concern   │ 156.666667 │
│ mammal │ endangered      │ 8000.0     │
│ bird   │ least concern   │ 2.0        │
└────────┴─────────────────┴────────────┘
```



请注意，Polars 包含了 `fish` 类的一行，而 Pandas 没有。我们稍后会回到这一点。

没有索引意味着以下 Pandas DataFrame 方法在 Polars 中没有等效的操作：`df.align()`，`df.droplevel()`，`df.reindex()`，`df.rename_axis()`，`df.reset_index()`，`df.set_axis()`，`df.set_index()`，`df.sort_index()`，`df.stack()`，`df.swapaxis()`，`df.swapevel()`，和 `df.unstack()`。

<h2 id="cJebs">轴（Axes）</h2>
你可以忘记的第二件事是轴。Pandas DataFrame 有两个轴：行和列。

许多 Pandas DataFrame 方法可以在行或列上操作，包括 `df.drop()`，`df.dropna()`，`df.filter()`，`df.rename()`，`df.shift()`，`df.sort_index()`，以及 `df.sort_values()`。这些方法接受 `axis` 参数，默认值为“0”或“rows”。忘记指定轴是很常见的错误，这可能导致错误，比如尝试删除 `weight` 列时：

```python
animals_pd.drop("weight")
```

```plain
KeyError: "'[weight] not found in axis'"
```

Pandas 假定你想删除具有索引值“weight”的行。要操作列，你需要指定 `axis=1` 或 `axis="columns"`。另外，有时你可以使用关键词参数 `columns`（正如我们在本章前面删除三列时所做的那样）。因此，要实际删除 `weight` 列，你可以执行以下操作：

```python
animals_pd.drop("weight", axis=1)
```

```plain
      animal   class  lifespan           status
0     dolphin  mammal        40    least concern
1        duck    bird         8    least concern
2    elephant  mammal        60       endangered
3        ibis    bird        16    least concern
4      impala  mammal        12    least concern
5        kudu  mammal        15    least concern
6     narwhal  mammal        40  near threatened
7       panda  mammal        20       vulnerable
8  polar bear  mammal        25       vulnerable
9         ray    fish        20              NaN
```



当然，Polars DataFrame 也有行和列，只是你不需要担心指定要操作哪个轴。Polars 的方法总是作用于列（如 `df.drop()` 和 `df.rename()`）或者行（如 `df.filter()`，`df.sort()` 和 `df.drop_nulls()`）。

<h2 id="EXPPc">索引和切片（<font style="color:rgb(61, 59, 73);">Indexing and Slicing</font>）</h2>
Pandas 代码通常包含许多方括号的形式 `df[]`。这些括号允许你对行和列进行索引和切片操作。它们用于选择列、切片行、过滤行、创建新列、更新现有列以及提取值。

例如，要选择 `animal` 和 `class` 列：

```python
animals_pd[["animal", "class"]]
```

```plain
       animal   class
0     dolphin  mammal
1        duck    bird
2    elephant  mammal
3        ibis    bird
4      impala  mammal
5        kudu  mammal
6     narwhal  mammal
7       panda  mammal
8  polar bear  mammal
9         ray    fish
```



或者，只保留濒危动物：

```python
animals_pd[animals_pd["status"] == "endangered"]
```

```plain
animal   class  lifespan      status  weight
2  elephant  mammal        60  endangered  8000.0
```



或者，显示 DataFrame 的前三行：

```python
animals_pd[:3]
```

```plain
     animal   class  lifespan         status  weight
0   dolphin  mammal        40  least concern   150.0
1      duck    bird         8  least concern     3.0
2  elephant  mammal        60     endangered  8000.0
```



或者要更新一个现有的列：

```python
animals_pd["weight"] = animals_pd["weight"] * 1000
```

请注意，这段代码片段没有返回任何内容？这是因为该操作直接在原DataFrame中进行修改。我们稍后会回到这一点，因为它与Polars的行为不同。

在Pandas中，方括号还可以通过某个属性来使用：

+ 使用 `df.at[]` 通过标签访问单个行/列对的值。
+ 使用 `df.iat[]` 通过整数位置访问单个行/列对的值。
+ 使用 `df.loc[]` 通过标签访问一组行和列。
+ 使用 `df.iloc[]` 通过整数位置访问一组行和列。

除了这些不同类型的方括号，Pandas 还有 `df.xs()`，`df.get()` 和 `df.filter()` 方法。访问行和列的方式太多，结合其灵活性和功能重叠，可能会让人不知所措。

Polars 没有这些功能。实际上，Polars 支持方括号的基本用法，但主要是出于兼容性原因。Polars 的一般规则是：**不要使用方括号**。Polars 提供了专门的方法来完成相同的操作。这不仅使你的代码更具可读性，还允许 Polars 在你处于懒执行模式时优化计算（稍后会详细讨论）。

---

_**<font style="color:#ECAA04;">别再用方括号了</font>**_

_正如所提到的，Polars 的一般规则是避免使用方括号。例外情况是当你只想对某些行或列进行一次性检查时，方括号才有用。_

_例如，在第15章中，我们使用切片舒适地打印宽 DataFrame 的所有列。_

```python
print(trips[:,:4])
print(trips[:,4:7])
print(trips[:,7:11])
print(trips[:,11:])
```

_除此之外，你在 Polars 代码中没有理由使用方括号。_

---

<h2 id="TSXuS">**急切性（Eagerness）**</h2>
在 Pandas 中，每个操作都是**急切的（eager）**，并且许多操作直接在原地发生。**急切执行**意味着每个操作都会立即执行。**原地操作**意味着操作不会返回新的 DataFrame，而是修改现有的 DataFrame。你之前已经看过一个例子，当我们更新 `weight` 列时就是这样。

在 Polars 中，默认情况下没有任何操作是原地进行的。

除了急切模式，Polars 还支持**惰性查询（lazy queries）**。使用惰性查询时，你以一个 `LazyFrame`（而不是 `DataFrame`）开始，指定你想执行的所有操作，然后通过调用 `lf.collect()` 来完成操作。Polars 随后优化查询并执行。

下面是一个例子，我们指定了一个惰性查询，读取 CSV 文件 `data/animals.csv`，计算每个类的平均体重，然后只保留类为“mammal”的行：

```python
lazy_query = (
    pl.scan_csv("data/animals.csv")
    .group_by("class")
    .agg(pl.col("weight").mean())
    .filter(pl.col("class") == "mammal")
)
```

`pl.scan_csv()` 函数不会立即读取文件。

以下是未优化的（原始的）查询计划：

```python
lazy_query.show_graph(optimized=False)
```

![Figure 2-3. Naive query plan.](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728537968356-801e9bbd-2e2a-4881-8c71-713b2eb7a87f.png)

优化过的查询计划：

```python
lazy_query.show_graph()
```

![Figure 2-4. Optimized query plan.](https://cdn.nlark.com/yuque/0/2024/png/1310472/1728538081041-a4814daf-68a1-4fb5-b536-7749a691c0f6.png)



Polars 识别到在聚合之前先进行过滤会更快。通过使用 `lf.collect()` 方法，我们将惰性查询转化为一个实际的 DataFrame：

```python
lazy_query.collect()
```

```plain
shape: (1, 2)
┌────────┬─────────────┐
│ class  │ weight      │
│ ---    │ ---         │
│ str    │ f64         │
╞════════╪═════════════╡
│ mammal │ 1548.333333 │
└────────┴─────────────┘
```



你可以在**第4章**中了解更多关于惰性 API 的内容。在如此小的数据集上使用惰性 API 可能没有完全体现出它的优势，但它的工作方式已经说明了问题。即使你不使用惰性 API，你的 Polars 代码看起来也很相似，有很多方法，并且（希望）没有方括号。



<h2 id="I21oS">**松散性（Relaxedness）**</h2>
你需要忘记的最后一个概念是 Pandas 的松散性。换句话说，适应 Polars 的严格性。

这不仅仅是一个单一的概念，而是各种特性的一系列集合。以下是不完整的列表：

+ Pandas 允许你在处理日期和时间时使用数据字符串。而在 Polars 中，你必须使用 Python 的 `datetime` 对象。
+ Pandas 允许列名是其他数据类型而不仅仅是字符串。在 Polars 中，只允许使用字符串作为列名。
+ Pandas 会在引入缺失值时将整数列转换为浮点数。这是因为整数数据类型无法包含缺失值。而 Polars 不会这样做。
+ Pandas 在聚合时，静默地删除了具有缺失值的行（见上面的聚合示例）。Polars 保留所有组。
+ 在 Pandas 中，数据类型为 Object 的列可以同时包含字符串和浮点数。Polars 则没有 Object 类型的数据列。
+ Pandas 有多种方式来显示或处理缺失值：`None`，`NaN`，`NA`，`<NA>`，`NaT`，以及 `np.nan`。Polars 区分缺失值（`null`）和无效数字（`NaN`，表示“不是数字”）。
+ 在 Pandas 中，你可以用非布尔 Series 进行过滤。而在 Polars 中，你必须使用布尔 Series 进行过滤。
+ 在 Pandas 中，你可以将布尔 Series 与整数进行比较。在 Polars 中，你只能将布尔 Series 与布尔值（和布尔 Series）进行比较。
+ 在 Pandas 中，当只传递一个单一值时，方法和属性如 `df.loc[]` 会返回一个 Series 而不是 DataFrame。在这种情况下，Polars 返回一个只有一行的 DataFrame。

Polars 的严格性，在我们看来，是一个特性而不是一个缺陷。



<h1 id="sEYkg">**需要遗忘的语法**</h1>
一旦你成功忘记了这些概念，就剩下一些较小的细节问题，比如函数、方法和参数的不同名称。在忘记 Pandas 语法时（在必要的情况下）可能会有些烦恼，但文档通常足够帮助你快速解决这些问题。



---

**<font style="color:#ECAA04;">翻译过程中遗失的内容</font>**

你可能会尝试逐行将现有的 Pandas 代码翻译成 Polars 的代码。这就像逐字逐句地将荷兰语表达翻译成英语一样，最后会得到不符合惯例的代码，甚至会导致性能问题。也就是说，这通常不是一个好主意。

---

<h2 id="bffL7">**常见操作对比**</h2>
接下来的几节中，我们将一起查看一些常见的 DataFrame 操作，比较它们在 Pandas 和 Polars 中的实现方式。这个列表并不完整，但可以让你了解这两个库如何以不同方式处理同样的任务。我们也会指出输出上的任何差异。

<h3 id="LwwrV">**删除重复值**</h3>
让我们删除那些 `class` 列中包含重复值的行。在 Pandas 中，可以这样做：

```python
animals_pd.drop_duplicates(subset="class")
```

```plain
    animal   class  lifespan         status  weight
0  dolphin  mammal        40  least concern   150.0
1     duck    bird         8  least concern     3.0
9      ray    fish        20            NaN    90.0
```



在 Polars 中使用：

```python
animals_pl.unique(subset="class")
```

```plain
shape: (3, 5)
┌─────────┬────────┬──────────┬───────────────┬────────┐
│ animal  │ class  │ lifespan │ status        │ weight │
│ ---     │ ---    │ ---      │ ---           │ ---    │
│ str     │ str    │ i64      │ str           │ i64    │
╞═════════╪════════╪══════════╪═══════════════╪════════╡
│ dolphin │ mammal │ 40       │ least concern │ 150    │
│ ray     │ fish   │ 20       │               │ 90     │
│ duck    │ bird   │ 8        │ least concern │ 3      │
└─────────┴────────┴──────────┴───────────────┴────────┘
```



请注意方法名称的不同：`pd.df.drop_duplicates()` 和 `pl.df.unique()`。此外，Polars 不维护行的顺序（`ray` 出现在 `duck` 之前）。可以通过指定 `maintain_order=True` 来更改这一点，不过这会增加计算开销。

<h3 id="t0gtr">**删除缺失值**</h3>
让我们删除 `weight` 列中有缺失值的任何行。在 Pandas 中：

```python
animals_pd.dropna(subset="weight")
```

```plain
       animal   class  lifespan         status  weight
0     dolphin  mammal        40  least concern   150.0
1        duck    bird         8  least concern     3.0
2    elephant  mammal        60     endangered  8000.0
3        ibis    bird        16  least concern     1.0
4      impala  mammal        12  least concern    70.0
5        kudu  mammal        15  least concern   250.0
7       panda  mammal        20     vulnerable   100.0
8  polar bear  mammal        25     vulnerable   720.0
9         ray    fish        20            NaN    90.0
```



在 Polars 中：

```python
animals_pl.drop_nulls(subset="weight")
```

```plain
shape: (9, 5)
┌────────────┬────────┬──────────┬───────────────┬────────┐
│ animal     │ class  │ lifespan │ status        │ weight │
│ ---        │ ---    │ ---      │ ---           │ ---    │
│ str        │ str    │ i64      │ str           │ i64    │
╞════════════╪════════╪══════════╪═══════════════╪════════╡
│ dolphin    │ mammal │ 40       │ least concern │ 150    │
│ duck       │ bird   │ 8        │ least concern │ 3      │
│ elephant   │ mammal │ 60       │ endangered    │ 8000   │
│ ibis       │ bird   │ 16       │ least concern │ 1      │
│ impala     │ mammal │ 12       │ least concern │ 70     │
│ kudu       │ mammal │ 15       │ least concern │ 250    │
│ panda      │ mammal │ 20       │ vulnerable    │ 100    │
│ polar bear │ mammal │ 25       │ vulnerable    │ 720    │
│ ray        │ fish   │ 20       │               │ 90     │
└────────────┴────────┴──────────┴───────────────┴────────┘
```

请注意方法名称的不同：`pd.df.dropna()` 和 `pl.df.drop_nulls()`。

<h3 id="S9fvt">**排序行**</h3>
让我们按照 `weight` 列的降序（即体重最重的动物排在最前）对 DataFrame 进行排序。在 Pandas 中：

```python
animals_pd.sort_values("weight", ascending=False)
```

```plain
       animal   class  lifespan           status  weight
2    elephant  mammal        60       endangered  8000.0
8  polar bear  mammal        25       vulnerable   720.0
5        kudu  mammal        15    least concern   250.0
0     dolphin  mammal        40    least concern   150.0
7       panda  mammal        20       vulnerable   100.0
9         ray    fish        20              NaN    90.0
4      impala  mammal        12    least concern    70.0
1        duck    bird         8    least concern     3.0
3        ibis    bird        16    least concern     1.0
6     narwhal  mammal        40  near threatened     NaN
```



在 Polars 中：

```python
animals_pl.sort("weight", descending=True)
```

```plain
shape: (10, 5)
┌────────────┬────────┬──────────┬─────────────────┬────────┐
│ animal     │ class  │ lifespan │ status          │ weight │
│ ---        │ ---    │ ---      │ ---             │ ---    │
│ str        │ str    │ i64      │ str             │ i64    │
╞════════════╪════════╪══════════╪═════════════════╪════════╡
│ narwhal    │ mammal │ 40       │ near threatened │ null   │
│ elephant   │ mammal │ 60       │ endangered      │ 8000   │
│ polar bear │ mammal │ 25       │ vulnerable      │ 720    │
│ kudu       │ mammal │ 15       │ least concern   │ 250    │
│ dolphin    │ mammal │ 40       │ least concern   │ 150    │
│ panda      │ mammal │ 20       │ vulnerable      │ 100    │
│ ray        │ fish   │ 20       │                 │ 90     │
│ impala     │ mammal │ 12       │ least concern   │ 70     │
│ duck       │ bird   │ 8        │ least concern   │ 3      │
│ ibis       │ bird   │ 16       │ least concern   │ 1      │
└────────────┴────────┴──────────┴─────────────────┴────────┘
```



请注意方法名称的不同：`df.sort_values()` 与 `df.sort()`，以及用于逆序排序的参数：`ascending=False` 与 `descending=True`。此外，Pandas 会将缺失值排在最后，而 Polars 则将它们排在最前。你可以通过指定 `nulls_last=True` 来改变这一行为。

<h3 id="Q9LkB">**转换现有列的数据类型**</h3>
让我们将 `lifespan` 列的数据类型从整数转换为浮点数。在 Pandas 中：

```python
animals_pd.assign(lifespan=animals_pd["lifespan"].astype(float))
```

```plain
       animal   class  lifespan           status  weight
0     dolphin  mammal      40.0    least concern   150.0
1        duck    bird       8.0    least concern     3.0
2    elephant  mammal      60.0       endangered  8000.0
3        ibis    bird      16.0    least concern     1.0
4      impala  mammal      12.0    least concern    70.0
5        kudu  mammal      15.0    least concern   250.0
6     narwhal  mammal      40.0  near threatened     NaN
7       panda  mammal      20.0       vulnerable   100.0
8  polar bear  mammal      25.0       vulnerable   720.0
9         ray    fish      20.0              NaN    90.0
```

在实践中，大多数 Pandas 代码直接通过赋值（`=`）来更改列，而不是使用 `df.assign()` 方法。



在 Polars 中：

```python
animals_pl.with_columns(pl.col("lifespan").cast(pl.Float64))
```

```plain
shape: (10, 5)
┌────────────┬────────┬──────────┬─────────────────┬────────┐
│ animal     │ class  │ lifespan │ status          │ weight │
│ ---        │ ---    │ ---      │ ---             │ ---    │
│ str        │ str    │ f64      │ str             │ i64    │
╞════════════╪════════╪══════════╪═════════════════╪════════╡
│ dolphin    │ mammal │ 40.0     │ least concern   │ 150    │
│ duck       │ bird   │ 8.0      │ least concern   │ 3      │
│ elephant   │ mammal │ 60.0     │ endangered      │ 8000   │
│ ibis       │ bird   │ 16.0     │ least concern   │ 1      │
│ impala     │ mammal │ 12.0     │ least concern   │ 70     │
│ kudu       │ mammal │ 15.0     │ least concern   │ 250    │
│ narwhal    │ mammal │ 40.0     │ near threatened │ null   │
│ panda      │ mammal │ 20.0     │ vulnerable      │ 100    │
│ polar bear │ mammal │ 25.0     │ vulnerable      │ 720    │
│ ray        │ fish   │ 20.0     │                 │ 90     │
└────────────┴────────┴──────────┴─────────────────┴────────┘
```

Polars 使用 `pl.col()` 函数来创建表达式。表达式是 Polars 中的一个重要概念，你可以在 **第6章** 中详细了解。

<h3 id="aaFzB">**聚合行**</h3>
让我们按 `class` 和 `status` 列进行分组，然后计算每组的平均体重。你之前已经看过这个例子，但还是值得再次说明。在 Pandas 中：

```python
animals_pd.groupby(["class", "status"])[["weight"]].mean()
```

```plain
weight
class  status
bird   least concern       2.000000
mammal endangered       8000.000000
       least concern     156.666667
       near threatened          NaN
       vulnerable        410.000000
```



在 Polars 中：

```python
animals_pl.group_by(["class", "status"]).agg(pl.col("weight").mean())
```

```plain
shape: (6, 3)
┌────────┬─────────────────┬────────────┐
│ class  │ status          │ weight     │
│ ---    │ ---             │ ---        │
│ str    │ str             │ f64        │
╞════════╪═════════════════╪════════════╡
│ fish   │                 │ 90.0       │
│ mammal │ least concern   │ 156.666667 │
│ mammal │ endangered      │ 8000.0     │
│ mammal │ vulnerable      │ 410.0      │
│ mammal │ near threatened │ null       │
│ bird   │ least concern   │ 2.0        │
└────────┴─────────────────┴────────────┘
```

方法名称略有不同：`pd.df.groupby()` 与 `pl.df.group_by()`。Pandas 基于 `class` 和 `status` 列创建了一个多重索引，而 Polars 则保持它们作为列。此外，Polars 使用表达式在 `df.GroupBy.agg()` 方法内完成操作。

<h1 id="CxBLE">**要点总结**</h1>
在本章中，我们探讨了 Pandas 和 Polars 之间的相似点和不同点。关键要点包括：

+ 如果没有 Pandas，可能就没有 Polars。
+ Pandas 和 Polars 在高层次上有一些共同点。
+ 在外部看起来，Polars DataFrame 与 Pandas DataFrame 非常相似，但 Polars 列的形状和数据类型信息更加明确。
+ 你可以忘记一些 Pandas 特定的概念，比如索引、坐标轴、索引与切片、急切执行和松散性。
+ 许多方法和参数有不同的名称，这可能让人感到困惑甚至沮丧。
+ 对于某些操作，输出也会有些许不同。



---

<font style="color:#E4495B;">注释：</font>

1. **Pandas 通常以小写字母 "p" 书写为 "pandas"**。但在这本书中，我们将其写作大写的 "P"，以便与 Polars 区分开来。
2. **正如本书标题所示，我们专注于 Python 版的 Polars**。从技术上讲，Polars 是用 Rust 编写的，并为 Python、R 和 JavaScript 提供了绑定。
3. **在 Polars 中，几乎没有操作可以原地完成**，例如 `df.hstack()` 和 `df.shrink_to_fit()`，只有当你指定 `in_place=True` 时才能实现。
4. **“That puts no sods on the dike” 是荷兰语表达的直接翻译**，原句为“Dat zet geen zoden aan de dijk”。意思是某些事情是无关紧要的，或者对解决问题没有帮助。

