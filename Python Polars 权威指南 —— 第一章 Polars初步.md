<h1 id="mjdMu">概述</h1>
为了探索Polars提供的所有激动人心的功能，你需要首先启动并运行它。在本章中，你将设置工作环境。这意味着你将安装Polars，或者如果你愿意，也可以从源代码构建它。接着，你将学习如何根据你的需要配置Polars。你还将学习如何下载本书中使用的数据集和代码示例。最后，你将快速学习JupyterLab，这是你在本书中运行代码示例的环境。如果你遇到问题，你还可以在Docker容器中运行代码，这将在本章末尾进行解释。



建议你按照代码示例进行操作。当你在实际操作中学习新库时，它们会比仅仅阅读可能性的描述更容易被记住。

<h1 id="PV028">安装Polars</h1>
为了开始使用Polars，你需要先安装它！最新的安装Polars的方法可以随时在[GitHub页面](https://github.com/pola-rs/polars)上找到。以下部分基于撰写时的相关说明。



Polars支持不同使用场景的可选依赖项。在撰写本书时，Polars支持的可选依赖项如表1-1所示。



表 1-1. Polars 依赖项

| 标签 | 描述 |
| --- | --- |
| all | 安装所有可选依赖项（包括以下所有内容） |
| pandas | 安装与Pandas集成，用于数据与Pandas DataFrames/Series之间的转换 |
| numpy | 安装与numpy集成，用于数据与numpy数组之间的转换 |
| pyarrow | 使用PyArrow读取数据格式 |
| fsspec | 支持从远程文件系统读取 |
| connector x | 支持从SQL数据库读取数据 |
| xlsx2csv | 支持从Excel文件读取 |
| deltalake | 支持从Delta Lake表读取数据 |
| timezone | 时区支持，仅在Python版本<3.9或你使用的是Windows时需要 |


这些依赖项可以通过Polars一起安装，使用以下括号表示法。本书中我们将探索Polars的所有可能性，因此我们将安装所有可选依赖项。可以在Jupyter单元中运行以下命令：

```bash
$ pip install 'polars[all]'
```

如果你想安装部分依赖项，可以使用以下方式安装：

```bash
$ pip install 'polars[pandas,numpy]'
```

如果你只想安装基本包，安装Polars最新版本的最佳方式是使用`pip`：

```bash
$ pip install polars
```

另外，有些人使用`conda`来管理包：

```bash
$ conda install -c conda-forge polars
```

然而，`pip`是Polars团队首选的安装方式。



<h1 id="F3oVE">从源码编译Polars</h1>
从源码编译Polars代码有几个优势。尽管对于Polars来说不太可能需要这样做，因为它的发布非常频繁，但从源码编译可以让你立即获得最新的更改。从源码编译还允许你对源代码进行修改，自行重新编译，并利用你自己的自定义功能。（如果这对大家都有帮助，请务必贡献给项目。）如果你正在一个非标准架构上工作，自己编译代码有时是必要的，因为预编译的版本可能无法使用。如果你非常清楚自己在做什么，还可以在编译自己的代码时调整编译器优化设置，从而可能实现更高效或更快速的软件。

从源码编译Polars所需的步骤如下：

1. 按照[下载页面](https://rust-lang.org/)上的说明安装Rust编译器。
2. 安装`maturin`，一个零配置包，可以帮助构建和发布带有Python绑定的Rust crates。

```bash
$ python -m pip install maturin
```

3. 编译二进制文件。有两种方式编译二进制文件：
    1. 如果你优先考虑运行时性能而不是构建时间长短（例如只构建一次包，并以最大性能运行）：

```bash
$ cd py-polars
$ maturin develop --release -- -C target-cpu=native
```

    2. 如果你优先考虑更快的构建时间而不是快速性能（例如在开发和测试更改时）：

```bash
$ cd py-polars
$ maturin develop --release -- -C codegen-units=16 -C lto=thin -C target-cpu=native
```

---

**注意**  
Rust crate实现了Python绑定，名为`py-polars`，以区分包装的Rust crate Polars本身。然而，Python包和Polars模块都命名为`polars`，因此你可以方便地运行`pip install polars`并`import polars`。

---

<h2 id="pshqV">边缘情况：非常大的数据集</h2>
如果你要处理非常大的数据集，超过42亿行，则需要以不同的方式安装Polars。内部的Polars使用32位整数表示来追踪数据。如果数据集增长超过这个限制，Polars需要使用`bigidx`功能标志来编译，以便内部表示能够反映出来。另外，你也可以通过运行以下命令来安装：

```bash
$ pip install polars-u64-idx
```

如果不需要这个功能，这可能会导致性能下降。

<h2 id="AMAgh">边缘情况：缺少AVX支持的处理器</h2>
高级矢量扩展（AVX）指的是对x86指令集架构所做的扩展。这些扩展允许在CPU级别执行更复杂和高效的计算操作。此特性集首次在2011年发布的Intel和AMD的CPU中实现。很遗憾，在此之前的处理器上无法使用这些功能，基于ARM架构的Apple Silicon也不支持。如果你使用的是不支持AVX的芯片组，则需要安装`polars-lts-cpu`。该软件包也可以在PyPI上找到，并可以通过以下命令安装：

```bash
$ pip install polars-lts-cpu
```

---

**注意**  
如果你自己编译这个软件包，请注意只能用Rust的nightly版本进行编译。稳定版本不允许使用带有AVX特性标志的构建，并且会抛出E0554错误。你可以通过运行以下命令下载并设置nightly版本为默认版本：

```bash
$ rustup install nightly
$ rustup default nightly
```

---

**警告**  
如果你运行需要Rust稳定版本的其他项目，此命令可能会干扰它们。要切换回Rust的稳定分支，请运行以下命令：

```bash
$ rustup default stable
```

---



<h1 id="cvhJK">配置Polars</h1>
Polars提供了许多配置设置。这些选项允许你启用实验性功能  ，改变打印表格的格式，设置日志级别，以及设置流块大小。在`polars.Config`类中，你可以找到以下设置，以及一些我们未涉及的其他配置。完整的概述可以在[Polars配置文档](https://docs.pola.rs/api/python/stable/reference/config.html)中找到。以下部分是该文档的摘录。



最重要的设置如表1-2所示。



表1-2：一些重要的Polars配置设置

| 设置 | 描述 |
| --- | --- |
| activate_decimals(active: bool) | Decimal数据类型目前是alpha版。你需要手动打开它。 |
| set_fmt_str_lengths(n: int) | 设置用于显示字符串值的字符数 |
| set_tbl_cols(n: int) | 设置显示表格时可见的列数 |
| set_streaming_chunk_size(size: int) | 覆盖streaming引擎中使用的块大小 |
| set_verbose(active: bool) | 启用额外的详细/调试日志记录 |


这些配置选项可以通过使用`load(cfg: str | Path)`和`save(file: str | Path)`函数更改、保存和加载为JSON字符串。要查看当前状态，可以调用`state()`。要将所有设置恢复为默认值，可以调用`restore_defaults()`。



<h2 id="n6Tjo">使用上下文管理器进行临时配置</h2>
要在特定代码范围内使用不同的配置，可以使用上下文管理器。上下文管理器是Python中的一种结构，允许精确地创建和删除资源。调用上下文管理器并缩进代码范围，可以表示应受其影响的代码。在Polars的情况下，只有上下文管理器范围内的代码会使用给定的配置执行，执行完毕后返回到之前的设置。

```python
import polars as pl

with pl.Config() as cfg:
    cfg.set_verbose(True)
    # 你想要查看详细日志的Polars操作

# 上下文管理器之外的代码不受影响
```

一个更简洁的方法是直接将选项作为参数传递给`Config()`构造函数。如果你使用这种方法，可以省略选项的`set_`部分。

```python
with pl.Config(verbose=True):
    # 你想要查看详细日志的Polars操作
    pass
```

为了展示一些格式配置设置，你将生成第一个DataFrame。DataFrame是一个二维数据结构，表示行和列组成的表格数据。这是Polars中使用的主要数据结构之一。本书稍后会更深入地介绍所有结构。

在以下代码中，我们编写了一个简短的函数，能够生成一个可以设置长度的随机字符串。之后我们创建一个字典，包含从`"column_1"`到`"column_20"`的键，以及5行随机生成的50个字符长的字符串。

```python
import random
import string

def generate_random_string(length: int) -> str:
    return "".join(random.choice(string.ascii_letters) for _ in range(length))

data = {}
for i in range(1, 11):
    data[f"column_{i}"] = [generate_random_string(50) for _ in range(5)]

df = pl.DataFrame(data)
```

让我们看看当你运行代码时，这个DataFrame是什么样子的：

```python
df
```

```python
shape: (5, 10)
┌────────────┬────────────┬────────────┬───┬───────────┬───────────┬───────────┐
│ column_1   │ column_2   │ column_3   │ … │ column_8  │ column_9  │ column_10 │
│ ---        │ ---        │ ---        │   │ ---       │ ---       │ ---       │
│ str        │ str        │ str        │   │ str       │ str       │ str       │
╞════════════╪════════════╪════════════╪═══╪═══════════╪═══════════╪═══════════╡
│ NITxKLUkXv │ vrLgRRjGXL │ ErZIZfRrEq │ … │ beymgYVfd │ bIghJrUqO │ HGdFNGSPa │
│ yOYxtzSnWQ │ QPcPJFsbjj │ jUgWnjTSkj │   │ LsnHFrZmS │ JqRwQUErd │ BmfvCdhzj │
│ …          │ …          │ …          │   │ Vg…       │ Zq…       │ vl…       │
│ MqZmJeOHNK │ ubSBwYOgYk │ HQdUpgsJus │ … │ eCkqtkOlh │ WAXfTTOBr │ vMRyUWIKs │
│ XceAPNdRbO │ fTatOQmkRm │ uscqAuvSfP │   │ sGftkqIII │ PsfVWUnPQ │ NxGcuadnN │
│ …          │ …          │ …          │   │ ox…       │ Fu…       │ Iq…       │
│ HlXQdVFTVL │ ybaZRpdIJh │ VtnYHFRNNA │ … │ LPZvTIIwV │ SkjhgiCfk │ WFNCaqjtg │
│ DbZHFIWPUw │ PzrHJsjSaA │ KCTLizyVyl │   │ UqtjLJOoU │ eDxeEcShL │ aEadCeEDR │
│ …          │ …          │ …          │   │ jW…       │ Rg…       │ Aw…       │
│ dODwyenwQR │ BmfJOHYZkA │ JbXtfyUyNG │ … │ NOtdYhuJy │ dbjtoFjvZ │ yQFKPBjQV │
│ PMqTnmiEzN │ oMfoGlBbBH │ DXbgdKpXjo │   │ yTiStIGcI │ NZlgFFPGW │ vjgyHvJrC │
│ …          │ …          │ …          │   │ jZ…       │ EW…       │ jA…       │
│ IuXzwotCLy │ cZzaPcRNBU │ vTlcINzgBB │ … │ kLtrJblaW │ ZxIcqHlie │ WpcBNWzeo │
│ OyjNrWVpyT │ DfuMHNCJGn │ zoCWOeoaTT │   │ xtcSpyOnC │ MgqgEqCXh │ OXJFltrfa │
│ …          │ …          │ …          │   │ ow…       │ DU…       │ uf…       │
└────────────┴────────────┴────────────┴───┴───────────┴───────────┴───────────┘
```

遗憾的是，标准的DataFrame输出在这本书中无法完全展示。如果你想要尽可能看到更多的列，但又不影响显示效果，可以通过缩小显示文本的方式来实现。在这种情况下，你可以将显示的列数设置为-1（表示显示所有列），并将字符串的显示长度设为4。

```python
with pl.Config(tbl_cols=-1, fmt_str_lengths=4):
    print(df)
```

```plain
shape: (5, 10)
┌─────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┐
│ col │ colu… │ colu… │ colu… │ colu… │ colu… │ colu… │ colu… │ colu… │ colu… │
│ u…  │ ---   │ ---   │ ---   │ ---   │ ---   │ ---   │ ---   │ ---   │ ---   │
│ --- │ str   │ str   │ str   │ str   │ str   │ str   │ str   │ str   │ str   │
│ str │       │       │       │       │       │       │       │       │       │
╞═════╪═══════╪═══════╪═══════╪═══════╪═══════╪═══════╪═══════╪═══════╪═══════╡
│ NIT │ vrLg… │ ErZI… │ aXoG… │ zQXd… │ BqAF… │ PrRN… │ beym… │ bIgh… │ HGdF… │
│ x…  │       │       │       │       │       │       │       │       │       │
│ MqZ │ ubSB… │ HQdU… │ jvRg… │ zcDr… │ Pees… │ Zqsj… │ eCkq… │ WAXf… │ vMRy… │
│ m…  │       │       │       │       │       │       │       │       │       │
│ HlX │ ybaZ… │ VtnY… │ JFHN… │ EXzX… │ aBdy… │ QkOA… │ LPZv… │ Skjh… │ WFNC… │
│ Q…  │       │       │       │       │       │       │       │       │       │
│ dOD │ BmfJ… │ JbXt… │ rHAq… │ pwJO… │ oRCW… │ OgCG… │ NOtd… │ dbjt… │ yQFK… │
│ w…  │       │       │       │       │       │       │       │       │       │
│ IuX │ cZza… │ vTlc… │ JNjW… │ OEuZ… │ AXWe… │ eQTy… │ kLtr… │ ZxIc… │ WpcB… │
│ z…  │       │       │       │       │       │       │       │       │       │
└─────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘
```

显示结果紧凑，但仍然展示了所有的列，非常完美。

---

**<font style="color:#117CEE;">注意</font>**

上下文管理器在底层包含两个关键方法。它们分别是`__enter__`和`__exit__`，分别在执行指定上下文内的代码之前和之后调用。一个简单的例子如下：

```python
class YourContextManager:
    def __enter__(self):
        print("Entering context")

    def __exit__(self, type, value, traceback):
        print("Exiting context")

with YourContextManager():
    print("Your code")
```

输出：

```plain
Entering context
Your code
Exiting context
```

上下文管理器的一个常见用法是读写文件，可以这样使用：

```python
with open("filename.txt", "w") as file:
    file.write("Hello, world!")
```

---

<h2 id="XGE3k">使用装饰器进行局部配置</h2>
如果你想在特定函数调用期间更改配置设置，可以使用`pl.Config()`装饰器装饰该函数。和上下文管理器一样，可以省略选项中的`set_`部分。

```python
@pl.Config(ascii_tables=True)
def write_ascii_frame_to_stdout(df: pl.DataFrame) -> None:
    print(str(df))

@pl.Config(verbose=True)
def function_that_im_debugging(df: pl.DataFrame) -> None:
    # 你想查看详细日志的Polars操作
    pass
```



<h1 id="mxDyP">下载数据集和代码示例</h1>
为了运行本书中的代码示例，你需要下载使用的数据集，使用`git`。`git`是一个版本控制系统，可以用来下载代码仓库，并跟踪其中的更改。你可以按照[Git官网](https://git-scm.com)上的说明安装`git`。

本书中的数据集可以在伴随本书的仓库中找到。下载并安装`git`后，你可以运行以下命令下载该仓库。这将会在当前工作目录中创建一个名为`python-polars-the-definitive-guide`的新目录。

```bash
$ git clone https://github.com/jeroenjanssens/python-polars-the-definitive-guide
```

你可以运行以下命令安装本书代码示例所需的依赖项：

```bash
$ cd python-polars-the-definitive-guide
$ pip install -r requirements.txt
```

这将设置你的系统以便能够配合本书工作。



<h1 id="Ncy6v">JupyterLab 快速入门</h1>
为了运行本书中的代码示例，你需要使用Jupyter。Jupyter是一个基于网页的交互式开发环境，支持笔记本、代码和数据。要开始使用，你可以创建一个Python 3笔记本，点击顶行的按钮。



要启动Jupyter，你可以在终端中运行以下命令：

```bash
$ jupyter lab
```

这将会在浏览器中打开一个包含Jupyter界面的窗口。如果没有弹出窗口，你需要复制终端中打印的URL。它看起来像这样：

```plain
http://127.0.0.1:8888/lab?token=...
```



点击它，或将其复制并粘贴到浏览器窗口中，连接到Jupyter服务器。这样你就可以在浏览器中打开JupyterLab并开始工作。

要在Jupyter笔记本中工作，你需要了解一些基础知识。Jupyter中的内容是以单元格的形式加载的。这些单元格可以标记为不同的编程语言，也可以是Markdown格式。在本书中，你将主要使用Python代码单元格。为了在这些单元格之间导航和编辑，Jupyter有两种模式：**命令模式**和**编辑模式**。

命令模式是打开笔记本时的默认模式，或者在某个单元格中按下`Esc`键时激活。当命令模式激活时，选中的单元格会有一个蓝色边框，且内部没有光标。命令模式用于整体编辑笔记本，或者添加、删除、编辑单元格类型。

编辑模式可以通过在选中的单元格上按`Enter`键激活。在编辑模式下，选中的单元格会有一个绿色边框。编辑模式用于在单元格中输入内容。

<h2 id="PLoEY">键盘快捷键</h2>
你应该知道的一些重要快捷键如下表所示。表1-3列出了可以在任何模式下运行的快捷键，表1-4列出了可以在命令模式下运行的快捷键，表1-5列出了可以在编辑模式下运行的快捷键。

<h3 id="KGrKw">**任何模式下的快捷键：**</h3>
| 快捷键 | 功能 |
| --- | --- |
| Shift + Enter | 运行选中的单元，并选择下面的单元 |
| Ctrl + Enter | 运行选中的单元，但不移动选择 |
| Alt + Enter | 运行选中的单元，并在其下插入新单元 |
| Ctrl + S | 保存笔记本 |


<h3 id="l96WL">**命令模式下的快捷键：**</h3>
| 快捷键 | 功能 |
| --- | --- |
| Enter | 切换到编辑模式 |
| Up/K | 选择上面的单元 |
| Down/J | 选择下面的单元 |
| A | 在当前单元上方插入新单元 |
| B | 在当前单元下方插入新单元 |
| D,D | 删除选中的单元 |
| Z | 撤销删除的单元 |
| M | 将单元类型更改为Markdown |
| Y | 将单元类型更改为代码 |


<h3 id="R4BuI">**编辑模式下的快捷键：**</h3>
| 快捷键 | 功能 |
| --- | --- |
| Esc | 切换到命令模式 |
| Ctrl + Shift + - | 在光标处拆分当前单元 |




请随时掌握这些快捷键，不久你就能在任何Jupyter笔记本中快速操作。

此外，Jupyter中有一些可以在代码单元格中使用的特殊标记。例如，感叹号（`!`）用于告诉Jupyter内核在bash会话中运行该命令，而不是将其解释为Python代码。例如，你可以在笔记本中使用以下命令安装Polars：

```bash
!pip install polars
```

另一个可以使用的特殊标记是百分号（`%`）。百分号标记是IPython内核的一个特殊功能，称为魔法命令。魔法命令是内置命令，旨在解决一些常见问题。这些命令不是Python语言的一部分，而是IPython shell的功能。magic命令分为两种类型：

1. **行magic**：这些命令前面有一个`%`，功能类似于shell命令。在这个例子中，我们使用`%pip`来安装在IPython shell的虚拟环境中运行的包。
2. **单元magic**：这些命令前面有两个`%%`。例如`%%time`可以显示该单元格中代码的运行时间，`%%bash`可以用来一次执行多个bash命令。

要查看IPython shell提供的所有其他命令，你可以运行以下命令 `%lsmagic`



<h1 id="lYgPz">在Docker容器中使用Polars</h1>
世界上有许多不同的系统配置，可能会遇到在执行代码时出现问题。在这种情况下，你可以选择使用Docker。Docker允许你在一个容器中运行本书中的代码，容器中我们可以精确控制系统配置的样子。这样我们可以确保代码在你的系统上像在我们系统上一样运行。

要开始，你需要拉取Docker镜像。镜像可以看作是构建你运行代码的容器的指令。本书中使用的镜像由Jupyter提供，因为你将从笔记本中运行代码。要拉取Jupyter镜像，你首先需要Docker。

前往[Docker官网](https://www.docker.com)并下载适合你操作系统的Docker Desktop。下载完成后，安装并启动它。

现在Docker正在后台运行，你可以打开终端（Windows上称为命令提示符）。在其中运行以下命令来运行镜像：

```bash
$ docker run -p 8888:8888 jupyter/minimal-notebook
```

此命令将尝试运行镜像`jupyter/minimal-notebook`。如果系统上没有此镜像，它将从DockerHub拉取。之后，Docker将启动容器，并将端口8888暴露给系统，这样你就可以在浏览器中打开在Docker中运行的Jupyter服务器。



<h1 id="kV4Jo">总结</h1>
在本章中，你已经学习了如何：

+ 安装Polars和可选的依赖项，并在必要时从源代码编译它。
+ 调整Polars的配置，使其达到最佳状态。
+ 下载本书中使用的数据集和代码示例。
+ 在JupyterLab中运行代码示例。
+ 在遇到问题时，在Docker容器中运行代码示例。

通过这些内容，你可以自己运行Polars并开始探索它带来的机会。在下一章中，你可以深入探讨Polars与流行的DataFrame库Spark和Pandas的相似之处和不同之处。

