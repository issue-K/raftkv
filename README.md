# 6.5840 Lab 1: MapReduce



我们在```src/main/mrsequence.go```中为您提供了一个简单的顺序```mapreduce```实现。它在一个进程中一次运行```map```和```reduce```。我们还为您提供了两个```MapReduce```应用程序.

```mrapps/wc.go```是一个```word-count```程序, ```mrapps/index.go```是一个文本索引器程序。

可以按如下顺序运行```word-count```

```shell
$ cd ~/6.5840
$ cd src/main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
A 509
ABOUT 2
ACT 8
...
```

```mrsequential.go```将其输出保留在文件```mr-out-0```中。输入来自名为```pg-xxx.txt```的文本文件。

你可以看看```mrsequential.go```的代码.

你也应该看看```mrapps/wc.go```来了解一个```MapReduce```程序是怎样的.

### Your Job

​	你的任务是实现一个分布式的```MapReduce```，它由两个程序组成:协调器(```coordinator```)和工作器(```worker```)。只有一个```coordinator```进程和一个或多个并行执行的```worker```进程。在实际系统中，```worker```会在许多不同的机器上运行，但在这个实验中，你将在一台机器上运行它们。```worker```将通过```RPC```与```coordinator```通信。每个```worker```会向```coordinator```请求任务，从一个或多个文件中读取任务的输入，执行任务，并将任务的输出写入一个或多个文件。```coordinator```应该注意到一个```worker```是否没有在合理的时间内完成其任务(在本实验为10秒)，并将相同的任务交给不同的```worker```。
​	我们已经给了你一些代码来开始。```coordinator```和```worker```的“主要”程序在```main/mrcoordinator.go```和```main/mrworker.go```中; 不要更改这些文件. 你应该将您的实现放在```mr/coordinator.go```, ```mr/worker.go``` 和```mr/rpc.go```中.

下面展示了如何在```word-count MapReduce```应用上运行你的代码。

首先，确保单词计数插件是新构建的:

```shell
$ go build -buildmode=plugin ../mrapps/wc.go
```

在主目录下运行```coordinator```

```shell
$ rm mr-out*
$ go run mrcoordinator.go pg-*.txt
```

将```pg-*.txt```参数传递给```mrcoordinator.go```作为输入文件. 每个文件对应一个“拆分”，是一个```Map```任务的输入。

在一个或多个窗口中运行一些```worker```

```shell
go run mrworker.go wc.so
```

当```worker```和```coordinator```完成后，查看```mr-out-*```中的输出。当你完成实验后，输出文件的排序并集应该与顺序输出匹配，如下所示:

```shell
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```

我们为您提供一个测试脚本```main/test-mr.sh```。当输入```pg-xxx.txt```文件时，测试检查```wc```和```indexer```的 MapReduce应用程序是否生成正确的输出。测试还会检查你的实现是否并行运行```Map```和```Reduce```任务，以及你的实现是否能从运行任务时崩溃的```worker```中恢复。

如果你现在运行测试脚本，它会挂起，因为```coordinator```永远不会结束:

```shell
$ cd ~/6.5840/src/main
$ bash test-mr.sh
*** Starting wc test.
```



### A few rules:

- ```map```阶段应该将中间```key```划分为```nReduce```个```reduce```任务的桶，其中```nReduce```是```reduce```任务的数量——```main/mrcoordinator.go```传递给```MakeCoordinator()```的参数。每个```mapper```应该创建```nReduce```个中间文件，供```reduce```任务使用。
- ```worker```实现应该把第$X$个```reduce```任务的输出放在文件```mr-out-X```中。
- 一个```mr-out-X```文件应该包含每个Reduce函数输出的一行。这一行应该使用 ```"%v %v"```格式生成，用```key```和```value```调用。```main/mrsequential.go```中有关于格式的具体描述.
- 你可以修改```mr/worker.go, mr/coordinator.go,  mr/rpc.go```。你可以临时修改其他文件进行测试，但要确保你的代码能够与原始版本兼容;我们将使用原始版本进行测试。
- ```worker```应该把```Map```的中间输出放在当前目录下的文件中，你的```worker```稍后可以把它们作为输入读取到```Reduce```任务中。
- ```main/mrcoordinator.go```预期```mr/coordinator.go```会实现一个```Done()```方法, 当它返回```true```时代表```MapReduce```任务全部完成, 然后```mrcoordinator.go```会```exit```.
- 当```job```完成时，```worker```进程应该退出。实现这一点的一个简单方法是使用```call()```的返回值: 如果```worker```没有联系到```coordinator```，它可以假设```coordinator```已经退出，因为工作已经完成，因此```worker```也可以终止。根据你的设计，你可能会发现设置一个```"please exit"```的伪任务让```coordinator```将其交给```worker```会很有用.

### Hints

- 一种开始的方式是修改```mr/worker.go```的```Worker()```来发送RPC给```coordinator```以请求任务. 然后修改```coordinator```响应一个还没有开始执行的```map```任务的文件名. 然后修改```worker```来读取这个文件并调用```application Map function```，就像```mrsequential.go```那样。

- 应用程序的Map和Reduce函数是在运行时使用```Go plugin package```从以.so结尾的文件中加载的。

- 当你修改```mr/```下的任何文件, 就需要重新构建你使用的所有```MapReduce```插件

  ```shell
  go build -buildmode=plugin ../mrapps/wc.go
  ```

- 这个实验依赖于```worker```共享一个文件系统。当所有```worker```进程运行在同一台机器上时，这很简单，但如果工作进程运行在不同的机器上，则需要像GFS这样的全局文件系统。

- 中间文件的一个合理命名约定是```mr-X-Y```，其中```X```是Map任务号，```Y```是reduce任务号。

- ```worker```的map任务代码需要在文件中存储中间的键值对，并且保证在reduce任务中可以正确读取。一种做法是使用Go的```encoding/json```包。将```JSON```格式的键值对写入打开的文件

  ```go
    enc := json.NewEncoder(file)
    for _, kv := ... {
      err := enc.Encode(&kv)
  ```

  写入如下所示

  ```go
    dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      if err := dec.Decode(&kv); err != nil {
        break
      }
      kva = append(kva, kv)
    }
  ```

- ```worker```的```map```部分可以使用```ihas(key)```函数(在```worker.go```中)选择给```reduce task```哪些```key```

- 你可以从```mrsequential.go```中"借鉴"一些代码。例如读取Map输入文件，对Map和Reduce之间的键值对进行排序，以及将Reduce的输出存储在文件中。

- ```coordinator```作为```rpc```的服务器是并发的, 不要忘记给数据加锁.

- worker有时需要等待，例如，直到最后一个map完成，reduce才能启动。一种可能是，worker周期性地向coordinator请求工作，在每次请求之间使用time.Sleep()睡眠。另一种可能是coordinator中相关的RPC处理程序有一个等待循环，可以使用time.Sleep()或sync.Cond。Go在自己的线程中为每个RPC运行处理程序，因此一个处理程序正在等待的事实不会阻止coordinator处理其他RPC。

- 协调器无法可靠地区分崩溃的worker、还活着但由于某种原因停止工作的worker，以及正在执行但速度太慢而无法发挥作用的worker。你能做的最好的事情是让协调器等待一段时间，然后放弃并重新将任务发送给另一个worker。对于这个实验室，让协调者等待十秒钟;在这之后，协调器应该假定worker已经死亡(当然，它也可能没有死亡)。

- 如果你选择实现备份任务(第3.6节)，请注意，我们测试了你的代码在worker执行任务没有崩溃时不会调度多余的任务。备份任务应该只安排在一段相对较长的时间之后(例如10秒)。

- 为了确保没有人在崩溃的情况下观察到部分写入的文件，那篇关于MapReduce的论文提到了使用临时文件的技巧，并在文件写入完成后原子性地重命名它。你可以使用```ioutil.TempFile```用于创建一个临时文件并使用```os.Rename```以原子性地重命名它。



### My Implement

- 将```Map```调用的输入数据切割为```M```个分区, 在本实验中, 每个分区就是一个输入文件
- ```Worker```需要主动向```coordinator```请求任务, 然后```coordinator```会分配一个```Map```任务(指定输入文件)给它.(如果所有```Map```任务完成, 就可以分配```Reduce```任务给```Worker```).

​		为了方便```Worker```退出, 当完成所有任务时, ```coordinator```可以分配一个```exit```任务让```Worker```终止.

​		```Worker```需要周期性得向```coordinator```来请求任务, 这个可以简单的用```time.Sleep()```实现

- 被分配了```Map```任务的```Worker```程序读取对应的输入文件,  然后将```Map```处理后产生的中间```key```分为```nReduce```个不同的分区文件, 通知```coordinator```
- 当所有```Map```任务完成后, ```coordinator```才能开始为```Worker```分配```reduce```任务, 并把所有分配给该```reduce```任务的中间文件名传递过去

- ```reduce worker```读取它需要处理的所有分区文件, 将```Reduce```调用产生的结果生成一个结果文件, 第```X```个```reduce worker```的输出文件取名为```mr-out-X```。

​		当然, ```reduce worker```完成任务后需要给```coordinator```通知

- 当```coordinator```收到所有```Reduce```任务完成的通知时, 就可以退出了.(根据你的实现, 可能需要先让```Worker```退出)

还有额外的一点是, 如果一个任务长时间没有得到```Worker```的响应, 需要把该任务交给另一个```Worker```完成.

