# Spark

## K 均值算法(K-means)

### K-means 聚类算法原理
聚类分析是一个无监督学习 (Unsupervised Learning) 过程, 一般是用来对数据对象按照其特征属性进行分组，经常被应用在客户分群，欺诈检测，图像分析等领域。K-means 应该是最有名并且最经常使用的聚类算法了，其原理比较容易理解，并且聚类效果良好，有着广泛的使用。

和诸多机器学习算法一样，K-means 算法也是一个迭代式的算法，其主要步骤如下:

1. 第一步，选择 K 个点作为初始聚类中心。
2. 第二步，计算其余所有点到聚类中心的距离，并把每个点划分到离它最近的聚类中心所在的聚类中去。在这里，衡量距离一般有多个函数可以选择，最常用的是欧几里得距离 (Euclidean Distance), 也叫欧式距离。公式如下：![](https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice4/img00.png),其中 C 代表中心点，X 代表任意一个非中心点。
3. 第三步，重新计算每个聚类中所有点的平均值，并将其作为新的聚类中心点。
4. 最后，重复 (二)，(三) 步的过程，直至聚类中心不再发生改变，或者算法达到预定的迭代次数，又或聚类中心的改变小于预先设定的阀值。

### 在实际应用中，K-means 算法有两个不得不面对并且克服的问题。

1. 聚类个数 K 的选择。K 的选择是一个比较有学问和讲究的步骤，我们会在后文专门描述如何使用 Spark 提供的工具选择 K。
2. 初始聚类中心点的选择。选择不同的聚类中心可能导致聚类结果的差异。

Spark MLlib K-means 算法的实现在初始聚类点的选择上，借鉴了一个叫 K-means||的类 K-means++ 实现。K-means++ 算法在初始点选择上遵循一个基本原则: 初始聚类中心点相互之间的距离应该尽可能的远。基本步骤如下:

1. 第一步，从数据集 X 中随机选择一个点作为第一个初始点。
2. 第二步，计算数据集中所有点与最新选择的中心点的距离 D(x)。
3. 第三步，选择下一个中心点，使得 ![](https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice4/img01.png) 最大。
4. 第四部，重复 (二),(三) 步过程，直到 K 个初始点选择完成。

### MLlib 的 K-means 
Spark MLlib 中 K-means 算法的实现类具有以下参数，具体如下。

- `k` 表示期望的聚类的个数。
- `maxInterations` 表示方法单次运行最大的迭代次数。
- `runs` 表示算法被运行的次数。K-means 算法不保证能返回全局最优的聚类结果，所以在目标数据集上多次跑 K-means 算法，有助于返回最佳聚类结果。
- `initializationMode` 表示初始聚类中心点的选择方式, 目前支持随机选择或者 K-means||方式。默认是 K-means||。
- `initializationSteps`表示 K-means||方法中的部数。
- `epsilon` 表示 K-means 算法迭代收敛的阀值。
- `seed` 表示集群初始化时的随机种子。

通常应用时，我们都会先调用 KMeans.train 方法对数据集进行聚类训练，这个方法会返回 KMeansModel 类实例，然后我们也可以使用 KMeansModel.predict 方法对新的数据点进行所属聚类的预测，这是非常实用的功能。

KMeansModel.predict 方法接受不同的参数，可以是向量，或者 RDD，返回是入参所属的聚类的索引号。

### 聚类测试数据集简介
参考博文中目标数据集是来自 UCI Machine Learning Repository 的 [Wholesale customer Data Set](http://archive.ics.uci.edu/ml/datasets/Wholesale+customers)。UCI 是一个关于机器学习测试数据的下载中心站点，里面包含了适用于做聚类，分群，回归等各种机器学习问题的数据集。

哎!想想博文的作者,我还得更加的努力啊!

我这里使用简单的训练数据,测试数据则是写入的稀疏向量.代码详见 GitHub Spark-Learning .就是参考的官方样例.

### 如何选择 K
前面提到 K 的选择是 K-means 算法的关键，Spark MLlib 在 KMeansModel 类里提供了 `computeCost` 方法，该方法通过计算所有数据点到其最近的中心点的平方和来评估聚类的效果。一般来说，同样的迭代次数和算法跑的次数，这个值越小代表聚类的效果越好。但是在实际情况下，我们还要考虑到聚类结果的可解释性，不能一味的选择使 `computeCost` 结果值最小的那个 K。通过学习 吴恩达老师的课程里面有讲到一个 “肘部法则”,此时是拐点,作为聚类数选择的依据.

### 分析

#### 关键代码片段

```java
String inputFile = "./kmeans_data.txt";
    int k = 2;
    int iterations = 3;
    int runs = 1;
      
    SparkConf sparkConf = new SparkConf().setAppName("JavaKMeans");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = sc.textFile(inputFile);
      
    JavaRDD<Vector> points = lines.map(new ParsePoint());


    KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs, KMeans.K_MEANS_PARALLEL());

    System.out.println("Cluster centers:");
    for (Vector center : model.clusterCenters()) {
      System.out.println(" " + center);
    }
    double cost = model.computeCost(points.rdd());
    System.out.println("Cost: " + cost);


    System.out.println("Predict: " + model.predict(Vectors.dense(98, 345, 90)));

    System.out.println("Predict: " + model.predict(Vectors.dense(0.1, 0, 0.1)));

    sc.stop();
```

#### 训练数据 和 测试数据
kmeans_data.txt 训练数据是三维的

```
0.0 0.0 0.0
0.1 0.1 0.1
0.2 0.2 0.2
9.0 9.0 9.0
9.1 9.1 9.1
9.2 9.2 9.2
```

测试数据为稀疏向量:(98, 345, 90),(0.1, 0, 0.1)

#### 输出结果

```
Cluster centers:
[0.1,0.1,0.1]
[9.1,9.1,9.1]

Cost: 0.11999999999994547
Predict: 1
Predict: 0
```

##### 分析
测试里面,没有根据 "肘部法则" 确定,而是写为 2.所以,两个聚类中的坐标为 `[0.1,0.1,0.1]` 和 `[9.1,9.1,9.1]`.模型分析测试用例,则分别在 1 和 0 聚类.这个结果是合理的.

## 总结
机器学习的算法有很多,常用的也不少.各类语言的具体实现也可以供多种选择.就目前 Spark 对于分布式计算的支持和结果迭代是比较好的,在生产中也有不少应用.当然 Python作为学习语言也比较适合用的.原生的 scala 了当然就更好啦!

## 参考
1. [Spark 实战，第 4 部分: 使用 Spark MLlib 做 K-means 聚类分析](https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice4/#ibm-pcon)
2. [Clustering - spark.mllib](https://spark.apache.org/docs/2.0.0-preview/mllib-clustering.html#k-means)