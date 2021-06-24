### About

#### Description

This spark-optimization-features repo is a Spark Application developed in Scala to attribute creation

#### Target

Introduce some data processing to be able to practice Spark resources optimize at Serasa Experian DataLab meetup

### Links

#### Kaggle Dataset

https://www.kaggle.com/kartik2112/fraud-detection

#### Sparklens

Project [repository](https://github.com/qubole/sparklens) [package](https://repos.spark-packages.org/qubole/sparklens/0.3.2-s_2.11/sparklens-0.3.2-s_2.11.jar)

### Application

#### commands to test/build

assembly create a fat-jar with all necessary dependencies

```bash
sbt clean
sbt test
sbt assembly
```

#### run

```bash
spark-submit --master yarn --conf spark.default.parallelism=50 \
 --conf spark.sql.shuffle.partitions=50 \
 --conf spark.sql.parquet.compression.codec=snappy \
 --conf spark.dynamicAllocation.enabled=false \
 --conf spark.network.timeout=360000 \
 --conf spark.shuffle.service.enabled=false \
 --conf spark.sql.autoBroadcastJoinThreshold=-1 \
 --conf spark.port.maxRetries=100 \
 --conf spark.yarn.maxAppAttempts=1 \
 --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
 --conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener \
 --conf spark.sparklens.data.dir=tmp/sparklens/ \
 --jars https://repos.spark-packages.org/qubole/sparklens/0.3.2-s_2.11/sparklens-0.3.2-s_2.11.jar \
 --num-executors 1 \
 --executor-cores 1 \
 --executor-memory 512M \
 --driver-memory 1G \
 --name "spark-optimization-features" \
 --class com.github.fernandobontorin.jobs.FraudBookProcessor \
 --queue root.default \
 --deploy-mode cluster \
 target/scala-2.11/spark-optimization-features-assembly-0.1.0-SNAPSHOT.jar \
 --dataframes data/fraudTest.csv,data/fraudTrain.csv --output data/fraud_book_features
```

### Environment
Up Applications
```bash
docker build -f airflow/Dockerfile -t serasa-airflow-pyspark:2.0.2 .
docker-compose up
```
Set Spark-master connection
```yaml
connId: spark.default
host: spark://spark-master
port: 7077
```
Optional
```yaml
extra: {"deploy-mode":"cluster"}
```

### Optimization

#### partitions
one of the biggest villains of distributed data processing is the shuffle
 read/write, causes cores to get bottleneck waiting IO operations,
 the more you avoid IO during the process the better the application performs. 
In this case is important to pay attention to small files/partitions and
larger than HDFS block size

#### GC Pressure
Control memory allocation and core ratio, if the ratio of GB/core is wrong, the
computation stages, waste much time on garbage collector instead of processing data,
putting too much pressure on GC

#### One plan (Code performance)
If possible, is very recommended reducing stages as more than possible, 
at data attributes creation, execute a unique final action

#### Parallelize (Code performance)
On aggregation operations is very difficult to process in one plan,
 also, is very recommended orient your code to execute all final actions
 in parallel

#### Buckets (Code performance)
On join operations, like join 2 datasets using a long primary key is recommended,
create a bucket using part of primary key content, this helps Spark to organize
