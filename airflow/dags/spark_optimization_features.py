from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

fraud_features_jar = "/tmp/applications/spark-optimization-features-assembly-0.1.0-SNAPSHOT.jar"
sparklens_jar = "https://repos.spark-packages.org/qubole/sparklens/0.3.2-s_2.11/sparklens-0.3.2-s_2.11.jar"

with DAG(dag_id='spark_optimization_features', default_args={'owner': 'Airflow'}, schedule_interval=None,
         start_date=days_ago(1), tags=['fraud_features_set'], catchup=False, concurrency=1, max_active_runs=1) as dag:

    start = DummyOperator(task_id="start")

    book_fraud = SparkSubmitOperator(
        task_id="book_fraud",
        conn_id="spark.default",
        name="Book Fraud",
        application=fraud_features_jar,
        conf={
            "spark.default.parallelism": 200,
            "spark.dynamicAllocation.enabled": "false",
            "spark.network.timeout": 360000,
            "spark.shuffle.service.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": -1,
            "spark.port.maxRetries": 10,
            "spark.yarn.maxAppAttempts": 1,
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC",
            "spark.extraListeners": "com.qubole.sparklens.QuboleJobListener",
            "spark.sparklens.data.dir": "/tmp/data/history/sparklens"
        },
        jars=sparklens_jar,
        num_executors=1,
        executor_cores=1,
        executor_memory="512m",
        driver_memory="1G",
        java_class="com.github.fernandobontorin.jobs.FraudBookProcessor",
        application_args=[
            "--dataframes",
            "file:///tmp/data/fraudTest.csv,file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv,"
            "file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv",
            "--output",
            "file:///tmp/data/fraud_book_features"
        ]
    )

    book_fraud_optimized = SparkSubmitOperator(
        task_id="book_fraud_optimized",
        conn_id="spark.default",
        name="Book Fraud Optimized",
        application=fraud_features_jar,
        conf={
            "spark.default.parallelism": 1,
            "spark.dynamicAllocation.enabled": "false",
            "spark.network.timeout": 360000,
            "spark.shuffle.service.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": -1,
            "spark.port.maxRetries": 10,
            "spark.yarn.maxAppAttempts": 1,
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC",
            "spark.extraListeners": "com.qubole.sparklens.QuboleJobListener",
            "spark.sparklens.data.dir": "/tmp/data/history/sparklens"
        },
        jars=sparklens_jar,
        num_executors=1,
        executor_cores=1,
        executor_memory="512m",
        driver_memory="1G",
        java_class="com.github.fernandobontorin.jobs.FraudBookProcessor",
        application_args=[
            "--dataframes",
            "file:///tmp/data/fraudTest.csv,file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv,"
            "file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv",
            "--output",
            "file:///tmp/data/fraud_book_features"
        ]
    )

    aggregation_fraud = SparkSubmitOperator(
        task_id="aggregation_fraud",
        conn_id="spark.default",
        name="Agg Fraud Set",
        application=fraud_features_jar,
        conf={
            "spark.default.parallelism": 200,
            "spark.dynamicAllocation.enabled": "false",
            "spark.network.timeout": 360000,
            "spark.shuffle.service.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": -1,
            "spark.port.maxRetries": 10,
            "spark.yarn.maxAppAttempts": 1,
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC",
            "spark.extraListeners": "com.qubole.sparklens.QuboleJobListener",
            "spark.sparklens.data.dir": "/tmp/data/history/sparklens"
        },
        jars=sparklens_jar,
        num_executors=1,
        executor_cores=1,
        executor_memory="512m",
        driver_memory="1G",
        java_class="com.github.fernandobontorin.jobs.AggregationProcessor",
        application_args=[
            "--dataframes",
            "file:///tmp/data/fraudTest.csv,file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv,"
            "file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv",
            "--output",
            "file:///tmp/data/aggregation_fraud"
        ]
    )

    aggregation_fraud_par = SparkSubmitOperator(
        task_id="aggregation_fraud_par",
        conn_id="spark.default",
        name="Agg Fraud Set Par",
        application=fraud_features_jar,
        conf={
            "spark.default.parallelism": 200,
            "spark.dynamicAllocation.enabled": "false",
            "spark.network.timeout": 360000,
            "spark.shuffle.service.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": -1,
            "spark.port.maxRetries": 10,
            "spark.yarn.maxAppAttempts": 1,
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC",
            "spark.extraListeners": "com.qubole.sparklens.QuboleJobListener",
            "spark.sparklens.data.dir": "/tmp/data/history/sparklens"
        },
        jars=sparklens_jar,
        num_executors=1,
        executor_cores=1,
        executor_memory="512m",
        driver_memory="1G",
        java_class="com.github.fernandobontorin.jobs.ParAggregationProcessor",
        application_args=[
            "--dataframes",
            "file:///tmp/data/fraudTest.csv,file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv,"
            "file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv,file:///tmp/data/fraudTrain.csv",
            "--output",
            "file:///tmp/data/aggregation_fraud_par"
        ]
    )

    end = DummyOperator(task_id="end")

    start >> book_fraud >> book_fraud_optimized >> (aggregation_fraud, aggregation_fraud_par) >> end


