# EMR_1 Code Documentation
Welcome to the iMarckDEV Blog Repository! This repository contains the source code for the [iMarckDEV blog site](https://www.imarck.dev), a platform dedicated to exploring cloud technologies, sharing tutorials, and providing valuable resources for developers.

# Description
In this case, I will demonstrate the use of EMR of AWS in an ETL process, utilizing JupyterLab as the notebook interface and PySpark for data processing. EMR is a cost-effective alternative to Glue, particularly for larger ETL jobs where resource requirements may be substantial. By leveraging EMR, we can optimize costs while efficiently handling significant data processing workloads.

## Especial notes
For this particular case, the entire dataset was provided in a CSV file. This is why a list of file paths is used in this ETL (Extract, Transform, Load) process to read the data. However, there was an issue encountered while reading all the folders due to some data being corrupted or in a different format.

# ETL transoformations
Some curious transformations in this ETL where the date:

```python
date_in='10 de Junio de 2023 18:15:00'
##and how i need
date_out='2023-06-10'
``` 

That's why i've used a USER DEFINED FUNCTION - UDF, called *get_fecha_consulta*.

Some others transformations:
-withColumnRenamed, renamed of fileds
-casting as date:
```python
df = df.withColumn("fecha", lit(datetime.date(int(year), int(month), int(day))))
``` 
-reselect of fileds
-use of repartition and coalesce:
```python
df=df.repartition(100).coalesce(50)
``` 
--In this case, the repartition was set to 100 because there was no reservation of a small portion of the RAM, resulting in the creation of 50 Parquet files with coalescing.

# Some EMR configurations

![setup EMR](setting_emr_1.png)

Some setups  for this use.

![setup EMR Master node](node_master.png)

The master node in this context refers to the central or primary node in a distributed computing system. It typically acts as a coordinator and is responsible for managing and overseeing the overall execution of tasks in the system. The master node plays a crucial role in task distribution, monitoring, and coordination among the worker nodes.

## Conecting to the master node using Putty or SSH
In this case it's necesary the user@.... to conect at port 22 and the key pair, to send some command at the master node:

Because i need to copy some files at the hadoop file system.
```batch
hdfs dfs -mkdir -p /apps/hudi/lib
hdfs dfs -copyFromLocal /usr/lib/hudi/hudi-spark-bundle.jar /apps/hudi/lib/hudi-spark-bundle.jar
hdfs dfs -copyFromLocal /usr/lib/spark/external/lib/spark-avro.jar /apps/hudi/lib/spark-avro.jar
``` 

![Master node in terminal](setting_emr_2.png)

## Contributing

Thank you for your interest in contributing to the iMarckDEV Blog Repository. If you have any improvements or bug fixes, please feel free to submit a pull request. We appreciate your contributions!

## License

This project is licensed under the [MIT License](LICENSE). Feel free to use, modify, and distribute this code for personal or commercial purposes.

For more information, visit the [iMarckDEV blog site](https://www.imarck.dev) and explore other resources and tutorials. Happy coding!'''