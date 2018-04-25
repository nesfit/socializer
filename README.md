# Socializer

## Installation

### Halyard Common dependency
```
mvn install:install-file -Dfile=libs/halyard-common-1.4-SNAPSHOT.jar -DgroupId=com.msd.gin.halyard.common -DartifactId=halyard-common -Dversion=1.4-SNAPSHOT -Dpackaging=jar -DgeneratePom=true
```

## Configuration

### Credentials

Set Facebook and Twitter credentials in `configuration.xml`.

```$xml
<property>
    <name>twitter.consumer.key</name>
    <value>key</value>
</property>
<property>
    <name>twitter.consumer.secret</name>
    <value>secret</value>
</property>

<property>
    <name>facebook.app.id</name>
    <value>id</value>
</property>
<property>
    <name>facebook.app.secret</name>
    <value>secret</value>
</property>
```

### HBase

Copy content of your `hbase-site.xml` into project's `hbase-site.xml`.

### Spark

Optionally you can specify some Spark properties into `configuration.xml`. 
This properties will not be overridden by `spark-submit` arguments. See
all available args at https://spark.apache.org/docs/latest/configuration.html.

### Cluster

Set number of partitions of you cluster in `configuration.xml`.

```$xml
<property>
    <name>cluster.partition_num</name>
    <value>1</value>
</property>
```

## Usage

### Build

Build jar with dependencies.
```$xslt
mvn clean compile assembly:single
```

### Creating tables

Sometimes, there it is not possible to create required tables via `spark-submit` job. 
If you are not able to create the table (`spark-submit` job freeze on start), use this builder:
```$xslt
java -cp socializer-1.0-jar-with-dependencies.jar cz.vutbr.fit.xtutko00.TableBuilder [halyard/hgraphdb] [table_name]
```

### Downloading data

Submit built jar into Spark cluster with `spark-submit`.

Jar params:
- `-hc, --hbase_client <arg>`    HBase client (halyard or hgraphdb)
- `-ht, --hbase_tablename <arg>` name of the HBase table
- `-s, --sources <arg>`          file with sources, each on new line (line format: T/F:sourceName)


Example:
```
$spark-submit --class cz.vutbr.fit.xtutko00.MainSpark socializer-1.0-jar-with-dependencies.jar -s sources.txt -hc halyard -ht test_table
```
