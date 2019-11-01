# Socializer

Downloads and analyze social network data, all in Hadoop cluster.

## Installation

### Halyard Common dependency

Application requires `Halyard-Common` dependency, which is not in standard Maven repo. To install it into 
your local repo, run this command in application root folder.

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

Copy content of your `hbase-site.xml` into project's `hbase-site.xml`. This file can be 
found in your HBase installation folder.

### Spark

Optionally you can specify some Spark properties into `configuration.xml`. 
This properties won't be overridden by `spark-submit` arguments. See
all available properties at https://spark.apache.org/docs/latest/configuration.html.

### Cluster

Set number of partitions of you cluster in `configuration.xml`. This number means how much
nodes cluster contains.

```$xml
<property>
    <name>cluster.partition_num</name>
    <value>1</value>
</property>
```

## Build

Build jar with dependencies.
```$xslt
mvn clean compile assembly:single
```

## Usage

### Creating tables

Created HBase tables has `_halyard`/`_hgraphdb` suffix. You don't have to write this suffix
when using application because it will be automatically added in the code. 

Sometimes, there it is not possible to create required tables via `spark-submit` job.  
If you are not able to create the table (`spark-submit` job freeze on start), use this builder:

```$xslt
java -cp socializer-1.0-jar-with-dependencies.jar cz.vutbr.fit.xtutko00.TableBuilder [halyard/hgraphdb] [table_name_without_suffix]
```

### Downloading data

Submit built jar into Spark cluster with `spark-submit`.

Jar params:
- `-hc, --hbase_client <arg>`    HBase client (halyard or hgraphdb)
- `-ht, --hbase_tablename <arg>` name of the HBase table (without suffix)
- `-s, --sources <arg>`          file with sources, each on new line (line format: T/F:sourceName)


Example:
```
$spark-submit --class cz.vutbr.fit.xtutko00.MainSpark socializer-1.0-jar-with-dependencies.jar -s sources.txt -hc halyard -ht test_table
```

### Testing database clients

Application contains functionality to test both implementations of database clients in one run.
Data are firstly downloaded distributed on cluster, then collected to one node
and then inserted into database with both Halyard and HGraphDB frameworks. 
Inserting data is also distributed.

This way it is possible to test writing speeds of both Halyard and HGraphDB.

To use application in testing mode, exclude database client from arguments. Example:
```
$spark-submit --class cz.vutbr.fit.xtutko00.MainSpark socializer-1.0-jar-with-dependencies.jar -s sources.txt -ht test_table
```

### Analyzing data

Both HGraphDB and Halyard clients have several database queries to analyze data:
- `longestText`: get entry with the longest text
- `timestamps`: get entries newer than 2018
- `timestampsSort`: get entries newer than 2018 sorted by time
- `numberOfEntries`: get timelines with number of entries
- `sharedUrls`: get urls, which are occurred in the database at least 2 times, sorted by occurrence

#### Halyard

To use Halyard tests you have to have Halyard RDF4J server and workbench distribution
installed on your cluster (see https://merck.github.io/Halyard/tools.html#rdf4j-web-applications).

Then you have to create RDF4J Halyard HBase store (see https://merck.github.io/Halyard/usage.html#with-rdf4j-workbench) 
pointing to your HBase table. Don't forget to use `_halyard` suffix.

Usage:
- `serverUrl` - url to rdf4j server (f.e. http://localhost:8080/rdf4j-server)
- `repositoryName` - name of the repository you have created (can be view via rdf4j workbench)
```shell
$ java -cp socializer-1.0-jar-with-dependencies.jar cz.vutbr.fit.xtutko00.MainHalyard [server_url] [repository_name] [test_name]
```

#### HGraphDB

Since HGraphDB is connecting right into HBase database, tests have to be runned on master node of the cluster with configured `hbase-site.xml` file.

Usage:
```shell
$ java -cp socializer-1.0-jar-with-dependencies.jar cz.vutbr.fit.xtutko00.MainHGraphDb [table_name_without_suffix] [test_name]
```

## Acknowledgements

*This work was supported by the Ministry of the Interior of the Czech Republic as a part of the project Integrated platform for analysis of digital data from security incidents VI20172020062.*
