# Themis 

[![Build Status](https://travis-ci.org/pingcap/themis.svg?branch=master)](https://travis-ci.org/pingcap/themis)

## Introduction

Themis provides cross-row/cross-table transaction on HBase based on [google's Percolator](http://research.google.com/pubs/pub36726.html).

pingcap/themis is forked from [XiaoMi/themis](https://github.com/XiaoMi/themis), with some optimizations for SQL layer.

1. Batch prewrite/commit secondary rows.
2. Asynchronous clean secondary locks (commit secondary rows in background).
3. Add batch get APIs.
4. Add golang client: [pingcap/go-themis](https://github.com/pingcap/go-themis)

depends on hbase >= 0.98.5 with hadoop.version=2.0.0-alpha.  
Tested on Oracle JDK7

## Usage

### Build

- Get the latest source code of Themis:
```
git clone https://github.com/pingcap/themis.git
```  

- Build Themis 
```
cd themis
mvn clean package -DskipTests
```

### Deploy

- Copy themis coprocessor to $HBASE_ROOT/lib/
```
cp themis-coprocessor/target/themis-coprocessor-1.0-SNAPSHOT-jar-with-dependencies.jar $HBASE_ROOT/lib
```

- Add configurations for themis coprocessor in hbase-site.xml:

```
<property>
    <name>hbase.coprocessor.user.region.classes</name>
    <value>org.apache.hadoop.hbase.themis.cp.ThemisEndpoint,org.apache.hadoop.hbase.themis.cp.ThemisScanObserver,org.apache.hadoop.hbase.regionserver.ThemisRegionObserver</value>
</property>
<property>
    <name>hbase.coprocessor.master.classes</name>
    <value>org.apache.hadoop.hbase.master.ThemisMasterObserver</value>
</property>
```
- Restart HBase.
- Enjoy it.

## Future Works

1. Buffer recent committed primary locks in region server for fast conflict checking.
2. SQL pushdown.
