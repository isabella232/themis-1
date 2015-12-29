# Themis 

[![Build Status](https://travis-ci.org/pingcap/themis.svg?branch=master)](https://travis-ci.org/pingcap/themis)

## Introduction

Themis provides cross-row/cross-table transaction on HBase based on [google's Percolator](http://research.google.com/pubs/pub36726.html).

pingcap/themis is forked from [XiaoMi/themis](https://github.com/XiaoMi/themis), with some optimizations for SQL layer.

1. Batch prewrite/commit secondary rows.
2. Asynchronous clean secondary locks (commit secondary rows in background).
3. Add batch get APIs.
4. Add golang client: [pingcap/go-themis](https://github.com/pingcap/go-themis)

depends on HBase=0.98.6 with hadoop.version=2.0.0-alpha.
cloudera version=5.3.8(some lib of other verstions may not match HBase version)
Tested on Oracle JDK7

Notice: **Please make sure use correct above software version, any other version
may occur unexpect error.**

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
cp themis-coprocessor/target/themis-coprocessor-1.0-SNAPSHOT-jar-with-dependencies.jar $CDH/lib/hbase/lib
```

$CDH is normally `/opt/cloudera/parcels/CDH/` if you install cloudera
in default directory

- Add configurations for themis coprocessor in cloudera:

    - Add `org.apache.hadoop.hbase.themis.cp.ThemisEndpoint`,
    `org.apache.hadoop.hbase.themis.cp.ThemisScanObserver`,
    `org.apache.hadoop.hbase.themis.cp.ThemisScanObserver` to `hbase.coprocessor.region.classes`
    in region configuration
    - Add `org.apache.hadoop.hbase.master.ThemisMasterObserver` to
    `hbase.coprocessor.master.classes` in master configuration

- Restart HBase.
- Enjoy it.

## Future Works

1. Buffer recent committed primary locks in region server for fast conflict checking.
2. SQL pushdown.
