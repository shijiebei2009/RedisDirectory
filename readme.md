RedisDirectory [![Build Status](https://api.travis-ci.org/shijiebei2009/RedisDirectory.svg?branch=master)](https://travis-ci.org/shijiebei2009/RedisDirectory)   [![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
===========================================================================================================================================================================================================================================================================================
A simple redis storage engine for lucene
========================================

_The repo is just a very simple implements for store lucene's index files in redis. I initially did this project is aims to be usable in production_.
It's a complete concise implementation, you can use a different jedis implements (jedis/jedis pool/sharded jedis pool/jedis cluster) without modifying the code. It supports index file slice, mutex lock and redis file cache. With redis file cache it can help you improve the performance of writing index files. In this repo the lock implements by java nio file lock, it can release lock when jvm exit abnormal.
If you use a singleton lock, then you can not achieve mutual exclusion across multi processes, or else if you use redis to store a flag as lock, then the flag will still store in redis when the java virtual machine exit abnormal. And when you use next time, you can not obtain lock again unless you delete the lock flag in the redis manual.

Requirements
------------

* JDK 1.8+
* Lucene 5.5.0+
* Jedis 2.9.0+
* Lombok 1.16.12+
* Log4j 2.6.2+
* Guava 20.0+
* snappy-java 1.1.2.6+

Installation
------------

* Clone the repo _git clone git@github.com:shijiebei2009/RedisDirectory.git RedisDirectory_
* cd RedisDirectory
* use maven commands or gradle commands to build the project

Features
--------
* Supports pool
* Supports sharding
* Supports cluster (not tested)
* Supports Maven or Gradle Compile
* Supports storage level distribution

Usage
-----

Make sure you have the RedisDirectory.jar in you class path (Gradle or Maven can help you). To use it just like follows, you can set `stop-writes-on-bgsave-error no` in the redis.windows.conf if it occurs **MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk. Commands that may modify the data set are disabled. Please check Redis logs for details about the error.**

JedisPool

```java
IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(IndexWriterConfig
                .OpenMode.CREATE);
JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost", 6379);
RedisDirectory redisDirectory = new RedisDirectory(new JedisPoolStream(jedisPool));
IndexWriter indexWriter = new IndexWriter(redisDirectory, indexWriterConfig);
indexWriter.addDocument(...);
indexWriter.close();
redisDirectory.close();
```

Jedis

```java
IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(IndexWriterConfig
                .OpenMode.CREATE);
RedisDirectory redisDirectory = new RedisDirectory(new JedisStream("localhost", 6379));
IndexWriter indexWriter = new IndexWriter(redisDirectory, indexWriterConfig);
indexWriter.addDocument(...);
indexWriter.close();
redisDirectory.close();
```

ShardedJedisPool

```java
IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(IndexWriterConfig
                .OpenMode.CREATE);
List<JedisShardInfo> shards = new ArrayList<>();
JedisShardInfo si = new JedisShardInfo("localhost", 6379);
shards.add(si);
JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
ShardedJedisPool shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
RedisDirectory redisDirectory = new RedisDirectory(new ShardedJedisPoolStream(shardedJedisPool));
IndexWriter indexWriter = new IndexWriter(redisDirectory, indexWriterConfig);
indexWriter.addDocument(...);
indexWriter.close();
redisDirectory.close();
```

File is divided into blocks and stored as HASH in redis in binary format that can be loaded on demand. You can customise the block size by modifying the DEFAULT_BUFFER_SIZE in config file. *Remember its a 1 time intialization once index is created on a particular size it can't be changed; higher block size causes lower fragmentation*.

The index files will store in redis as follows:<br/>
directory metadata (user definition) => index file name => index file length<br/>
file metadata (user definition) => @index file name:block number => the block values

TODO
----

I've just started. Have to:

*   Include support for Snappy compression to compress file block.
*   Rock solid JUNIT test cases for each class.
*   Enable atomic operations on RedisFile, this will allow multiple connections to manipulate single file.
*   Redundancy support, maintain multiple copies of a file (or its blocks).

## Simple Performance Test ( Windows 7, i7 4790CPU, 8GB, Redis-x64-3.2 )
In command line, I run RedisDirectory jar file with arguments like this `java -Xms1024m -Xmx5120m -jar RedisDirectory-0.0.1.jar`, and the performance test results are as below. When the redis as the store engine, before the program start I will run `flushall` in redis and after the program done, I get the index size by `info` in redis commands line.

|Type|Documents|Fields|Write Time |Search Time(10 million)|Index Size|
|---|---|---|---|---|---|
|RamDirectory|10 million|15|303s|278s|2.63G(Approximately)|
|MMapDirectory|10 million|15|381s|307s|2.59G|
|RedisDirectory (Local JedisPool)|10 million|15|423s|632s|used_memory_human:2.67G|
|RedisDirectory (Local Jedis)|10 million|15|452s|536s|used_memory_human:2.67G|
|RedisDirectory (Local ShardedJedisPool)|10 million|15|477s|790s|used_memory_human:2.67G|

## Related Project
https://github.com/maxpert/RedisDirectory<br/>
https://github.com/DDTH/redir

## License

Copyright 2016 [Xu Wang](http://codepub.cn)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.