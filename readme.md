A Simple Redis storage engine for Lucene
========================================

_The repo is just a very simple implements for store lucene's index files in redis. I initially did this project is aims to be usable in production_.
But it is a completely small simple implements. It supports index file slice and mutex lock, the lock implements by java nio file lock, it can release lock when jvm exit abnormal.
If you use a singleton lock, then you can not achieve mutual exclusion across processes, or else if you use redis to store a flag as lock, then the virtual machine can not be automatically unlock the if it crash.

Requirements
------------

* Lucene 5.5.0+
* Jedis 2.9.0+
* Lombok 1.16.12+
* Log4j 2.6.2+
* Guava 20.0+

Installation
------------

*   Clone the repo _git clone git@github.com:maxpert/RedisDirectory.git RedisDirectory_
*   cd RedisDirectory
*   use maven commands or gradle commands to build the project

Features
--------
*   Supports pool
*   Supports sharding
*   Supports cluster(not tested)
*   Storage level distribution

Usage
-----

 Make sure you have the RedisDirectory.jar in you class path (Gradle or Maven can help you). To use it just please see TestLucene.java

File is divided into blocks and stored as HASH in redis in binary format that can be loaded on demand. You can customise the block size by modifying the DEFAULT_BUFFER_SIZE in config file. *Remember its a 1 time intialization once index is created on a particular size it can't be changed; higher block size causes lower fragmentation*.

Sharding
--------

 Look closely Jedis is doing the complete sharding for us.

TODO
----

I've just started. Have to:

*   Include support for Snappy compression to compress file block.
*   Rock solid JUNIT test cases for each class.
*   Enable atomic operations on RedisFile, this will allow multiple connections to manipulate single file.
*   Redundancy support, maintain multiple copies of a file (or its blocks).

## Simple Performance Test ( Windows 7, Redis-x64-3.2 )
|Type|Documents|Fields|Write Time(less is better)|100 Search Time(less is better)|
|---|---|---|---|---|
|RedisDirectory with Local Jedis|500000|10|18s|810ms|
|RedisDirectory with Local JedisPool|500000|10|13s|822ms|
|RedisDirectory with Local ShardedJedisPool|500000|10|13s|503ms|
|RamDirectory|500000|10|10s|48ms|
|MMapDirectory|500000|10|14s|50ms|

## Related Project
https://github.com/maxpert/RedisDirectory

https://github.com/DDTH/redir

## License

Copyright 2016 [Xu Wang](www.codepub.cn)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.