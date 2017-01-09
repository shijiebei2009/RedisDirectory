package cn.codepub.redis.directory;

import cn.codepub.redis.directory.io.ShardedJedisPoolStream;
import cn.codepub.redis.directory.util.Constants;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Created by wangxu on 2017/01/09 10:36.
 * </p>
 * <p>
 * Description: TODO
 * </p>
 *
 * @author Wang Xu
 * @version V1.0.0
 * @since V1.0.0 <br></br>
 * WebSite: http://codepub.cn <br></br>
 * Licence: Apache v2 License
 */
@Log4j2
public class Main {
    public static void main(String[] args) throws IOException {
        try {
            testRedisDirectoryWithShardedJedisPool();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Document addDocument(int i) {
        Document document = new Document();
        document.add(new StringField("key1", "key" + i, Field.Store.YES));
        document.add(new IntField("key2", i * 100000, Field.Store.YES));
        document.add(new FloatField("key3", (float) i * 100000, Field.Store.YES));
        document.add(new LongField("key4", (long) i * 100000, Field.Store.YES));
        document.add(new DoubleField("key5", (double) i * 100000, Field.Store.YES));
        document.add(new TextField("key6", RandomStringUtils.randomAlphabetic(10), Field.Store.YES));
        document.add(new StringField("key7", RandomStringUtils.randomAlphabetic(5), Field.Store.YES));
        document.add(new BinaryDocValuesField("key8", new BytesRef(RandomStringUtils.randomAlphabetic(5))));
        document.add(new DoubleDocValuesField("key9", RandomUtils.nextDouble(0, 1000)));
        document.add(new FloatDocValuesField("key10", RandomUtils.nextFloat(0, 1000)));
        document.add(new LongField("key11", (long) i * 50000, Field.Store.YES));
        document.add(new IntField("key12", i * 50000, Field.Store.YES));
        document.add(new FloatField("key13", (float) i * 50000, Field.Store.YES));
        document.add(new DoubleField("key14", (double) i * 50000, Field.Store.YES));
        document.add(new StringField("key15", RandomStringUtils.randomAlphabetic(6), Field.Store.YES));
        return document;
    }


    public static void testRedisDirectoryWithShardedJedisPool() throws IOException {
        long start = System.currentTimeMillis();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(IndexWriterConfig
                .OpenMode.CREATE);
        //indexWriterConfig.setInfoStream(System.out);
        //indexWriterConfig.setRAMBufferSizeMB(2048);
        //LogByteSizeMergePolicy logByteSizeMergePolicy = new LogByteSizeMergePolicy();
        //logByteSizeMergePolicy.setMinMergeMB(1);
        //logByteSizeMergePolicy.setMaxMergeMB(64);
        //logByteSizeMergePolicy.setMaxCFSSegmentSizeMB(64);
        //indexWriterConfig.setRAMBufferSizeMB(1024).setMergePolicy(logByteSizeMergePolicy).setUseCompoundFile(false);
        //GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        //获取连接等待时间
        //genericObjectPoolConfig.setMaxWaitMillis(3000);
        //10s超时时间
        List<JedisShardInfo> shards = new ArrayList<>();
        JedisShardInfo si = new JedisShardInfo("localhost", 6379, Constants.TIME_OUT);
        //JedisShardInfo si2 = new JedisShardInfo("localhost", 6380);
        shards.add(si);
        //shards.add(si2);
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        ShardedJedisPool shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
        RedisDirectory redisDirectory = new RedisDirectory(new ShardedJedisPoolStream(shardedJedisPool));
        IndexWriter indexWriter = new IndexWriter(redisDirectory, indexWriterConfig);
        for (int i = 0; i < 10000000; i++) {
            indexWriter.addDocument(addDocument(i));
        }
        indexWriter.commit();
        indexWriter.close();
        redisDirectory.close();
        long end = System.currentTimeMillis();
        log.error("RedisDirectoryWithShardedJedisPool consumes {}s!", (end - start) / 1000);
        shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
        start = System.currentTimeMillis();
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(new RedisDirectory(new ShardedJedisPoolStream
                (shardedJedisPool))));
        int total = 0;
        for (int i = 0; i < 10000000; i++) {
            TermQuery key1 = new TermQuery(new Term("key1", "key" + i));
            TopDocs search = indexSearcher.search(key1, 10);
            total += search.totalHits;
        }
        System.out.println(total);
        end = System.currentTimeMillis();
        log.error("RedisDirectoryWithShardedJedisPool search consumes {}ms!", (end - start));
    }
}
