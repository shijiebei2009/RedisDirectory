import cn.codepub.redis.directory.RedisDirectory;
import cn.codepub.redis.directory.io.JedisPoolStream;
import cn.codepub.redis.directory.io.JedisStream;
import cn.codepub.redis.directory.io.ShardedJedisPoolStream;
import cn.codepub.redis.directory.utils.Constants;
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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * <p>
 * Created by wangxu on 2016/11/08 10:18.
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
public class TestLucene {
    public static void main(String[] args) throws ExecutionException, IOException, InterruptedException {
        //"10.97.19.55"
        new TestLucene().testRedisDirectoryWithShardedJedisPool();
        //new TestLucene().testRedisDirectoryWithJedis();
        //new TestLucene().testRedisDirectoryWithJedisPool();
        //new TestLucene().testRamDirectory();
        //new TestLucene().testMMapDirectory();
        //new TestLucene().testRedisDirectoryWithRemoteJedisPool();
    }

    public void testRamDirectory() throws IOException {
        long start = System.currentTimeMillis();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(IndexWriterConfig
                .OpenMode.CREATE);
        RAMDirectory ramDirectory = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(ramDirectory, indexWriterConfig);
        for (int i = 0; i < 5000000; i++) {
            indexWriter.addDocument(addDocument(i));
        }
        indexWriter.commit();
        indexWriter.close();
        long end = System.currentTimeMillis();
        log.error("RamDirectory consumes {}s!", (end - start) / 1000);
        start = System.currentTimeMillis();
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(ramDirectory));
        int total = 0;
        for (int i = 0; i < 1000000; i++) {
            TermQuery key1 = new TermQuery(new Term("key1", "key" + i));
            TopDocs search = indexSearcher.search(key1, 10);
            total += search.totalHits;
        }
        System.out.println(total);
        end = System.currentTimeMillis();
        log.error("RamDirectory search consumes {}ms!", (end - start));
    }

    private Document addDocument(int i) {
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

    public void testRedisDirectoryWithRemoteJedisPool() throws IOException {
        long start = System.currentTimeMillis();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(IndexWriterConfig
                .OpenMode.CREATE);
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "10.97.19.55", 6379, Constants.timeOut);
        RedisDirectory redisDirectory = new RedisDirectory(new JedisPoolStream(jedisPool));
        IndexWriter indexWriter = new IndexWriter(redisDirectory, indexWriterConfig);
        for (int i = 0; i < 5000000; i++) {
            indexWriter.addDocument(addDocument(i));
        }
        indexWriter.commit();
        indexWriter.close();
        redisDirectory.close();
        long end = System.currentTimeMillis();
        log.error("RedisDirectoryWithJedisPool consumes {}s!", (end - start) / 1000);
        start = System.currentTimeMillis();
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(new RedisDirectory(new JedisStream("localhost",
                6379))));
        int total = 0;
        for (int i = 0; i < 1000000; i++) {
            TermQuery key1 = new TermQuery(new Term("key1", "key" + i));
            TopDocs search = indexSearcher.search(key1, 10);
            total += search.totalHits;
        }
        System.out.println(total);
        end = System.currentTimeMillis();
        log.error("RedisDirectoryWithJedisPool search consumes {}ms!", (end - start));
    }


    public void testRedisDirectoryWithJedisPool() throws IOException {
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
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, Constants.timeOut);
        RedisDirectory redisDirectory = new RedisDirectory(new JedisPoolStream(jedisPool));
        IndexWriter indexWriter = new IndexWriter(redisDirectory, indexWriterConfig);
        for (int i = 0; i < 5000000; i++) {
            indexWriter.addDocument(addDocument(i));
        }
        indexWriter.commit();
        indexWriter.close();
        redisDirectory.close();
        long end = System.currentTimeMillis();
        log.error("RedisDirectoryWithJedisPool consumes {}s!", (end - start) / 1000);
        start = System.currentTimeMillis();
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(new RedisDirectory(new JedisStream("localhost",
                6379))));
        int total = 0;
        for (int i = 0; i < 1000000; i++) {
            TermQuery key1 = new TermQuery(new Term("key1", "key" + i));
            TopDocs search = indexSearcher.search(key1, 10);
            total += search.totalHits;
        }
        System.out.println(total);
        end = System.currentTimeMillis();
        log.error("RedisDirectoryWithJedisPool search consumes {}ms!", (end - start));
    }


    public void testRedisDirectoryWithShardedJedisPool() throws IOException {
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
        JedisShardInfo si = new JedisShardInfo("localhost", 6379, Constants.timeOut);
        //JedisShardInfo si2 = new JedisShardInfo("localhost", 6380);
        shards.add(si);
        //shards.add(si2);
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        ShardedJedisPool shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
        RedisDirectory redisDirectory = new RedisDirectory(new ShardedJedisPoolStream(shardedJedisPool));
        IndexWriter indexWriter = new IndexWriter(redisDirectory, indexWriterConfig);
        for (int i = 0; i < 50000000; i++) {
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

    public void testRedisDirectoryWithJedis() throws IOException {
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
        RedisDirectory redisDirectory = new RedisDirectory(new JedisStream("localhost", 6379));
        IndexWriter indexWriter = new IndexWriter(redisDirectory, indexWriterConfig);
        for (int i = 0; i < 5000000; i++) {
            indexWriter.addDocument(addDocument(i));
        }
        indexWriter.commit();
        indexWriter.close();
        redisDirectory.close();
        long end = System.currentTimeMillis();
        log.error("RedisDirectoryWithJedis consumes {}s!", (end - start) / 1000);
        start = System.currentTimeMillis();
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(new RedisDirectory(new JedisStream("localhost",
                6379))));
        int total = 0;
        for (int i = 0; i < 1000000; i++) {
            TermQuery key1 = new TermQuery(new Term("key1", "key" + i));
            TopDocs search = indexSearcher.search(key1, 10);
            total += search.totalHits;
        }
        System.out.println(total);
        end = System.currentTimeMillis();
        log.error("RedisDirectoryWithJedis search consumes {}ms!", (end - start));
    }

    public void testMMapDirectory() throws IOException {
        long start = System.currentTimeMillis();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(IndexWriterConfig
                .OpenMode.CREATE);
        FSDirectory open = FSDirectory.open(Paths.get("E:/testlucene"));
        IndexWriter indexWriter = new IndexWriter(open, indexWriterConfig);
        for (int i = 0; i < 5000000; i++) {
            indexWriter.addDocument(addDocument(i));
        }
        indexWriter.commit();
        indexWriter.close();
        long end = System.currentTimeMillis();
        log.error("MMapDirectory consumes {}s!", (end - start) / 1000);
        start = System.currentTimeMillis();
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(open));
        int total = 0;
        for (int i = 0; i < 1000000; i++) {
            TermQuery key1 = new TermQuery(new Term("key1", "key" + i));
            TopDocs search = indexSearcher.search(key1, 10);
            total += search.totalHits;
        }
        System.out.println(total);
        end = System.currentTimeMillis();
        log.error("MMapDirectory search consumes {}ms!", (end - start));
    }

}
