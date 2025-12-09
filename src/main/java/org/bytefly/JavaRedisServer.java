package org.bytefly;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class JavaRedisServer {

    /* Redis 内存淘汰策略 */
    static int REDIS_MAXMEMORY_VOLATILE_LRU = 0;
    static int REDIS_MAXMEMORY_VOLATILE_TTL = 1;
    static int REDIS_MAXMEMORY_VOLATILE_RANDOM = 2;

    static int REDIS_MAXMEMORY_ALLKEYS_LRU = 3;
    static int REDIS_MAXMEMORY_ALLKEYS_RANDOM = 4;
    static int REDIS_MAXMEMORY_NO_EVICTION = 5;
    static int REDIS_DEFAULT_MAXMEMORY_POLICY = REDIS_MAXMEMORY_NO_EVICTION;

    static long maxmemory = 3; // 模拟最大内存大小，键值对的数量
    static int maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_LRU;

    // 16个db
    static RedisDB[] redisDb;

    // 客户端链接集合
    static List<RedisClient> clients = new ArrayList<>();

    // 订阅频道
    static Map<String, List<RedisClient>> pubsub_channels = new HashMap<>();

    static ServerSocketChannel serverSocketChannel;
    static Selector selector;
    static HashMap<SelectionKey, RedisClient> clientMap = new HashMap<>();

    static class RedisObject {
        int type; // 类型
        int encoding; // 编码

        long lru; // 最近一次被访问的时间戳

        int refcount; // 引用计数
        Object value;

        public RedisObject(Object value) {
            this.lru = System.currentTimeMillis();
            this.value = value;
        }

        @Override
        public String toString() {
            return "RedisObject{" +
                    "type=" + type +
                    ", encoding=" + encoding +
                    ", lru=" + lru +
                    ", refcount=" + refcount +
                    ", value=" + value +
                    '}';
        }

    }

    /**
     * 每秒执行的次数(执行频率)
     */
    static int hz = 10;

    // 每个db
    static class RedisDB {
        // 存储键值对
        public Dict<RedisObject> dict = new Dict();

        // 键的过期时间，保存了所有键的过期时间 key = keyName， value = "expireTime"
        public Dict<Long> expires = new Dict();

        public int id; // db的索引

        public List<WatchKeyClient> watched_keys = new ArrayList<>();

        public static class WatchKeyClient {
            RedisClient client;
            String key;
        }


    }

    // 字典
    static class Dict<T> {
        Hashtable<String, T>[] ht = new Hashtable[2];

        {
            for (int i = 0; i < 2; i++) {
                ht[i] = new Hashtable();
            }
        }

        // -1 表示没有正在进行渐进式hash
        int rehashinx = -1;


        // 通用的代码
        public void set(String key, T value) {
            if (rehashinx == -1) {
                ht[0].put(key, value); // 没有进行渐进式 rehash，则直接放入ht[0]中
            } else {
                ht[1].put(key, value); // 进行渐进式 rehash，则放入ht[1]中
            }
        }

        public void remove(String key) {
            if (rehashinx == -1) {
                ht[0].remove(key); // 没有进行渐进式 rehash，则直接从ht[0]中删除
            } else {
                ht[1].remove(key); // 没有进行渐进式 rehash，则从ht[1]中删除
            }
        }

        public long getDictSize() {
            return ht[0].size() + ht[1].size();
        }

        // dict的方法
        public RedisObject getRedisObject(String key) {
            if (rehashinx == -1) {
                RedisObject redisObject = (RedisObject) ht[0].get(key);
                if (redisObject != null) {
                    return redisObject;
                } else {
                    return (RedisObject) ht[1].get(key);
                }
            } else {
                return (RedisObject) ht[0].get(key);
            }
        }

        /**
         * 获取 key 值的 idle 时间
         *
         * @param key
         * @return
         */
        public Long getIDLE(String key) {
            RedisObject redisObject = this.getRedisObject(key);
            return redisObject == null ? null : System.currentTimeMillis() - redisObject.lru;
        }

        // expire的方法
        public Long getTTL(String key) {
            if (rehashinx == 1) {
                Long redisObject = (Long) ht[0].get(key);
                if (redisObject != null) {
                    return redisObject;
                } else {
                    return (Long) ht[1].get(key);
                }
            } else {
                return (Long) ht[0].get(key);
            }
        }

    }

    static class RedisClient {
        /**
         * 当前客户端正在使用的数据库
         */
        RedisDB selectedDb;


        byte[] queryBuf = new byte[1024]; // 输入缓冲区
        int queryBufLen; // 输入缓冲区长度

        byte[] outBuf = new byte[1024]; // 输出缓冲区
        int outBufLen; // 输出缓冲区长度

        // ===== 非redis数据结构 =====
        Object retValue; // 返回值
        SocketChannel channel;

        boolean read; // 是否可读
        boolean write; // 是否可写
        boolean accept; // 是否课接受连接

        /**
         * 客户端状态
         * 0: salve
         * 1: master
         * 2: slave monitor
         * 3: multi   事务中   ###
         * 4: blocked
         * 5: watched key modified  ###
         * 等等
         */
        int flags = 0;
        Multi.MultiState multiState; // 保存事务相关的数据

        List<WatchKey> watched_keys = new ArrayList<>();

        public static class WatchKey {
            String key; // key
            int db; // db的索引
        }

        BlockingState bpop;

        public static class BlockingState {
            long timeout;
            Set<String> keys = new HashSet<>();
        }

        // 将 ByteBuffer 中的数据值追加到 queryBuf 中
        public void appendToQueryBuf(ByteBuffer buffer) {
            buffer.flip();
            try {
                // 直接复制全部可读数据
                int bytesToCopy = buffer.remaining();
                ensureQueryBufCapacity(bytesToCopy);
                buffer.get(queryBuf, 0, bytesToCopy);
                queryBufLen = bytesToCopy;
            } finally {
                buffer.clear();
            }
        }

        // 动态扩容逻辑
        private void ensureQueryBufCapacity(int required) {
            if (queryBufLen + required > queryBuf.length) {
                // 按需扩容（建议 2 倍扩容策略）
                int newCapacity = Math.max(queryBuf.length * 2, queryBufLen + required);
                queryBuf = Arrays.copyOf(queryBuf, newCapacity);
            }
        }

    }

    public static long eventTime = System.currentTimeMillis();

    // 高频函数，每次时间循环都会进入
    public static void beforeSleep() {
        activeExpireCycle(true); // 快速模式删除过期的 key

        // flushAppendOnlyFile   将 AOF 缓冲区的内容写入到 AOF文件
        // clusterBeforeSleep() 集群模式下执行

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        initServer();

        long nextEventTime = System.currentTimeMillis();
        while (true) {

            beforeSleep();
            long now = System.currentTimeMillis(); // 当前时间
            long timeout = eventTime - now; // 超时时间

            if (timeout <= 0) {
                timeout = 1;
            }

            // 处理文件事件
            aeProcessEvents(timeout);

            // 处理时间事件
            if (now >= nextEventTime) {
                // 定时任务，默认每秒执行 hz(10) 次
                serverCron();
                nextEventTime = now + 1000 / hz;
            }
        }

    }

    public static void initServer() throws IOException {
        /**
         * 从配置文件中读取配置信息
         */
        int dbCount = 16;
        int port = 6379;

        redisDb = new RedisDB[16];
        for (int i = 0; i < dbCount; i++) {
            redisDb[i] = new RedisDB();
        }
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        selector = Selector.open();
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    /**
     * 文件事件处理
     */
    public static void aeProcessEvents(long timeout) throws IOException {
        selector.select(timeout);
        Set<SelectionKey> selectionKeys = selector.selectedKeys();
        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            System.out.println("处理事件" + key);

            if (key.isAcceptable()) {
                handleAccept(key);
            } else if (key.isReadable()) {
                handleRead(key);
            } else if (key.isWritable()) {
                handleWrite(key);
            }
        }
    }

    /**
     * 连接事件
     */
    public static void handleAccept(SelectionKey key) throws IOException {
        RedisClient redisClient = new RedisClient();
        SocketChannel clientChannel = serverSocketChannel.accept();
        if (clientChannel == null) {
            return;
        }

        System.out.println("接受连接:" + clients);
        clientChannel.configureBlocking(false);

        /** 注册读事件 */
        clientChannel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(1024));
        redisClient.channel = clientChannel;
        redisClient.selectedDb = redisDb[0];

        clientMap.put(clientChannel.keyFor(selector), redisClient);
        clients.add(redisClient);
    }

    public static void handleRead(SelectionKey key) throws IOException {
        RedisClient redisClient = clientMap.get(key);
        if (redisClient == null) {
            return;
        }
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        try {
            int bytesRead = socketChannel.read(buffer);
            if (bytesRead == -1) {
                closeClient(socketChannel, key, redisClient);
                return;
            }
        } catch (IOException e) {
            closeClient(socketChannel, key, redisClient);
        }
        System.out.println("读取数据:" + new String(buffer.array()));

        //将数据追加到客户端的缓冲区中
        redisClient.appendToQueryBuf(buffer);
        processQueryBuffer(redisClient);

    }

    /**
     * 处理客户端请求的缓冲区
     * tcp 拆包 封包
     * 1. queryBuf 不能构成一个完整的resp
     * 2. queryBuf 正好一个resp
     * 3. 超过一个resp协议（不构成两个）
     * 4. 多个resp报文
     */
    private static void processQueryBuffer(RedisClient client) {
        while (client.queryBufLen > 0) {
            RedisRequest redisRequest = new RedisRequest();
            int processed = RespUtil.parseComment(client.queryBuf, 0, client.queryBufLen, redisRequest);
            if (processed > 0) {
                // 处理完整命令
                byte[] remaining = Arrays.copyOfRange(client.queryBuf, processed, client.queryBufLen);
                System.arraycopy(remaining, 0, client.queryBuf, 0, remaining.length);
                client.queryBufLen = remaining.length;

                Object result = null;
                try {
                    result = processCommand(client, redisRequest);
                } catch (Exception e) {
                    result = new ErrorObject("Err" + e.getMessage());
                }
                if (result != null) {
                    client.retValue = result;
                    client.write = true;

                    // 注冊写事件
                    SelectionKey key = client.channel.keyFor(selector);
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                }
            } else if (processed == 0) {
                break; // 数据不完整，等待下次读取
            } else {
//                 closeClient(client.channel, client.channel.keyFor(selector), client);
                break;
            }
        }
    }

    public static Object lookupKeyRead(RedisDB db, String key) {
        // 惰性删除
        expireIfneeded(db, key);
        return lookupKey(db, key);
    }

    /**
     * 惰性删除
     */
    public static void expireIfneeded(RedisDB db, String key) {
        Long expireTime = db.expires.getTTL(key);
        if (expireTime != null) {
            if (expireTime <= System.currentTimeMillis()) {
                db.expires.remove(key);
                db.dict.remove(key);
            }
        }
    }

    public static Object lookupKey(RedisDB db, String key) {
        RedisObject redisObject = db.dict.getRedisObject(key);
        if (redisObject != null) {
            redisObject.lru = System.currentTimeMillis();
            return redisObject.value;
        }
        return null;
    }

    public static Object processCommand(RedisClient redisClient, RedisRequest request) {
        System.out.println("handle command: " + request.command + " args: " + request.args);

        RedisDB selectedDb = redisClient.selectedDb;

        String command = request.command;

        // 内存淘汰
        if (maxmemory > 0) {
            int rtval = freeMemoryIfNeeded();
            if (rtval == -1 && isCmdDenyoom(request.command)) {
                return new ErrorObject("OOM command not allowed when used memory > 'maxmemory'.");
            }
        }

        if (redisClient.flags == 3 && !command.equalsIgnoreCase(Multi.MULTI) && !command.equalsIgnoreCase(Multi.DISCARD) && !command.equalsIgnoreCase(Multi.WATCH) && !command.equalsIgnoreCase(Multi.EXEC)) {
            Multi.queueMultiCommand(redisClient, request);
            return "OK";
        }

        return call(redisClient, request);
    }

    static long getDbSize() {
        long size = 0;
        for (int i = 0; i < redisDb.length; i++) {
            size += redisDb[i].dict.getDictSize();
        }
        return size;
    }

    /**
     * 如果需要，根据内存淘汰策略 释放内存
     */
    public static int freeMemoryIfNeeded() {
        long dbSize = getDbSize();
        if (dbSize < maxmemory) {
            // 内存充足
            return 0;
        }

        // 内存满了
        if (maxmemory_policy == REDIS_MAXMEMORY_NO_EVICTION) {
            return -1;
        }

        // 先计算要淘汰多少内存
        long mem_tofree = dbSize - maxmemory;

        int mem_freed = 0; // 已释放的内存

        boolean keys_freed = false;

        while (mem_freed < mem_tofree) {
            for (int i = 0; i < redisDb.length; i++) {
                Dict targetDict = null; // 目标字典

                String deleteKey = null; // 删除的key
                if (maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM || maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU) {
                    targetDict = redisDb[i].dict;
                } else {
                    targetDict = redisDb[i].expires;
                }

                if (targetDict.getDictSize() == 0) {
                    continue;
                }

                // Random 策略
                if (maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM || maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_RANDOM) {
                    deleteKey = dictGetRandomKey(targetDict);
                }
                // LRU 策略
                else if (maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU || maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_LRU) {
                    // LRU策略
                    // 那么从一集 sample 键中选出 IDLE 事件最长的那个键
                    // LRU策略：随机采样 server.maxmemory_samples 个键，选其中idle时间最长的（即最近最少使用）
                    int samplesCount = 5; // 默认采样集合大小
                    String bestKey = null; // 最佳键
                    long minIDLE = Long.MAX_VALUE;
                    for (int j = 0; j < samplesCount; j++) {
                        String key = dictGetRandomKey(targetDict);
                        if (key == null) {
                            continue;
                        }

                        // 重点:从主字典（redisDb[i].dict）获取idle时间（不是从expires字典！）
                        Long idle = redisDb[i].dict.getIDLE(key);
                        if (idle < minIDLE) {
                            minIDLE = idle;
                            bestKey = key;
                        }
                    }
                    if (bestKey != null) {
                        deleteKey = bestKey;
                    }

                }
                // ttl 策略
                else if (maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_TTL) {
                    // TTL策略
                    // 随机从过期键中选取5个键，然后选一个ttl最短的键    ### 并不是吧所有的进行排序 ###
                    String tempKey = null;
                    long tempTTL = Long.MAX_VALUE;
                    for (int j = 0; j < 5; j++) {
                        String randomKey = dictGetRandomKey(targetDict);
                        long ttl = targetDict.getTTL(randomKey);
                        if (ttl < tempTTL) {
                            tempKey = randomKey;
                            tempTTL = ttl;
                        }
                    }
                    deleteKey = tempKey;
                }

                if (deleteKey != null) {
                    System.out.println("######## 内存淘汰，策略" + maxmemory_policy + "，删除键" + deleteKey + "########");
                    // 删除键
                    keys_freed = true;
                    redisDb[i].dict.remove(deleteKey);
                    redisDb[i].expires.remove(deleteKey);
                    mem_tofree += 1;
                }
            }
            if (!keys_freed) {
                // 没有被释放，说明内存已满，但是没有满足策略的键
                return -1;
            }
        }
        return mem_freed;
    }

    // 如果内存淘汰失败，且是 修改类型的命令
    private static boolean isCmdDenyoom(String command) {
        return "set".equalsIgnoreCase(command) || "setex".equalsIgnoreCase(command) || "mset".equalsIgnoreCase(command) || "msetnx".equalsIgnoreCase(command);
    }

    // 获取一个随机键
    public static String dictGetRandomKey(Dict dict) {
        if (dict.getDictSize() == 0) {
            return null;
        }
        List<String> keysArray = null;
        if (dict.rehashinx == -1) {
            keysArray = new ArrayList<>(dict.ht[0].keySet());
        } else {
            keysArray = new ArrayList<>(dict.ht[0].keySet());
            List<String> keysArray1 = new ArrayList<>(dict.ht[1].keySet());
            keysArray.addAll(keysArray1);
        }

        Random random = new Random();
        int index = random.nextInt(keysArray.size());
        return keysArray.get(index);
    }

    public static Object call(RedisClient redisClient, RedisRequest request) {
        RedisDB selectedDb = redisClient.selectedDb;

        // 开启事务
        if ("multi".equalsIgnoreCase(request.command)) {
            return Multi.multi(redisClient);
        }

        if ("discard".equalsIgnoreCase(request.command)) {
            return Multi.discard(redisClient);
        }

        // 执行事务
        if ("exec".equalsIgnoreCase(request.command)) {
            return Multi.exec(redisClient, request);
        }

        if ("watch".equalsIgnoreCase(request.command)) {
            return Multi.watch(redisClient, request);
        }

        if ("unwatch".equalsIgnoreCase(request.command)) {
            return Multi.unwatch(redisClient);
        }

        String key = null;
        if (request.args.size() > 0) {
            key = request.args.get(0);
        }


        /**
         * subscribe test1 test2
         */

        // subscribe
        // 订阅
        if ("subscribe".equalsIgnoreCase(request.command)) {
            for (String channel : request.args) {
                List<RedisClient> redisClients = pubsub_channels.get(channel);
                if (redisClients == null) {
                    redisClients = new ArrayList<>();
                    pubsub_channels.put(channel, redisClients);
                }
                redisClients.add(redisClient); // 添加订阅者 - 当前客户端
            }
            return new ArrayObject("subscribe", key, 1);
        }

        // 发布 publish test1 "hello world"
        if ("publish".equalsIgnoreCase(request.command)) {
            String channel = key;
            String message = request.args.get(1);
            List<RedisClient> redisClients = pubsub_channels.get(channel);
            if (redisClients != null) {
                for (RedisClient client : redisClients) {
                    client.retValue = message;
                    client.write = true;

                    // 注册可写事件
                    SelectionKey key2 = client.channel.keyFor(selector);
                    key2.interestOps(key2.interestOps() | SelectionKey.OP_WRITE);
                }
            }
            return Long.valueOf(redisClients.size()).toString();
        }

        if ("expire".equalsIgnoreCase(request.command)) {
            RedisObject redisObject = selectedDb.dict.getRedisObject(request.args.get(0));
            if (redisObject == null) {
                return 0;
            }
            long l = Long.parseLong(request.args.get(1)); // 偏移时间，秒
            long expireTime = System.currentTimeMillis() + l * 1000;
            selectedDb.expires.set(key, expireTime);
            return 1;
        }
        if ("ttl".equalsIgnoreCase(request.command)) {
            Long ttl = selectedDb.expires.getTTL(key);
            if (ttl == null) {
                return "-1";
            }
            long l = ((long) ttl - System.currentTimeMillis()) / 1000;
            return Long.valueOf(l).toString();
        }

        if ("lpush".equalsIgnoreCase(request.command)) {
            RedisObject redisObject = selectedDb.dict.getRedisObject(key);
            if (redisObject != null && redisObject.type != RedisConstants.REDIS_LIST) {
                return new ErrorObject("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            if (redisObject == null) {
                ZipList zipList = new ZipList();
                redisObject = new RedisObject(zipList);
                redisObject.type = RedisConstants.REDIS_LIST;
                redisObject.encoding = RedisConstants.REDIS_ENCODING_ZIPLIST;
                selectedDb.dict.set(key, redisObject);
            }

//            if (redisObject == null) {
//                LinkedList zipList = new LinkedList<>();
//                redisObject = new RedisObject(zipList);
//                redisObject.type = RedisConstants.REDIS_LIST;
//                redisObject.encoding = RedisConstants.REDIS_ENCODING_LINKEDLIST;
//                selectedDb.dict.set(key, redisObject);
//            }
            int count = 0;
            for (String value : request.args.subList(1, request.args.size())) {
                listTypePush(redisObject, value, true);
                count++;
            }
            return Long.valueOf(count).toString();
        }

        /**
         * lrange test1 0 -1
         */
        if ("lrange".equalsIgnoreCase(request.command)) {
            long start = Long.parseLong(request.args.get(1));
            long end = Long.parseLong(request.args.get(2));

            RedisObject redisObject = selectedDb.dict.getRedisObject(key);
            if (redisObject != null && redisObject.type != RedisConstants.REDIS_LIST) {
                return new ErrorObject("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            if (redisObject == null) {
                return new ArrayObject();
            }

            if (redisObject.encoding == RedisConstants.REDIS_ENCODING_ZIPLIST) {
                ZipList zipList = (ZipList) redisObject.value;
                // List<String> list = zipList.range(start, end);
                List<String> list = new ArrayList<>();
                return new ArrayObject(list.toString());
            } else if (redisObject.encoding == RedisConstants.REDIS_ENCODING_LINKEDLIST) {
                LinkedList linkedList = (LinkedList) redisObject.value;

                List<Object> range = new ArrayList<>();
                if (end == -1) {
                    end = linkedList.size() - 1;
                }
                for (int i = 0; i < linkedList.size(); i++) {
                    if (i >= start && i <= end) {
                        Object o = linkedList.get(i);
                        range.add(o);
                    }
                }
                return new ArrayObject(range.toArray());
            }
        }

        if ("blpop".equalsIgnoreCase(request.command)) {
            String s = request.args.get(1);
            long timeout = Long.parseLong(s);
            Object rtObject = null;
            RedisObject redisObject = selectedDb.dict.getRedisObject(key);
            if (redisObject != null && redisObject.type != RedisConstants.REDIS_LIST) {
                return new ErrorObject("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            if (redisObject != null) {

            }
            if (redisObject == null) {
                blockForKeys(redisClient, key, timeout);
                return null;
            }
        }

        if ("get".equalsIgnoreCase(request.command)) {
            return lookupKeyRead(selectedDb, key);
        }
        if ("set".equalsIgnoreCase(request.command)) {
            RedisObject redisObject = new RedisObject(request.args.get(1));
            selectedDb.dict.set(key, redisObject);

            Multi.touchWatchedKeys(redisClient, request);

            return "OK";
        }
        if ("select".equalsIgnoreCase(request.command)) {
            int dbIndex = Integer.parseInt(key);
            if (dbIndex < 0 | dbIndex >= redisDb.length) {
                return "ERR invalid DB index";
            }
            redisClient.selectedDb = redisDb[dbIndex];
            return "OK";
        }
        if ("auth".equalsIgnoreCase(request.command)) {
            return "OK";
        }
        if ("ping".equalsIgnoreCase(request.command)) {
            return "PONG";
        }
        if ("info".equalsIgnoreCase(request.command)) {
            return infoResponse;
        }
        if ("hello".equalsIgnoreCase(request.command)) {
            return new ErrorObject("ERR unknown command 'HELLO'");
        }
        if ("keys".equalsIgnoreCase(request.command)) {
            List<String> list = new ArrayList<>();
            // 匹配模式
            String pattern = key;
            for (RedisDB redisDB : redisDb) {
                // TODO 判断是否过期
                for (String getkey : redisDB.dict.ht[0].keySet()) {
                    if (isMatch(getkey, pattern)) {
                        list.add(getkey);
                    }
                }
                if (redisDB.dict.rehashinx != -1) {
                    for (String getKey : redisDB.dict.ht[1].keySet()) {
                        if (isMatch(getKey, pattern)) {
                            list.add(getKey);
                        }
                    }
                }
            }
            return list.toString();
        }
        return "ERR unknown command '" + request.command + "'";
    }

    static void blockForKeys(RedisClient redisClient, String key, long timeout) {
        redisClient.flags = 4;
        redisClient.bpop = new RedisClient.BlockingState();
        redisClient.bpop.timeout = System.currentTimeMillis() + timeout * 1000;
        redisClient.bpop.keys.add(key);
    }

    static void listTypeTryConversion() {

    }

    /**
     * left     head <<<<< tail     right
     * lpush               rpush
     */
    public static void listTypePush(RedisObject redisObject, String value, boolean isHead) {
        listTypeTryConversion();
        if (redisObject.encoding == RedisConstants.REDIS_ENCODING_ZIPLIST) {
            ZipList zipList = (ZipList) redisObject.value;
            if (isHead) {
                // zipList.insertFromHead(value);
            } else {
                // zipList.insertFromTail(value);
            }
        } else if (redisObject.encoding == RedisConstants.REDIS_ENCODING_LINKEDLIST) {
            LinkedList linkedList = (LinkedList) redisObject.value;
            if (isHead) {
                linkedList.add(0, value);
            } else {
                linkedList.add(value);
            }
        } else {
            System.out.println("Unknown list encoding");
        }

    }

    /**
     * 使用动态规划实现通配符匹配
     * 参考自 LeetCode 44.通配符匹配的解题思路
     * 检查字符串是否匹配给定的通配符模式
     *
     * @param s 待匹配的字符串（例如一个Redis key）
     * @param p 包含通配符的模式字符串（例如"abc*"）
     * @return 如果字符串s完全匹配模式p，返回true，否则返回false
     */
    public static boolean isMatch(String s, String p) {
        int sLen = s.length();
        int pLen = p.length();

        // dp[i][j] 表示：s 的前 i 个字符是否与 p 的前 j 个字符匹配
        boolean[][] dp = new boolean[sLen + 1][pLen + 1];

        // 基础情况：两个空字符串匹配
        dp[0][0] = true;

        // 处理模式p开头是连续*的情况：*可以匹配空字符串
        for (int j = 1; j <= pLen; j++) {
            if (p.charAt(j - 1) == '*') {
                dp[0][j] = dp[0][j - 1]; // 当前状态依赖于前一个状态
            } else {
                // 遇到非*字符，后续不可能再匹配空字符串，直接跳出循环
                break;
            }
        }

        // 填充dp数组
        for (int i = 1; i <= sLen; i++) {
            for (int j = 1; j <= pLen; j++) {
                char charOfP = p.charAt(j - 1);

                if (charOfP == '*') {
                    // 当遇到'*'时,有两种情况可以使dp[i][j]为true:
                    // 1. 忽略'*'（即*匹配空串）：dp[i][j-1]
                    // 2. 使用'*'匹配当前字符串s[i-1],并继续尝试使用这个'*'匹配s中更前面的字符：dp[i-1][j]
                    dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
                } else {
                    // 当字符精确匹配，该模式中是'?'时，当前字符匹配成功
                    // 并且前名单字符也需匹配成功
                    if (charOfP == '?' || charOfP == s.charAt(i - 1)) {
                        dp[i][j] = dp[i - 1][j - 1];
                    }
                    // 否则，dp[i][j]保持默认的false
                }
            }
        }
        return dp[sLen][pLen];
    }

    /**
     * 返回错误信息
     */
    static class ErrorObject {
        String message;

        ErrorObject(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    /**
     * 客户端请求信息
     */
    static class RedisRequest {
        String command;
        List<String> args;
    }

    static void handleWrite(SelectionKey key) throws IOException {
        RedisClient client = clientMap.get(key);
        if (client == null) {
            return;
        }
        Object retValue = client.retValue;

        SocketChannel socketChannel = (SocketChannel) key.channel();

        // ============= 新增 RESP 协议封装逻辑 =============
        if (client.retValue == null) {
            client.outBuf = "$-1\r\n".getBytes(StandardCharsets.UTF_8);
        } else if (client.retValue instanceof String) {
            String rawValue = (String) client.retValue;
            // 判断响应类型（示例逻辑，需根据实际命令完善）
            if ("OK".equals(rawValue)) {
                client.outBuf = RespUtil.formatSimpleString(rawValue); // +OS\r\n;
            } else {
                client.outBuf = RespUtil.formatBulkString(rawValue); // $5\r\nhello\r\n
            }
        } else if (client.retValue instanceof Integer) {
            client.outBuf = RespUtil.formatInteger((Integer) client.retValue);
        } else if (client.retValue instanceof Throwable) {
            client.outBuf = RespUtil.formatError(((Throwable) client.retValue).getMessage());
        } else if (client.retValue instanceof ErrorObject) {
            client.outBuf = RespUtil.formatError(((ErrorObject) client.retValue).message);
        } else if (client.retValue instanceof ArrayObject) {
            ArrayObject retValue2 = (ArrayObject) client.retValue;
            client.outBuf = RespUtil.formatArray(retValue2.elements);
        }
        // ===============================================
        ByteBuffer buffer = ByteBuffer.wrap(client.outBuf);
        try {
            int totalWritten = 0;
            while (buffer.hasRemaining()) {
                int bytesWritten = socketChannel.write(buffer);
                if (bytesWritten <= 0) {
                    break;
                }
                totalWritten += bytesWritten;
            }

            if (buffer.hasRemaining()) {
                // 分片写入优化
                if (totalWritten < client.outBuf.length) {
                    client.outBuf = Arrays.copyOfRange(
                            client.outBuf, totalWritten, client.outBuf.length
                    );
                }
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                client.write = true;
            } else {
                // 状态清零与事件切换
                // client.outBuf = null;
                client.retValue = null;
                // 取消写事件
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                // key.interestOps(SelectionKey.OP_READ);
                client.write = false;
            }

        } catch (Exception e) {
            System.out.println("write error:" + e.getMessage());
            closeClient(socketChannel, key, client); // 需实现资源释放
        }
    }

    /**
     * 关闭客户端链接
     */
    private static void closeClient(SocketChannel channel, SelectionKey key, RedisClient client) throws IOException {
        channel.close();
        clients.remove(client);
        clientMap.remove(key);
        System.out.println("关闭连接:" + channel);
    }

    /**
     * 定时函数，每秒执行 hz 次，不进行设置默认是 每100ms执行一次
     */
    public static void serverCron() throws InterruptedException {
        /**
         * 1. 关闭redis服务器
         * 2. 关闭超时客户端
         * 3. 对数据执行各种操作， 如过期键删除， 渐进式 rehash
         * 4. aof 持久化
         * 5。rdb 持久化
         * 6. 和主服务器同步数据， run_with_period(1000) - 1000ms一次
         * 7. 集群操作(集群模式)  gossip协议和其他节点通信，故障转移等  run_with_period(100) - 100ms一次
         * 8. sentinel模式，监控主从服务器状态，故障转移等 run_with_period(100) - 100ms一次
         * 9. 数据统计，慢查询日志等
         */
        Thread.sleep(10);

        activeExpireCycle(false);
        clientsCron();
    }

    public static void clientsCron() {
        for (RedisClient client : clients) {
            if (client.flags == 4) {
                if (client.bpop != null && client.bpop.timeout <= System.currentTimeMillis()) {
                    client.write = true;
                    client.retValue = null;

                    // 注册可写   事件
                    SelectionKey key = client.channel.keyFor(selector);
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);

                    client.flags = 0;
                    client.bpop = null;

                }
            }
        }
    }

    // 过期键的删除，主动删除
    public static void activeExpireCycle(boolean flag) {
        /**
         * 1. 要控制这个函数需要跑多久
         * 2. 要控制每次删除键的数量
         */
        // 假设最多跑 10000 个键值对，最多 50ms
        long endTime = System.currentTimeMillis() + 50; // 截止时间
        long maxSum = 10000; // 最大 10000

        if (flag) {
            endTime = System.currentTimeMillis() + 10;
            maxSum = 1000;
        }

        long sum = 0; // 删除的键值对数量
        for (RedisDB db : redisDb) {
            long dbSize = db.dict.getDictSize();
            if (dbSize == 0) {
                continue;
            }

            for (Object entry : db.expires.ht[0].entrySet()) {
                Map.Entry entry1 = (Map.Entry) entry;
                String key = (String) entry1.getKey();
                long expireTime = (Long) entry1.getValue();

                // 判断是否过期
                if (expireTime <= System.currentTimeMillis()) {
                    // 打印
                    RedisObject redisObject = db.dict.getRedisObject(key);
                    System.out.println("过期键主动淘汰:" + key + " value:" + (redisObject != null ? redisObject.toString() : "null"));

                    // 删除
                    db.dict.remove(key);
                    db.expires.remove(key);

                    sum++;
                    if (sum >= maxSum || System.currentTimeMillis() > endTime) {
                        return;
                    }
                }
            }

        }
    }

    static String infoResponse = "#Server\r\n" +
            "redis_version:7.0.0\r\n" +
            "redis_mode:standalone\r\n" +
            "os:Linux 5.4.0 x86_64\r\n" +
            "arch_bits:64\r\n" +
            "multiplexing_api:epoll\r\n" +
            "process_id:12345\r\n" +
            "run_id:abc123def456\r\n" + // 模拟运行ID
            "tcp_port:6379\r\n" +
            "uptime_in_seconds:1000\r\n" +
            "uptime_in_days:0\r\n" +
            "hz:10\r\n" +
            "config_file:/path/to/redis.config\r\n" +
            "\r\n" +
            "# Clients\r\n" +
            "connect_clients:1\r\n" +
            "client_recent_max_input_buffer:2\r\n" +
            "blocked_clients:0\r\n" +
            "\r\n" +
            "# Memory\r\n" +
            "user_memory:1048576\r\n" +
            "user_memory_human:1.00M\r\n" +
            "user_memory_rss_2097152\r\n" +
            "user_memory_peak:2097152\r\n" +
            "user_memory_peak_oerc:50.00%\r\n" +
            "mem_fragmentation_ratio:2.00\r\n" +
            "maxmemory:0\r\n" +
            "maxmemory_policy:noeviction\r\n" +
            "mem_allocator:jemalloc-5.2.1\r\n" +
            "\r\n" +
            "# Persistence\r\n" +
            "loading:0\r\n" +
            "rdb_changes_since_last_save:0\r\n" +
            "rdb_bgsave_in_progress:0\r\n" +
            "aof_enabled:0\r\n" +
            "\r\n" +
            "# Stats\r\n" +
            "total_connections_received:5\r\n" +
            "total_commands_processed:100\r\n" +
            "instantaneous_ops_per_sec:0\r\n" +
            "rejected_connections:0\r\n" +
            "keyspace_hits:50\r\n" +
            "keyspace_misses:10\r\n" +
            "\r\n" +
            "# Replication\r\n" +
            "role:master\r\n" +
            "connected_slaves:0\r\n" +
            "master_repl_offset:0\r\n" +
            "\r\n" +
            "# CPU\r\n" +
            "used_cpu_sys:10.5\r\n" +
            "used_cpu_user:20.3\r\n" +
            "\r\n" +
            "# Keyspace\r\n" +
            "db0:keys=10,expires=0,avg_ttl=0\r\n";

    static class ArrayObject {
        Object[] elements;

        ArrayObject(Object... elements) {
            this.elements = elements;
        }
    }


}
