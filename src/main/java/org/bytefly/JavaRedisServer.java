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

    // 16个db
    static RedisDB[] redisDb;

    // 客户端链接集合
    static List<RedisClient> clients = new ArrayList<>();

    static ServerSocketChannel serverSocketChannel;
    static Selector selector;
    static HashMap<SelectionKey, RedisClient> clientMap = new HashMap<>();

    // 每个db
    static class RedisDB {
        // 存储键值对
        Dict dict = new Dict();

        // 键的过期时间，保存了所有键的过期时间 key = keyName， value = "expireTime"
        Dict expires = new Dict();

        public Object get(String key) {
            return dict.get(key);
        }

        public void set(String key, Object value) {
            dict.set(key, value);
        }

    }

    // 字典
    static class Dict {
        Hashtable[] ht = new Hashtable[2];

        {
            for (int i = 0; i < 2; i++) {
                ht[i] = new Hashtable();
            }
        }

        // -1 表示没有正在进行渐进式hash
        int rehashinx = -1;

        public Object get(String key) {
            if (rehashinx == 1) {
                // 正在进行渐进式 rehash，则需要判断ht[0]和ht[1]
                Object o = ht[0].get(key);
                if (o != null) {
                    return o;
                } else {
                    return ht[1].get(key);
                }
            } else {
                return ht[0].get(key);
            }
        }

        public void set(String key, Object value) {
            if (rehashinx == -1) {
                ht[0].put(key, value); // 没有进行渐进式hash
            } else {
                ht[1].put(key, value); // 正在进行渐进式hash
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


    public static void main(String[] args) throws IOException, InterruptedException {
        initServer();

        long nextEventTime = System.currentTimeMillis();
        while (true) {
            long now = System.currentTimeMillis();
            long timeout = 0;
            if (now < nextEventTime) {
                /**
                 * 还未到达 下次时间事件
                 * 计算超时时间 = 下次时间事件 - 当前时间
                 */
                timeout = nextEventTime - now;
            } else if (now > nextEventTime) {
                // 到达了，设置下次时间事件
                nextEventTime = now + 100;
            }

            // 处理文件事件
            aeProcessEvents(timeout);

            // 处理时间事件
            if (now >= nextEventTime) {
                // 定时任务，默认每秒执行 hz(10) 次
                serverCron();
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

        /** 注册嘟事件 */
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

                Object result = doCommand(client, redisRequest);
                client.retValue = result;
                client.write = true;

                // 注冊写事件
                SelectionKey key = client.channel.keyFor(selector);
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            } else if (processed == 0) {
                break; // 数据不完整，等待下次读取
            } else {
                // closeClient(client.channel, client.channel.keyFor(selector), client);
                break;
            }
        }
    }

    public static Object doCommand(RedisClient redisClient, RedisRequest request) {
        System.out.println("handle command: " + request.command + " args: " + request.args);

        if ("get".equalsIgnoreCase(request.command)) {
            return redisClient.selectedDb.get(request.args.get(0));
        } else if ("Set".equalsIgnoreCase(request.command)) {
            redisClient.selectedDb.set(request.args.get(0), request.args.get(1));
            return "OK";
        } else if ("select".equalsIgnoreCase(request.command)) {
            int dbIndex = Integer.parseInt(request.args.get(0));
            if (dbIndex < 0 | dbIndex >= redisDb.length) {
                return "ERR invalid DB index";
            }
            redisClient.selectedDb = redisDb[dbIndex];
            return "OK";
        } else if ("auth".equalsIgnoreCase(request.command)) {

        }
        return "ERR unknown command '" + request.command + "'";
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
                            client.outBuf,totalWritten,client.outBuf.length
                    );
                }
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                client.write = true;
            }else{
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
    }

}
