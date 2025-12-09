# Java Redis Server

一个用Java编写的简易Redis服务器实现，支持基本的Redis命令和RESP协议。

## 项目概述

该项目实现了Redis的核心功能，包括：
- 基于NIO的高性能网络通信
- RESP协议解析与响应
- 多数据库支持
- 基本Redis命令（GET、SET、SELECT等）
- 数据结构实现（字符串、列表、有序集合等）
- 内存淘汰策略
- 事务支持

## 核心组件

### JavaRedisServer
Redis服务器主类，负责：
- 启动服务器并监听端口
- 管理多个数据库实例
- 处理客户端连接和请求
- 实现事件循环机制
- 内存管理和淘汰策略

### SimpleRedisClient
简单的Redis客户端测试程序，用于验证服务器功能：
- 连接到Redis服务器
- 发送认证命令
- 执行SET、GET、SELECT等命令

### RespUtil
RESP协议工具类，提供：
- Redis协议解析功能
- 响应数据格式化

### 数据结构实现

#### ZipList
压缩列表实现，用于存储小型列表数据：
- 支持从头部和尾部插入元素
- 实现了基本的遍历功能
- 模拟Redis的ziplist编码

#### ScoredSkipList
基于分值的跳表实现，类似于Redis的有序集合(ZSet)：
- 支持按分值排序的元素存储
- 提供高效的插入、删除、查找操作
- 支持范围查询功能

## 支持的功能

- **多数据库**: 支持16个独立的数据库
- **基本命令**:
  - `SET` - 设置键值对
  - `GET` - 获取键值
  - `SELECT` - 切换数据库
  - `AUTH` - 认证（部分实现）
  - `EXPIRE` - 设置键过期时间
  - `TTL` - 查询键剩余生存时间
  - `LPUSH` - 向列表头部插入元素
  - `LRANGE` - 获取列表指定范围元素
  - `ZADD` - 向有序集合添加元素
  - `ZRANGE` - 获取有序集合指定范围元素
- **数据结构**:
  - 字符串(String)
  - 列表(List) - 使用ZipList实现
  - 有序集合(Sorted Set) - 使用跳表实现
- **协议支持**: RESP协议解析与响应
- **并发处理**: 基于NIO的非阻塞I/O模型
- **内存管理**: 支持多种内存淘汰策略
- **事务支持**: 基本的MULTI/EXEC/WATCH/DISCARD命令实现

## 技术特点

- 使用Java NIO实现高性能网络通信
- 采用事件驱动架构
- 支持TCP拆包/粘包处理
- 实现了简单的内存存储机制
- 支持多种Redis数据结构的编码方式
- 实现了Redis的内存淘汰策略

## 项目结构

```
src/main/java/org/bytefly/
├── JavaRedisServer.java    # Redis服务器主程序
├── SimpleRedisClient.java  # 测试客户端
├── RespUtil.java          # RESP协议工具类
├── ZipList.java           # 压缩列表实现
├── ScoredSkipList.java    # 跳表实现
├── Multi.java             # 事务处理
├── RedisConstants.java    # Redis常量定义
└── readme.md              # 项目说明文档
```


## 内存淘汰策略

支持多种内存淘汰策略：
- `VOLATILE_LRU`: 从设置了过期时间的键中使用LRU算法淘汰
- `VOLATILE_TTL`: 从设置了过期时间的键中选择TTL最小的淘汰
- `VOLATILE_RANDOM`: 从设置了过期时间的键中随机淘汰
- `ALLKEYS_LRU`: 从所有键中使用LRU算法淘汰
- `ALLKEYS_RANDOM`: 从所有键中随机淘汰
- `NO_EVICTION`: 不淘汰数据，内存满时返回错误

## 数据结构编码

根据Redis的设计原则，不同数据类型会根据数据规模使用不同的底层编码：

- **列表(List)**: 小型列表使用`ZIPLIST`编码，大型列表使用`LINKEDLIST`编码
- **有序集合(ZSet)**: 小型有序集合使用`ZIPLIST`编码，大型有序集合使用`SKIPLIST`编码
- **字符串(String)**: 根据长度和内容使用`INT`、`RAW`或`EMBSTR`编码

## 快速开始

### 启动服务器
```bash
javac org/bytefly/JavaRedisServer.java
java org/bytefly/JavaRedisServer
```


### 运行测试客户端
```bash
javac org/bytefly/SimpleRedisClient.java
java org/bytefly/SimpleRedisClient
```


## 注意事项

这是一个学习性质的简化实现，不适用于生产环境。缺少以下重要功能：
- 持久化存储
- 完整的Redis命令集
- 安全认证机制
- 高可用和集群支持
- 完善的错误处理机制
- 性能优化

## 未来改进方向

- 实现完整的Redis命令集
- 添加持久化功能（RDB/AOF）
- 支持数据过期和清理机制
- 优化内存使用和性能
- 添加配置文件支持
- 实现主从复制功能
- 添加更多数据结构支持

## 致谢

特别感谢哔哩哔哩的 @黑鸡咕咕Debug 提供的手写Redis视频内容，本项目参考了其中的实现思路和设计模式。