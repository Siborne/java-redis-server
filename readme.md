# Java Redis Server

一个用Java编写的简易Redis服务器实现，支持基本的Redis命令和RESP协议。

## 项目概述

该项目实现了Redis的核心功能，包括：
- 基于NIO的高性能网络通信
- RESP协议解析与响应
- 多数据库支持
- 基本Redis命令（GET、SET、SELECT等）

## 核心组件

### JavaRedisServer
Redis服务器主类，负责：
- 启动服务器并监听端口
- 管理多个数据库实例
- 处理客户端连接和请求
- 实现事件循环机制

### SimpleRedisClient
简单的Redis客户端测试程序，用于验证服务器功能：
- 连接到Redis服务器
- 发送认证命令
- 执行SET、GET、SELECT等命令

### RespUtil
RESP协议工具类，提供：
- Redis协议解析功能
- 响应数据格式化

## 支持的功能

- **多数据库**: 支持16个独立的数据库
- **基本命令**:
    - `SET` - 设置键值对
    - `GET` - 获取键值
    - `SELECT` - 切换数据库
    - `AUTH` - 认证（部分实现）
- **协议支持**: RESP协议解析与响应
- **并发处理**: 基于NIO的非阻塞I/O模型

## 技术特点

- 使用Java NIO实现高性能网络通信
- 采用事件驱动架构
- 支持TCP拆包/粘包处理
- 实现了简单的内存存储机制

## 快速开始

### 启动服务器
```bash
javac org/bytefly/JavaRedisServer.java
java org.bytefly.JavaRedisServer
```


### 运行测试客户端
```bash
javac org/bytefly/SimpleRedisClient.java
java org.bytefly.SimpleRedisClient
```


## 项目结构

```
src/main/java/org/bytefly/
├── JavaRedisServer.java    # Redis服务器主程序
├── SimpleRedisClient.java  # 测试客户端
└── RespUtil.java          # RESP协议工具类
```


## 注意事项

这是一个学习性质的简化实现，不适用于生产环境。缺少以下重要功能：
- 持久化存储
- 完整的Redis命令集
- 安全认证机制
- 高可用和集群支持
- 内存管理和优化

## 未来改进方向

- 实现完整的Redis命令集
- 添加持久化功能（RDB/AOF）
- 支持数据过期和清理机制
- 优化内存使用和性能
- 添加配置文件支持