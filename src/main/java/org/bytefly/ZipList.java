package org.bytefly;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZipList {

    private static final byte ZIPLIST_END = (byte) 0xFF;

    // 内部存储，使用字节数组模拟连续内存
    private byte[] data;

    private int totalLength; // zlbtes
    private int tailOffset; // zltail
    private int entryCount; // zllen

    // 创建一个新的空 zipList
    public ZipList() {
        // 初始化头部：zlbytes(4)+ zltail(4) +zllen(2)+zlend(1)
        data = new byte[11];
        totalLength = 11; // 4+4+2+1
        tailOffset = 11; // zltail 指向 zlend 的位置
        entryCount = 0;

        // 设置 zlbytes, zltail, zllen
        setZlbytes(totalLength);
        setZltail(tailOffset);
        setZllen(entryCount);

        // 设置 zlend
        data[data.length - 1] = ZIPLIST_END;
    }

    // 设置 zlbytes（4字节）
    private void setZlbytes(int value) {
        data[0] = (byte) (value >> 24);
        data[1] = (byte) (value >> 16);
        data[2] = (byte) (value >> 8);
        data[3] = (byte) value;
    }

    // 设置 zltail（4字节）
    public void setZltail(int value) {
        data[4] = (byte) (value >> 24);
        data[5] = (byte) (value >> 16);
        data[6] = (byte) (value >> 8);
        data[7] = (byte) value;
    }

    // 设置 zllen（2字节）
    public void setZllen(int value) {
        data[8] = (byte) (value >> 8);
        data[9] = (byte) value;
    }

    // 添加字符串到 ziplist 末尾
    public void addString(String s) {
        // 1. 计算新元素所需空间
        int entryLength = calculateEntryLength(s);
        int requiredSpace = entryLength + 4; // prevlen(4) + entry

        // 2. 扩容（实际实现中需要更复杂的扩容逻辑）
        if (data.length < totalLength + requiredSpace) {
            byte[] newData = new byte[totalLength + requiredSpace + 10]; // 额外预留空间
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
        }

        // 3. 计算 prevlen（当前表尾到新元素的偏移量）
        int prevlen = totalLength - tailOffset;

        // 4. 写入新元素
        int pos = tailOffset;
        pos = writePrevlen(data, pos, prevlen);
        pos = writeEncoding(data, pos, s);
        pos = writeEntryData(data, pos, s);

        // 5. 更新头部信息
        totalLength += requiredSpace;
        tailOffset = pos;
        entryCount++;

        setZlbytes(totalLength);
        setZltail(tailOffset);
        setZllen(entryCount);
    }

    /**
     * 从头部插入字符串（类似 Redis 的 LPUSH 操作）
     * T = O(N) （因为需要移动整个 ziplist 数据）
     */
    public void insertFromHead(String s) {
        // 1. 计算新节点所需空间（包含 prevlen + encoding + entry-data）
        int entryLength = calculateEntryLength(s);
        int requiredSpace = entryLength + 4; // prevlen(4) + entry

        // 2. 计算当前 ziplist 长度（用于后续内存移动）
        int currentLength = totalLength;

        // 3. 扩容（确保有足够的空间）
        if (data.length < currentLength + requiredSpace) {
            byte[] newData = new byte[currentLength + requiredSpace + 10];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
        }

        // 4. 将现有数据（从头部开始）向后移动 requiredSpace 字节
        // 注意：需要移动从索引 11（第一个节点位置）到 tailOffset-1 的数据
        System.arraycopy(data, 11, data, 11 + requiredSpace, tailOffset - 11);

        // 5. 写入新节点（在头部位置 11）
        int pos = 11;

        // 写入 prevlen = 0（第一个节点没有前置节点）
        pos = writePrevlen(data, pos, 0);

        // 写入 encoding（根据字符串类型）
        pos = writeEncoding(data, pos, s);

        // 写入 entry-data
        pos = writeEntryData(data, pos, s);

        // 6. 更新头部信息
        totalLength += requiredSpace;
        tailOffset += requiredSpace; // 表尾偏移量增加
        entryCount++;

        setZlbytes(totalLength);
        setZltail(tailOffset);
        setZllen(entryCount);
    }

    // 计算字符串元素所需的长度
    private int calculateEntryLength(String s) {
        // 实际中需要根据字符串长度计算，这里简化
        return s.length() + 2; // 假设 encoding 占2字节
    }

    // 写入 prevlen
    private int writePrevlen(byte[] data, int pos, int prevlen) {
        // 实际实现中需要处理不同长度的 prevlen
        // 这里简化为写入4字节
        data[pos] = (byte) (prevlen >> 24);
        data[pos + 1] = (byte) (prevlen >> 16);
        data[pos + 2] = (byte) (prevlen >> 8);
        data[pos + 3] = (byte) prevlen;
        return pos + 4;
    }

    // 写入 encoding
    private int writeEncoding(byte[] data, int pos, String s) {
        // 实际实现中需要根据字符串长度选择编码
        // 这里简化为写入固定2字节
        data[pos] = (byte) 0x00; // 简化编码
        data[pos + 1] = (byte) 0x00;
        return pos + 2;
    }

    // 写入 entry-data
    private int writeEntryData(byte[] data, int pos, String s) {
        // 写入字符串内容
        for (int i = 0; i < s.length(); i++) {
            data[pos + i] = (byte) s.charAt(i);
        }
        return pos + s.length();
    }

    // 从尾部遍历
    public String getFromTail(int index) {
        int pos = tailOffset - 1; // 指向 zlend
        for (int i = 0; i < index; i++) {
            // 从后往前遍历
            // 实际实现需要根据 prevlen 计算上一个节点的位置
            pos = getPrevNodePosition(data, pos);
        }
        // 解析节点内容
        return parseEntry(data, pos);
    }

    private int getPrevNodePosition(byte[] data, int pos) {
        // 从当前节点获取前一个节点的位置
        // 实际实现需要读取 prevlen 字段
        int prevlen = readPrevlen(data, pos);
        return pos - prevlen;
    }

    private int readPrevlen(byte[] data, int pos) {
        // 读取 prevlen 字段
        return ((data[pos - 4] & 0xFF) << 24) |
                ((data[pos - 3] & 0xFF) << 16) |
                ((data[pos - 2] & 0xFF) << 8) |
                (data[pos - 1] & 0xFF);
    }

    private String parseEntry(byte[] data, int pos) {
        // 解析节点内容
        // 实际实现需要根据 encoding 和 entry-data 解析
        // 这里简化为直接读取字符串
        int start = pos - 2; // 跳过 encoding
        int length = 0;
        while (data[start + length] != 0) { // 简化：假设字符串以0结尾
            length++;
        }
        byte[] bytes = Arrays.copyOfRange(data, start, start + length);
        return new String(bytes);
    }


    /**
     * 获取指定范围的元素（类似 Redis 的 LRANGE 命令）
     *
     * @param start 起始索引（0-based，可为负数表示从尾部开始）
     * @param end   结束索引（0-based，可为负数表示从尾部开始）
     * @return 指定范围的字符串列表（顺序为头部到尾部）
     */

    public List<String> range(int start, int end) {
        List<String> list = new ArrayList<>();
        int pos = 11; // 跳过头部
        while (pos < tailOffset) {
            int prevlen = readPrevlen(data, pos);
            // 读取 encoding (简化)
            pos += 2; // 跳过 encoding
            // 读取 entry-data
            int len = 0;
            while (pos + len < tailOffset && data[pos + len] != 0) {
                len++;
            }
            if (len > 0) {
                String s = new String(Arrays.copyOfRange(data, pos, pos + len));
                list.add(s);
            }
            pos += len;
        }
        if (end == -1) {
            return list.subList(start, list.size());
        }
        return list.subList(start, Math.min(end, list.size()));
    }

    public List<String> range1(int start, int end) {
        // 处理空列表
        if (entryCount == 0) {
            return new ArrayList<>();
        }

        // 转换负索引（-1 表示最后一个元素，-2 表示倒数第二个...）
        if (start < 0) {
            start = entryCount + start;
        }
        if (end < 0) {
            end = entryCount + end;
        }

        // 调整边界到有效范围 [0, entryCount-1]
        if (start < 0) start = 0;
        if (end >= entryCount) end = entryCount - 1;
        if (start > end) {
            return new ArrayList<>();
        }

        // 计算尾部索引范围（用于通过 getFromTail 获取元素）
        int startTail = entryCount - 1 - end;  // 头部索引 end 对应的尾部索引
        int endTail = entryCount - 1 - start;  // 头部索引 start 对应的尾部索引

        // 从尾部索引范围获取元素（按尾部顺序，需要反转）
        List<String> result = new ArrayList<>();
        for (int i = endTail; i >= startTail; i--) {
            result.add(getFromTail(i));
        }
        return result;
    }

    // 打印 ziplist 内容（用于调试）
    public void print() {
        List<String> range = range(0, entryCount - 1);
        System.out.println("ZipList:" + range.toString());
    }

    public static void main(String[] args) {
        ZipList ziplist = new ZipList();
        ziplist.addString("1");
        ziplist.addString("118769999999999999");
        ziplist.addString("3");
        ziplist.addString("4");
        ziplist.addString("5");

        ziplist.print();
        List<String> range = ziplist.range(0, 3);
        System.out.println("Range: " + range.toString());
    }

}
