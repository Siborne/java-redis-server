package org.bytefly;

import java.util.*;

/**
 * 基于分值（Score）的跳表实现，类似Redis的Zset概念。
 * 节点按照分值排序，支持高效的插入、删除、查找和范围查询。
 */
public class ScoredSkipList<T> {
    /**
     * 跳表节点定义
     */
    static class SkipListNode<T> {
        T value; // 存储的数据
        double score; // 用于排序的分值
        SkipListNode<T>[] forward; // 前进指针数组，forward[i]表示第i层的下一个节点
        int level; // 节点实际拥有的层数

        public T getValue() {
            return value;
        }

        @SuppressWarnings("unchecked")
        public SkipListNode(T value, double score, int level) {
            this.value = value;
            this.score = score;
            this.level = level;
            this.forward = new SkipListNode[level + 1]; // 创建层指针数组
        }

        @Override
        public String toString() {
            return "(" + value + ":" + score + ")";
        }
    }

    // 常量定义
    private static final int DEFAULT_MAX_LEVEL = 16; // 默认最大层数
    private static final double DEFAULT_PROBABILITY = 0.5; // 层数增长概率

    // 跳表属性
    private final int maxLevel;
    private final double probability;
    private final SkipListNode<T> header; // 头节点，不存储数据
    private int currentLevel; // 当前跳表的最大层数（不包括头节点）
    private int size; // 元素个数
    private final Random random;
    private final Comparator<T> valueComparator; // 用于分值相同时的值比较器

    /**
     * 使用默认参数构造跳表
     */
    public ScoredSkipList() {
        this(DEFAULT_MAX_LEVEL, DEFAULT_PROBABILITY, null);
    }

    /**
     * 自定义参数构造跳表
     *
     * @param maxLevel        最大层数
     * @param probability     层数增长概率
     * @param valueComparator 值比较器（用于分值相同时的比较）
     */
    @SuppressWarnings("unchecked")
    public ScoredSkipList(int maxLevel, double probability, Comparator<T> valueComparator) {
        if (maxLevel <= 0) throw new IllegalArgumentException("最大层数必须为正整数");
        if (probability <= 0 || probability >= 1) throw new IllegalArgumentException("概率必须在(0,1)区间内");

        this.maxLevel = maxLevel;
        this.probability = probability;
        this.valueComparator = (valueComparator != null) ? valueComparator :
                (a, b) -> ((Comparable<T>) a).compareTo(b); // 默认使用自然排序

        // 创建头节点，分值为最小，拥有最大层数
        this.header = new SkipListNode<>(null, Double.MIN_VALUE, maxLevel);
        this.currentLevel = 1; // 初始为1层
        this.size = 0;
        this.random = new Random();

        // 初始化头节点的指针
        for (int i = 0; i <= maxLevel; i++) {
            header.forward[i] = header; // 初始指向自己，形成环形结构便于遍历
        }
    }

    /**
     * 生成随机层数（基于概率的指数分布）
     */
    private int randomLevel() {
        int level = 1;
        while (level < maxLevel && random.nextDouble() < probability) {
            level++;
        }
        return level;
    }

    /**
     * 插入元素
     *
     * @param score 排序分值
     * @param value 元素值
     * @return 是否插入成功（如果分值和新值都相同，则不会重复插入）
     */
    public boolean insert(double score, T value) {
        // update数组记录每层需要更新的节点（即插入位置的前驱）
        @SuppressWarnings("unchecked")
        SkipListNode<T>[] update = new SkipListNode[maxLevel + 1];
        Arrays.fill(update, header); // 初始化为头节点

        SkipListNode<T> current = header;

        // 从最高层开始查找插入位置
        for (int i = currentLevel; i >= 0; i--) {
            while (current.forward[i] != header &&
                    (current.forward[i].score < score ||
                            (current.forward[i].score == score &&
                                    compareValues(current.forward[i].value, value) < 0))) {
                current = current.forward[i];
            }
            update[i] = current; // 记录该层最后访问的节点
        }

        current = current.forward[0];

        // 检查是否已存在（分值和值都相同）
        if (current != header && current.score == score && compareValues(current.value, value) == 0) {
            return false; // 已存在，不插入
        }

        // 为新区随机生成层数
        int newLevel = randomLevel();

        // 如果新节点的层数大于当前跳表的最大层数，更新高层update指针
        if (newLevel > currentLevel) {
            for (int i = currentLevel + 1; i <= newLevel; i++) {
                update[i] = header;
            }
            currentLevel = newLevel;
        }

        // 创建新节点
        SkipListNode<T> newNode = new SkipListNode<>(value, score, newLevel);

        // 逐层插入新节点
        for (int i = 0; i <= newLevel; i++) {
            newNode.forward[i] = update[i].forward[i];
            update[i].forward[i] = newNode;
        }

        size++;
        return true;
    }

    /**
     * 查找指定分值和值的节点
     *
     * @param score 分值
     * @param value 值
     * @return 找到的节点，未找到返回null
     */
    public SkipListNode<T> find(double score, T value) {
        SkipListNode<T> current = header;

        // 从最高层开始查找
        for (int i = currentLevel; i >= 0; i--) {
            while (current.forward[i] != header &&
                    (current.forward[i].score < score ||
                            (current.forward[i].score == score &&
                                    compareValues(current.forward[i].value, value) < 0))) {
                current = current.forward[i];
            }
        }

        current = current.forward[0];

        // 检查是否找到
        if (current != header && current.score == score && compareValues(current.value, value) == 0) {
            return current;
        }

        return null;
    }

    /**
     * 查找指定分值的所有节点
     *
     * @param score 分值
     * @return 包含所有匹配节点的列表
     */
    public List<SkipListNode<T>> findByScore(double score) {
        List<SkipListNode<T>> result = new ArrayList<>();
        SkipListNode<T> current = header;

        // 从最高层开始查找第一个分值等于score的节点
        for (int i = currentLevel; i >= 0; i--) {
            while (current.forward[i] != header && current.forward[i].score < score) {
                current = current.forward[i];
            }
        }

        // 在最底层遍历所有分值等于score的节点
        current = current.forward[0];
        while (current != header && current.score == score) {
            result.add(current);
            current = current.forward[0];
        }

        return result;
    }

    /**
     * 范围查询，查找分值在[minScore, maxScore]之间的所有节点
     *
     * @param minScore 最小分值（包含）
     * @param maxScore 最大分值（包含）
     * @return 范围内的所有节点列表
     */
    public List<SkipListNode<T>> rangeByScore(double minScore, double maxScore) {
        List<SkipListNode<T>> result = new ArrayList<>();
        SkipListNode<T> current = header;

        // 找到第一个大于等于minScore的节点
        for (int i = currentLevel; i >= 0; i--) {
            while (current.forward[i] != header && current.forward[i].score < minScore) {
                current = current.forward[i];
            }
        }

        current = current.forward[0];

        // 遍历直到超过maxScore
        while (current != header && current.score <= maxScore) {
            result.add(current);
            current = current.forward[0];
        }

        return result;
    }

    /**
     * 删除指定分值和值的节点
     *
     * @param score 分值
     * @param value 值
     * @return 是否删除成功
     */
    public boolean delete(double score, T value) {
        @SuppressWarnings("unchecked")
        SkipListNode<T>[] update = new SkipListNode[maxLevel + 1];
        SkipListNode<T> current = header;

        // 查找要删除的节点及其前驱
        for (int i = currentLevel; i >= 0; i--) {
            while (current.forward[i] != header &&
                    (current.forward[i].score < score ||
                            (current.forward[i].score == score &&
                                    compareValues(current.forward[i].value, value) < 0))) {
                current = current.forward[i];
            }
            update[i] = current;
        }

        current = current.forward[0];

        // 如果没找到要删除的节点
        if (current == header || current.score != score || compareValues(current.value, value) != 0) {
            return false;
        }

        // 逐层更新指针，跳过要删除的节点
        for (int i = 0; i <= currentLevel; i++) {
            if (update[i].forward[i] != current) {
                break;
            }
            update[i].forward[i] = current.forward[i];
        }

        // 如果删除的是最高层节点，更新跳表层数
        while (currentLevel > 1 && header.forward[currentLevel] == header) {
            currentLevel--;
        }

        size--;
        return true;
    }

    /**
     * 更新元素的分值（先删除旧值，再插入新值）
     *
     * @param oldScore 旧分值
     * @param newScore 新分值
     * @param value    元素值
     * @return 是否更新成功
     */
    public boolean update(double oldScore, double newScore, T value) {
        if (delete(oldScore, value)) {
            return insert(newScore, value);
        }
        return false;
    }

    /**
     * 比较两个值（用于分值相同时的情况）
     */
    @SuppressWarnings("unchecked")
    private int compareValues(T a, T b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        if (valueComparator != null) {
            return valueComparator.compare(a, b);
        }

        // 默认使用Comparable
        return ((Comparable<T>) a).compareTo(b);
    }

    /**
     * 获取跳表元素个数
     */
    public int size() {
        return size;
    }

    /**
     * 跳表是否为空
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * 打印跳表结构（用于调试）
     */
    public void printSkipList() {
        System.out.println("=== 跳表结构展示（层数: " + currentLevel + ", 大小: " + size + "）===");

        for (int i = currentLevel; i >= 0; i--) {
            System.out.print("Level " + i + ": header");
            SkipListNode<T> node = header.forward[i];

            int count = 0;
            while (node != header && count < 50) { // 防止无限循环
                System.out.print(" -> " + node);
                node = node.forward[i];
                count++;
            }
            System.out.println();
        }
        System.out.println("=== 展示结束 ===");
    }

    /**
     * 测试用例
     */
    public static void main(String[] args) {
        ScoredSkipList<String> skipList = new ScoredSkipList<>();

        System.out.println("=== 带分值的跳表测试 ===");

        // 测试插入
        System.out.println("插入元素:");
        skipList.insert(3.0, "苹果");
        skipList.insert(1.5, "香蕉");
        skipList.insert(2.5, "橙子");
        skipList.insert(2.5, "橘子"); // 相同分值，不同值
        skipList.insert(4.0, "芒果");

        skipList.printSkipList();

        // 测试查找
        System.out.println("\n=== 查找测试 ===");
        System.out.println("查找(2.5, 橙子): " + skipList.find(2.5, "橙子"));
        System.out.println("查找(2.5, 苹果): " + skipList.find(2.5, "苹果")); // 不存在

        // 测试分值查找
        System.out.println("查找分值为2.5的所有节点: " + skipList.findByScore(2.5));

        // 测试范围查询
        System.out.println("查找分值在2.0到3.5之间的节点: " + skipList.rangeByScore(2.0, 3.5));

        // 测试删除
        System.out.println("\n=== 删除测试 ===");
        System.out.println("删除(2.5, 橙子): " + skipList.delete(2.5, "橙子"));
        skipList.printSkipList();

        // 测试更新
        System.out.println("\n=== 更新测试 ===");
        System.out.println("将(1.5, 香蕉)更新为(5.0, 香蕉): " +
                skipList.update(1.5, 5.0, "香蕉"));
        skipList.printSkipList();

        // 性能测试
        System.out.println("\n=== 性能测试 - 插入1000个随机元素 ===");
        ScoredSkipList<Integer> perfTest = new ScoredSkipList<>();
        int testSize = 1000;

        long startTime = System.currentTimeMillis();
        Random rand = new Random();
        for (int i = 0; i < testSize; i++) {
            double score = rand.nextDouble() * 100;
            perfTest.insert(score, i);
        }
        long endTime = System.currentTimeMillis();

        System.out.println("插入 " + testSize + " 个元素耗时: " + (endTime - startTime) + "ms");
        System.out.println("跳表大小: " + perfTest.size());
        System.out.println("范围查询(25.0, 75.0)结果数量: " + perfTest.rangeByScore(25.0, 75.0).size());
    }

}
