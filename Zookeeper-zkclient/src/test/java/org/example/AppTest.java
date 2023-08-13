package org.example;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest {

    private ZkClient zkClient;

    /**
     * 获取Zookeeper连接
     * 初始化客户端对象
     */
    @Before
    public void before() {
        //参数1  --> zk server 服务ip地址:端口号,服务ip地址:端口号,......
        //参数2 -->  会话超时时间 单位ms
        //参数3  --> 连接超时时间 单位ms
        //参数4  --> 序列化方式   JDK序列号方式

        // 连接单机
//        zkClient = new ZkClient("192.168.131.130:2181", 1000 * 60 * 30, 1000 * 60, new SerializableSerializer());

        // 连接集群
        zkClient = new ZkClient("192.168.131.130:3001,192.168.131.130:4001,192.168.131.130:5001", 1000 * 60 * 30, 1000 * 60, new SerializableSerializer());
    }

    /**
     * 连接测试
     */
    @Test
    public void test() {
        System.out.println(zkClient);
    }

    /**
     * 查看节点的子节点
     */
    @Test
    public void testFindNodes() {
        //获取指定路径的节点信息  //返回值: 为当前节点的子节点信息
        List<String> children = zkClient.getChildren("/");
        for (String child : children) {
            System.out.println(child);
        }
    }

    /**
     * 创建结点 返回创建节点的名称
     */
    @Test
    public void testCreateNode1() {
        // 持久结点
        zkClient.create("/appP", "app", CreateMode.PERSISTENT);
        // 持久顺序结点
        zkClient.create("/appPS", "data", CreateMode.PERSISTENT_SEQUENTIAL);
        // 临时结点
        zkClient.create("/appE", "data", CreateMode.EPHEMERAL);
        // 临时顺序结点
        zkClient.create("/appES", "data", CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    /**
     * 创建结点 不返回创建节点的名称
     */
    @Test
    public void testCreateNode2() {
        // 持久结点
        zkClient.createPersistent("/appP", "app");
        // 持久顺序结点
        zkClient.createPersistentSequential("/appPS", "data");
        // 临时结点
        zkClient.createEphemeral("/appE", "data");
        // 临时顺序结点
        zkClient.createEphemeralSequential("/appES", "data");
    }

    /**
     * 删除结点
     */
    @Test
    public void testDeleteNode() {
        // 删除没有子节点的节点  返回值:是否删除成功
        boolean delete = zkClient.delete("/app");
        // 递归删除节点信息     返回值:是否删除成功
        boolean recursive = zkClient.deleteRecursive("/app");
    }

    /**
     * 获取节点数据
     * 注意:如果出现:org.I0Itec.zkclient.exception.ZkMarshallingError: java.io.StreamCorruptedException: invalid stream header: 61616161
     * 异常的原因是: 在shell中的数据序列化方式 和 java代码中使用的序列化方式不一致导致  因此要解决这个问题只需要保证序列化一致即可  都使用相同端操作即可
     */
    @Test
    public void testFindNodeData() {
        Object readData = zkClient.readData("/appP");
        System.out.println(readData);

        Stat stat = new Stat();
        Object readDataStat = zkClient.readData("/appP", stat);
        System.out.println(readDataStat);
        System.out.println(stat);
    }

    /**
     * 修改节点数据
     */
    @Test
    public void testUpdateNodeData() {
        zkClient.writeData("/appP", new User("1001", "ylan", "19"));
    }

    /**
     * 监听结点数据变化
     * 在shell终端操作，Java是监听不了的，必须使用Java代码操作
     * 并发现Java代码进行监听是永久的，不是一次性的
     */
    @Test
    public void testOnNodeDataChange() throws IOException {
        zkClient.subscribeDataChanges("/appP", new IZkDataListener() {
            // 当节点的值被修改时,会自动调用这个方法  将当前修改节点的名字和节点变化之后的数据传递给方法
            public void handleDataChange(String nodeName, Object result) throws Exception {
                System.out.println("修改节点的名字:" + nodeName);
                System.out.println("修改后的内容:" + result);
            }

            // 当节点被删除时,会自动调用这个方法  将当前删除将节点的名字传递给方法
            public void handleDataDeleted(String nodeName) throws Exception {
                System.out.println("删除节点的名字:" + nodeName);
            }
        });
        // 阻塞客户端
        System.in.read();
    }

    /**
     * 监听节点的变化
     * 在shell终端操作，Java是监听不了的，必须使用Java代码操作
     * 并发现Java代码进行监听是永久的，不是一次性的
     */
    @Test
    public void testOnNodesChange() throws IOException {
        zkClient.subscribeChildChanges("/appP", new IZkChildListener() {
            // 当节点的发生变化时，会自动调用这个方法
            // 参数1:父节点名称
            // 参数2:父节点中的所有子节点名称
            public void handleChildChange(String nodeName, List<String> list) throws Exception {
                System.out.println("父节点名称: " + nodeName);
                System.out.println("发生变更后字节孩子节点名称:");
                for (String name : list) {
                    System.out.println(name);
                }
            }
        });
        //阻塞客户端
        System.in.read();
    }

    /**
     * 释放资源
     */
    @After
    public void after() throws InterruptedException {
        Thread.sleep(10000); // 线程休眠  方便我们查看创建的临时结点
        zkClient.close();
    }
}