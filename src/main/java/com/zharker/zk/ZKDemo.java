package com.zharker.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class ZKDemo {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        String url = "localhost:8211";
        ZKTools zkTools = ZKTools.getInstance();
        ZooKeeper zk = zkTools.connect(url);
        long sessionid = zk.getSessionId();
        System.out.println(sessionid);

        String children = zkTools.list("path",false).stream().collect(Collectors.joining(","));
        System.out.println("children:"+children);

        zkTools.create("path/createbyjava","23333".getBytes(),false,false);

        children = zkTools.list("path",false).stream().collect(Collectors.joining(","));
        System.out.println("children:"+children);

        byte[] getResult = zkTools.get("path/createbyjava",true);
        System.out.println("result:"+new String(getResult));

        boolean exist = zkTools.exist("path/noexist",true);
        System.out.println("exist:"+exist);

        zkTools.set("path/createbyjava","1111".getBytes());

        Thread.sleep(5000);
        zk.close();
    }

}

class ZKTools {
    private ZooKeeper zk;
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final int SESSION_TIMEOUT = 5000;
    public ZooKeeper connect(String url) throws IOException, InterruptedException {
        zk = new ZooKeeper(url,SESSION_TIMEOUT,(we)->{
            if(we.getState()== Watcher.Event.KeeperState.SyncConnected){
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        return zk;
    }
    public void close() throws InterruptedException {
        zk.close();
    }

    private ZKTools(){}
    private static final ZKTools instance = new ZKTools();
    public static ZKTools getInstance(){return instance;}

    public List<String> list(String znode,boolean watch) throws KeeperException, InterruptedException {
        String path = "/"+znode;
        return zk.getChildren(path,watch);
    }

    public void create(String znode,byte[] data,boolean isS, boolean isE) throws KeeperException, InterruptedException {
        if(exist(znode,false)){
            return ;
        }
        String path = "/"+znode;
        CreateMode createMode = CreateMode.PERSISTENT;
        if(isS && isE){
            createMode = CreateMode.EPHEMERAL_SEQUENTIAL;
        }else if(isS){
            createMode = CreateMode.PERSISTENT_SEQUENTIAL;
        }else if(isE){
            createMode = CreateMode.EPHEMERAL;
        }
        zk.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
    }

    public void set(String znode, byte[] data) throws KeeperException, InterruptedException {
        String path = "/"+znode;
        Stat stat = zk.setData(path,data,-1);
    }
    public byte[] get(String znode, boolean watch) throws KeeperException, InterruptedException {
        String path = "/"+znode;
        return zk.getData(path,watch,null);
    }
    public void del(String znode) throws KeeperException, InterruptedException {
        String path = "/"+znode;
        zk.delete(path, -1);
    }
    public boolean exist(String znode,boolean watch) throws KeeperException, InterruptedException {
        String path = "/"+znode;
        Stat stat = zk.exists(path,watch);
        return stat != null;
    }
}
