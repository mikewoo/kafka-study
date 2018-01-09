package com.study.kafka.election;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;
import java.util.TreeSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class LeaderElection {
	
	private static Object lock = new Object();

	private final String ROOT = "/kafka";

	private final String LEADER = "leader";
	
	private final byte[] DEFAULT_DATA = {0x11, 0x12};

	private ZooKeeper zk;

	private String znode;

	private byte[] localhost;

	public LeaderElection(String connectString, int sessionTimeout, Watcher watcher) throws Exception {
		super();
		this.zk = new ZooKeeper(connectString, sessionTimeout, watcher);
		localhost = getLocalIpAdressBytes();
		checkRootExists(ROOT);
		znode = createLeaderNode(LEADER);
	}

	private String createLeaderNode(String node) throws Exception {
		return zk.create(LEADER, localhost, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}
	
	private void checkRootExists(String root) throws Exception {
		Stat state = zk.exists(ROOT, false);
		if (state == null) {
			zk.create(ROOT, DEFAULT_DATA, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
	
	public void electorUntilGetLeaderShip() throws Exception {
		while (true) {
			synchronized (lock) {
				TreeSet<String> sortedSetNodes = getChildNodes();
				String first = sortedSetNodes.first();// 获取最小节点
				if ((znode).equals(first)) {
					// 当前节点为leader
					byte[] data = zk.getData(first, null, null);  
					System.out.println("当前节点为leader: " + znode + " data: " + new String(data, "UTF-8"));
					lock.notify();
					return ;
				} 
				// 监听前一个节点
		        String preNode = sortedSetNodes.floor(znode);
		        zk.getData(preNode, new Watcher() {
					public void process(WatchedEvent event) {
						if (event.getType() == EventType.NodeDeleted) {
							try {
								TreeSet<String> sortedSetNodes = getChildNodes();
                                     // 获取最小节点
								String first = sortedSetNodes.first();
								if ((znode).equals(first)) {
									// 当前节点为leader
									byte[] data = zk.getData(first, null, null);  
									System.out.println("当前节点为leader: " + znode + " data: " + new String(data, "UTF-8"));
					                lock.notify();  
									return;
								} 
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}, null);
		        lock.wait(); 
			}
		}
	}
	
	public boolean elector() throws Exception {
		TreeSet<String> sortedSetNodes = getChildNodes();
		// 获取最小节点
		String first = sortedSetNodes.first();
		if ((znode).equals(first)) {
			// 当前节点为leader
			byte[] data = zk.getData(first, null, null);  
			System.out.println("当前节点为leader: " + znode + " data: " + new String(data, "UTF-8"));
			return true;
		} 
		// 监听前一个节点
        String preNode = sortedSetNodes.floor(znode);
        zk.getData(preNode, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getType() == EventType.NodeDeleted) {
					try {
						TreeSet<String> sortedSetNodes = getChildNodes();
						String first = sortedSetNodes.first();// 获取最小节点
						if ((znode).equals(first)) {
							// 当前节点为leader
							byte[] data = zk.getData(first, null, null);  
							System.out.println("当前节点为leader: " + znode + " data: " + new String(data, "UTF-8"));
							return;
						} 
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}, null);
		return false;
	}


	private TreeSet<String> getChildNodes() throws KeeperException, InterruptedException {
		List<String> nodes = zk.getChildren(ROOT, null);
		TreeSet<String> sortedSetNodes = new TreeSet<String>();
		for (String node : nodes) {
			sortedSetNodes.add(ROOT + "/" + node);
		}
		return sortedSetNodes;
	}

	public static String getLocalIpAddress() throws SocketException {  
        // 获得本机的所有网络接口  
        Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();  
  
        while (nifs.hasMoreElements()) {  
            NetworkInterface nif = nifs.nextElement();  
  
            // 获得与该网络接口绑定的 IP 地址，一般只有一个  
            Enumeration<InetAddress> addresses = nif.getInetAddresses();  
            while (addresses.hasMoreElements()) {  
                InetAddress addr = addresses.nextElement();  
  
                String ip = addr.getHostAddress();  
                // 只关心 IPv4 地址  
                if (addr instanceof Inet4Address && !"127.0.0.1".equals(ip)) {  
                    return ip;  
                }  
            }  
        }  
        return null;  
    }  
  
    public static byte[] getLocalIpAdressBytes() throws SocketException {  
        String ip = getLocalIpAddress();  
        return ip == null ? null : ip.getBytes();  
    }  

}
