package utils;

import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.ArrayList;
import java.util.List;

public class NodeToHostList {
    public static List<Host> convert (List<Node> nodeList){
        List<Host> hostList = new ArrayList<>();
        nodeList.forEach(node -> hostList.add(node.getHost()));
        return hostList;
    }
}
