package com.wangheng.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author wangheng
 * @date 2021/12/30
 */
public class NNClient {
  public static void main(String[] args) throws IOException {
    RPCProtocol client = RPC.getProxy(RPCProtocol.class, 1L, new InetSocketAddress("localhost", 8888), new Configuration());
    client.makeDirs("/hello/rpc");
  }
}
