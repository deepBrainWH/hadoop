package com.wangheng.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author wangheng
 * @date 2021/12/30
 * @desc
 */
public class NNServer implements RPCProtocol {

  public static void main(String[] args) throws IOException {
    //启动服务
    RPC.Server server = new RPC.Builder(new Configuration())
        .setBindAddress("localhost")
        .setPort(8888)
        .setProtocol(RPCProtocol.class)
        .setInstance(new NNServer())
        .build();
    System.out.println("服务器启动成功");
    server.start();
  }

  @Override
  public void makeDirs(String path) {
    System.out.println("服务端接收到了客户端请求" + path);
  }
}
