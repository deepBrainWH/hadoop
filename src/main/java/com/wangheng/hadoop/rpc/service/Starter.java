package com.wangheng.hadoop.rpc.service;

import com.wangheng.hadoop.rpc.service.imp.LoginServiceImp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

public class Starter {

    public static void main(String [] args) throws IOException {

        Builder builder = new Builder(new Configuration());
        builder.setBindAddress("localhost").setPort(10000)
                .setProtocol(LoginService.class)
                .setInstance(new LoginServiceImp());
        Server server = builder.build();
        server.start();
    }
}
