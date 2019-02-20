package com.wangheng.hadoop.rpc.controller;

import com.wangheng.hadoop.rpc.service.LoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

public class LoginController {

    public static void main(String[] args) throws Exception{
        LoginService proxy = RPC.getProxy(LoginService.class, 1L,
                new InetSocketAddress("localhost", 10000),
                new Configuration());
        String result = proxy.login("dfsfsdfsdfsd", "123456");
        System.out.println(result);
    }
}
