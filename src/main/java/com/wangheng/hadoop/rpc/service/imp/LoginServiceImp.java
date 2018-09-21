package com.wangheng.hadoop.rpc.service.imp;

import com.wangheng.hadoop.rpc.service.LoginService;

public class LoginServiceImp implements LoginService {
    @Override
    public String login(String username, String passwd) {

        return username+"loged in successfully!";
    }
}
