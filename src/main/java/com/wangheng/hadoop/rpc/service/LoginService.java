package com.wangheng.hadoop.rpc.service;

public interface LoginService {
    public static final long versionID=1L;
    public String login(String username, String passwd);
}
