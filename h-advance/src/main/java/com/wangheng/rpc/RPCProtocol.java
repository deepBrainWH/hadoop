package com.wangheng.rpc;

/**
 * @author wangheng
 * @date 2021/12/30
 */
public interface RPCProtocol {
  long versionID = 1L;

  void makeDirs(String path);
}
