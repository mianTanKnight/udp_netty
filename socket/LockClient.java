package com.lishi.cloud.miteno.core.broadcast.socket;

import cn.hutool.core.util.HexUtil;
import com.lishi.cloud.miteno.core.broadcast.constans.Constants;
import com.lishi.cloud.miteno.core.broadcast.enums.CmdEnum;
import com.lishi.cloud.miteno.core.broadcast.utils.ByteUtil;

/**
 * @author ztq
 */
public interface LockClient {

    /**
     * 释放命令锁
     */
    void releaseCommandLock();

    /**
     * 安全的发送命令
     */
    void sendCommandOfLock(byte[] data, CmdEnum cmdEnum, boolean reentry);

    /**
     * 初始化命令
     */
    default void initCommand() {
        byte[] header = ByteUtil.buildHeader(CmdEnum.INIT.getCmd(), 52, Constants.EMPTY_DID, Constants.NON_PARAM);
        byte[] param = HexUtil.decodeHex(Constants.INIT_PARAM);
        sendCommandOfLock(ByteUtil.appendHeaderAndParam(header, param), CmdEnum.INIT, false);
    }

    /**
     * close命令
     */
    default void closeCommand() {
        byte[] headerBytes = ByteUtil.buildHeader(CmdEnum.CLOSE.getCmd(), 0, Constants.DID, Constants.NON_PARAM);
        //跳过锁
        sendCommandOfLock(headerBytes, CmdEnum.CLOSE,false);
    }

}
