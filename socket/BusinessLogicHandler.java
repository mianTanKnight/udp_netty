package com.lishi.cloud.miteno.core.broadcast.socket;

import com.lishi.cloud.miteno.core.broadcast.enums.CmdParaEnum;
import com.lishi.cloud.miteno.core.broadcast.service.BroadcastService;
import com.lishi.cloud.miteno.core.broadcast.utils.ByteUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * BusinessLogicHandler 的职责是专注于业务上的处理
 *
 * @author ztq
 */
@Slf4j
public class BusinessLogicHandler extends SimpleChannelInboundHandler<BroadcastUDPMesHandler.UDPOfBroadCastMes> {

    private final BroadcastService broadcastService;
    // proxy
    private final ExecutorService proxyBussinessExcutors = Executors.newSingleThreadExecutor();

    public BusinessLogicHandler(BroadcastService broadcastService) {
        this.broadcastService = broadcastService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, BroadcastUDPMesHandler.UDPOfBroadCastMes udpOfBroadCastMes) throws Exception {
        //把业务代理给单线程工作池 隔离Netty的loop 保证netty的稳定性
        proxyBussinessExcutors.execute(() -> {
            try {
                CmdParaEnum cmdParaEnum = CmdParaEnum.getByParam(ByteUtil.hexString2Byte(udpOfBroadCastMes.getPara()));
                log.info("businessLogicHandler cmdParaEnum :[{}]", cmdParaEnum.getValue());
                cmdParaEnum.handle(broadcastService, udpOfBroadCastMes);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                udpOfBroadCastMes.disHandler(); // release obj
            }
        });
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        gracefulShutdown(this.proxyBussinessExcutors, 10, TimeUnit.SECONDS);
    }

    /**
     * 关闭代理池
     */
    public void gracefulShutdown(ExecutorService pool, long timeout, TimeUnit timeUnit) {
        // 禁止提交新任务
        pool.shutdown();
        try {
            // 循环等待关闭
            long pendingTerminationTime = timeUnit.toMillis(timeout);
            long waitTime = 1000;
            while (!pool.awaitTermination(waitTime, TimeUnit.MILLISECONDS) && pendingTerminationTime > 0) {
                pendingTerminationTime -= waitTime;
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        }
        if (!pool.isTerminated()) {
            // 如果还没完全关闭，强制关闭
            pool.shutdownNow();
        }
    }

}
