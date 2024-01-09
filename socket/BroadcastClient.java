package com.lishi.cloud.miteno.core.broadcast.socket;

import com.lishi.cloud.miteno.core.broadcast.config.BroadcastPropertiesConfig;
import com.lishi.cloud.miteno.core.broadcast.enums.CmdEnum;
import com.lishi.cloud.miteno.core.broadcast.service.BroadcastService;
import com.lishi.cloud.miteno.core.broadcast.utils.ByteUtil;
import com.lishi.cloud.miteno.core.commons.condition.ConditionOnProfile;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;


/**
 * @author zwx & ztq
 * 提供更细致锁粒度的控制的实现和更高并发性能
 * BroadcastReadWriteLockClient 提供了对UDP通信中单桢和多桢数据的高效处理。
 * 利用读写锁（ReadWriteLock）来优化并发处理，提升性能和数据一致性。
 * 在处理单桢数据时（更频繁的操作），使用读锁允许并发读取，而在处理多桢数据时（较少发生），使用写锁确保数据的完整性和一致性。
 * 根据实际生产环境中单多桢比例（如 50:1）的设计，旨在提高处理单桢数据时的并发性能，同时保证多桢数据处理的原子性和数据安全。
 * <p>
 * 详情参考设计文档: 'BroadCastDesignInfo.md'
 * </p>
 */
@Slf4j
@Component
@ConditionOnProfile("prod")
public class BroadcastClient implements LockClient {

    private final BroadcastPropertiesConfig broadcastConfig;
    private final BroadcastService broadcastService;
    private Channel channel;
    private EventLoopGroup group;
    /**
     * Command 原子性保证
     */
    private volatile boolean multiFrameMesProcessing = false; // multiFrameMesProcessing 不依赖锁保证线程可见性问题 使用volatile并支持提前检验
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Map<Thread, Integer> getLockThreadMap = new ConcurrentHashMap<>();


    /**
     * UPD在多帧通信在最大限定时间没有有效完成的保证回调
     * BroadCastClient 的超时机制的实现是"惰性实现"
     * 被阻塞的线程在必要的时候检查 而不是周期检查,在竞争不算激烈的情况是高效的
     */
    private Supplier<Void> timeOutCallBack;
    private Long timeOutListenObj = null;


    @Autowired
    public BroadcastClient(BroadcastPropertiesConfig broadcastConfig, BroadcastService broadcastService) {
        this.broadcastConfig = broadcastConfig;
        this.broadcastService = broadcastService;
    }


    @PostConstruct
    public void init() throws Exception {
        log.info("Check Config :[{}] ", broadcastConfig.toString());
        connect();
        //先sendClose 清除缓存 再init
//        closeCommand();
        initCommand();
        //主动重置所有的BroadCast状态
        broadcastService.syncBroadcastDevice();
        log.info("broadCast communication establish successful Of Upd Socket !");
    }


    private void connect() {
        final BroadcastUDPMesHandler broadcastClientHandler = new BroadcastUDPMesHandler(this);
        timeOutCallBack = broadcastClientHandler::giveUpOldMultiFrameOfTimeOut;
        final BusinessLogicHandler businessLogicHandler = new BusinessLogicHandler(broadcastService);
        Bootstrap b = new Bootstrap();
        group = new NioEventLoopGroup();
        try {
            b.group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, true)
                    .handler(new ChannelInitializer<DatagramChannel>() {
                        @Override
                        protected void initChannel(DatagramChannel ch) {
                            ch.pipeline().addLast(broadcastClientHandler);
                            ch.pipeline().addLast(businessLogicHandler);
                        }
                    });

            channel = b.bind(8000).sync().channel();
            channel.connect(new InetSocketAddress(broadcastConfig.getServerHost(), broadcastConfig.getServerPort())).sync();
        } catch (InterruptedException e) {
            log.error("Connect error please Check adder  [{} : {}]", broadcastConfig.getServerHost(), broadcastConfig.getServerHost());
        }
    }

    /**
     * 细粒度并发控制
     * 如果 reentry 是ture 则证明是netty loop正在处理多帧cmd(多次命令发送)
     * 如果是单帧cmd 那么就无需控制并发(但需要持有Read锁 为了控制write锁的安全获取)
     * 如果是多帧cmd 那么我们需要保证此cmd整体的原子性 不可被破坏,结束条件有二 : 完整的通信结束 和 通信超时(由申请锁执行惰性检查)
     *
     * @param data    byte[]
     * @param cmdEnum cmd
     * @param reentry 是否来至netty loop线程的重试请求
     */
    @Override
    public void sendCommandOfLock(byte[] data, CmdEnum cmdEnum, boolean reentry) {
        //support reentry
        if (reentry) {
            multiFrameMesProcessing = true;
            sendCommand(data, cmdEnum, true, true);
            return;
        }
        int isMultiFrame = cmdEnum.getIsMultiFrame();
        // use read lock
        if (isMultiFrame == 0 && !multiFrameMesProcessing) {
            log.info("Get ReadLock Successful  Thread Name : [{}]", Thread.currentThread().getName());
            readWriteLock.readLock().lock();
            try {
                sendCommand(data, cmdEnum, false, false);
            } finally {
                readWriteLock.readLock().unlock();
                log.info("Release ReadLock Successful  Thread Name : [{}]", Thread.currentThread().getName());
            }
            return;
        }
        /*牺牲主动唤醒功能
        使用短时间轮询  + volatile的isProcessing 解决突然高并发时间 多个线程wait在lock()上 只有在接到可用通知时 才会去获得commandLock
        不需要wait 直接开始尝试最大等待
         */
        int interval = 50_000_000;
        // 1 秒转换为纳秒
        long totalDuration = 1_000_000_000L;
        long startTime = System.nanoTime();
        while (multiFrameMesProcessing) {
            log.info("[{}],Get WriteCommandLock start park", Thread.currentThread().getName());
            LockSupport.parkNanos(interval);
            if (waitMax(Thread.currentThread())) {
                getLockThreadMap.remove(Thread.currentThread());
                //中断业务链
                log.error(Thread.currentThread().getName() + "Wait Max Count,give up : " + cmdEnum.getCmd());
                return;
            }
            if (System.nanoTime() - startTime >= totalDuration) {
                log.info("[{}] Wait Thread notify Of TimeOut", Thread.currentThread().getName());
                getLockThreadMap.compute(Thread.currentThread(), (k, v) -> null == v ? 1 : v + 1);
                //如果线程被阻塞是因为超时唤醒 那么检查正在进行中 command 的超时状态
                checkTimeOut();
                startTime = System.nanoTime();
            }
        }
        readWriteLock.writeLock().lock();
        try {
            log.info("[{}],Get WriteLock successful!", Thread.currentThread().getName());
            multiFrameMesProcessing = true;
            sendCommand(data, cmdEnum, false, true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            readWriteLock.writeLock().unlock();
            getLockThreadMap.remove(Thread.currentThread());
        }
    }

    /**
     * 受保护的方法
     * 多桢命令受锁保护 在命令执行周期中只会存在一个(并且排它 读写锁都会阻塞)
     */
    public void sendCommand(byte[] data, CmdEnum cmdEnum, boolean reentry, boolean isMultiFrame) {
        //如果是多帧命令 并且不是内部重试 注册超时监听
        if (isMultiFrame && !reentry) {
            registerTimeOutListen();
        }
        ByteBuf buf = Unpooled.copiedBuffer(data);
        log.info("sendCommand:[{}], params:[{}]", cmdEnum.name(), ByteUtil.bytesToHexString(data, false));
        try {
            ChannelFuture future = channel.writeAndFlush(new DatagramPacket(buf, new InetSocketAddress(broadcastConfig.getServerHost(), broadcastConfig.getServerPort()))).sync();
            future.awaitUninterruptibly();
        } catch (InterruptedException e) {
            if (isMultiFrame) {
                releaseCommandLock(); //释放可能的锁
            }
            log.error("sendCommand error e:[{}], please Check Cmd :[{}]", e, cmdEnum.getCmd());
        }
    }

    private void registerTimeOutListen() {
        this.timeOutListenObj = System.currentTimeMillis();
    }


    /**
     * 惰性检查 由未获取到写锁的多帧请求(自旋中)检查
     */
    public void checkTimeOut() {
        if (null == timeOutListenObj) {
            return;
        }
        // timeOut
        if (System.currentTimeMillis() - timeOutListenObj >= broadcastConfig.getTimeoutMax()) {
            log.warn("Check Have timed out notify Thread name :[{}]", Thread.currentThread().getName());
            timeOutCallBack.get(); // execute callback
            releaseCommandLock(); // unlock commandLock
        }
    }

    /**
     * loop 线程完整执行整个Command后调用
     * readWriteLock.writeLock()是可重入锁
     * 不会发生 内部执行完之后申请释放会被wait的情况(锁被另外一个线程持有)
     * 因为 writeLock 会保证完全排他性(执行必然会拥有锁)
     */
    @Override
    public void releaseCommandLock() {
        readWriteLock.writeLock().lock();
        try {
            multiFrameMesProcessing = false;
            timeOutListenObj = null;
        } finally {
            readWriteLock.writeLock().unlock();
        }
        log.info("[{}],ReleaseCommandLock successful", Thread.currentThread().getName());
    }

    /**
     * 兜底保证 预防极端情况 保证安全性
     */
    public boolean waitMax(Thread thread) {
        Integer count;
        if ((count = getLockThreadMap.get(thread)) == null) {
            return false;
        }
        return count >= broadcastConfig.getThreadTryLockMax();
    }

    /**
     * Spring容器销毁前关闭广播中间件
     */
    @PreDestroy
    public void close() {
//        closeCommand();
        group.shutdownGracefully();
    }

}
