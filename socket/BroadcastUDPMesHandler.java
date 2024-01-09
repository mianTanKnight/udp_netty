package com.lishi.cloud.miteno.core.broadcast.socket;

import cn.hutool.core.collection.CollectionUtil;
import com.lishi.cloud.miteno.core.broadcast.constans.Constants;
import com.lishi.cloud.miteno.core.broadcast.enums.CmdEnum;
import com.lishi.cloud.miteno.core.broadcast.utils.ByteUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;

import java.util.*;

/**
 * BroadcastUDPMesHandler 的职责是专注UPD通信的保障
 * 此handler 为单线程环境 netty 保证会被同一个loop 线程调用
 * 由于UDP 不可靠通信特性  信息的无效性和完整性需要client端保证
 * 详细的 UDP 协议规则,数据帧结构,设计保证 请参阅 'BroadCastDesignInfo.md' 设计详情文件，
 *
 * @author ztq
 */
@Slf4j
public class BroadcastUDPMesHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private final LockClient broadcastLockClient;

    /**
     * BroadCast UPD 通信协议的起始符
     * 这里使用 int申明 byte的范围是 -128 - 127 OxFE会超范围。
     */
    private static final int[] BROAD_CAST_MESSAGE_START_BYTES = new int[]{0xFE, 0xE0, 0xA7};

    /**
     * BroadCast 通信协议偏移量
     * 高16位储存offset 低16位置储存 len
     */
    private static final int OFFSET_AND_LENGTH_OF_TYPE = (3 << 16) | 1;
    private static final int OFFSET_AND_LENGTH_OF_DID = (4 << 16) | 4;
    private static final int OFFSET_AND_LENGTH_OF_SN = (8 << 16) | 2;
    private static final int OFFSET_AND_LENGTH_OF_LEN = (10 << 16) | 2;
    private static final int OFFSET_AND_LENGTH_OF_CMD = (12 << 16) | 1;
    private static final int OFFSET_AND_LENGTH_OF_PARA = (13 << 16) | 1;
    private static final int OFFSET_AND_LENGTH_OF_ACCEPT = (14 << 16) | 1;
    private static final int OFFSET_AND_LENGTH_OF_CHECKSUM = (15 << 16) | 1;
    private static final int OFFSET_END_BIT_INDEX = 15;


    public BroadcastUDPMesHandler(LockClient broadcastLockClient) {
        this.broadcastLockClient = broadcastLockClient;
    }

    /**
     * 处理Upd 多桢问题
     * 一个答应信息过多 会使用多帧传输
     * 但UPD并不保证顺序答应 使用 sn 处理
     * BroadcastClientHandler 本质上是由单一线程处理 Netty保证
     * See {@link com.lishi.cloud.miteno.core.broadcast.socket.BroadcastClient#sendCommandOfLock(byte[] data, CmdEnum cmdEnum, boolean reentry)}
     * 保证整个 BroadcastClientHandler 处理 Command 过程中的的原子性不会被破坏 那么 multiFrameMes就不会出现并发问题
     */
    public UDPOfBroadCastMes multiFrameMes;

    /**
     * 由于UDP本身的不可靠性，我们采用以下策略确保数据传输的完整性和顺序：
     * 1. 使用TreeMap来维护多帧数据的顺序和完整性。
     * 2. 对于未接收到的头桢和尾桢，我们使用超时机制和完整性检查来处理。
     * 3. 在事件循环线程中，我们不直接抛出异常，而是记录错误信息，避免影响事件处理的稳定性。
     * 注意 CommandLock 的释放很重要
     * 更多UDP协议规则和数据帧结构的详细信息，请参阅 'BroadCastDesignInfo.md'。
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket datagramPacket) {
        log.info("channelReadO receive data begin handle");
        ByteBuf content = datagramPacket.content();
        UDPOfBroadCastMes udpOfBroadCastMes = new UDPOfBroadCastMes(content);
        if (!udpOfBroadCastMes.getIsValid()) {
            if (ByteUtil.notOnLienError(udpOfBroadCastMes.getAcceptValue())) { //如果是未上线错误 尝试通知上线
                broadcastLockClient.initCommand();
            }
            log.error("Valid not pass error message :[{}]", udpOfBroadCastMes.getErrorStr());
            // Valid not pass error message
            releaseLockAndClear(); //释放可能的锁
            return;
        }
        if (isMultiFrame(udpOfBroadCastMes)) {
            handleMultiFrame(ctx, udpOfBroadCastMes);
            return;
        }
        // Handle single frame
        push2NextHandler(ctx, udpOfBroadCastMes, false);
        log.info("channelReadO receive data end !");
    }

    private boolean isMultiFrame(UDPOfBroadCastMes udpOfBroadCastMes) {
        return CmdEnum.SEND_DATA.getCmd() == udpOfBroadCastMes.getCmd();
    }

    private void handleMultiFrame(ChannelHandlerContext ctx, UDPOfBroadCastMes udpOfBroadCastMes) {
        if (multiFrameMes == null) {
            log.info("MultiFrame Request Handle Start..");
        }
        boolean isNewFrameChain = false;
        //if old MultiFrame discard this
        int multiFrameSn;
        // 如果 multiFrameMes 是null 初始化,但在这个if中 multiFrameMes不为null时会触发 另外2个检查 1:检查是否出现重复桢 2: 如果出现了重复桢 检查是否为头桢 如果是丢掉旧的
        if (Objects.isNull(multiFrameMes) ||
                ((multiFrameSn = multiFrameMes.isExistsSpecifyFrameNum(udpOfBroadCastMes.getSn())) == 0 && udpOfBroadCastMes.getSn() == 0)) {
            multiFrameMes = udpOfBroadCastMes;
            isNewFrameChain = true;
        } else {
            if (multiFrameSn > 0) {
                log.warn("MultiFrame Duplicate Frame :[{}]", multiFrameSn);
            }
            multiFrameMes.pushContent(udpOfBroadCastMes);
        }
        if (existsEndBit(udpOfBroadCastMes)) {
            finalizeMultiFrame(ctx, udpOfBroadCastMes, isNewFrameChain);
            return;
        }
        sendNextData();
    }

    private void finalizeMultiFrame(ChannelHandlerContext ctx, UDPOfBroadCastMes udpOfBroadCastMes, boolean isNewFrameChain) {
        // 判断是第一帧也是最后一桢 但最后一桢的sn号会破坏整体连续性
        if (!isNewFrameChain && !multiFrameMes.checkMultiFrameCompleteness()) {
            log.error("Incomplete MultiFrame data: [{}]", multiFrameMes.getContentTreeMap());
            releaseLockAndClear();
            return;
        }
        log.info("MultiFrame Request complete.");
        push2NextHandler(ctx, isNewFrameChain ? udpOfBroadCastMes : multiFrameMes, true);
    }

    //push to next handler of pipeline
    private void push2NextHandler(ChannelHandlerContext ctx, UDPOfBroadCastMes udpOfBroadCastMes, boolean isMultiFrameMes) {
        if (isMultiFrameMes) {
            releaseLockAndClear();
        }
        ctx.fireChannelRead(udpOfBroadCastMes);
    }

    private void releaseLockAndClear() {
        broadcastLockClient.releaseCommandLock();
        multiFrameMes = null;
    }

    /**
     * 如果在最长等待时间限制中 没有接受到有效完整的UPD信息
     * 那就 give Up
     * 这是个回调函数 在超过最大等待时间触发
     */
    public Void giveUpOldMultiFrameOfTimeOut() {
        if (null != multiFrameMes) {
            log.warn("Give Up Multi Frame Of Time Out ,give up this Data [" + multiFrameMes.cmd + ":" + multiFrameMes.para + "]");
            multiFrameMes.disHandler();
            multiFrameMes = null;
        }
        return null;
    }

    private boolean existsEndBit(UDPOfBroadCastMes udpOfBroadCastMes) {
        return ((udpOfBroadCastMes.getSn()) >> OFFSET_END_BIT_INDEX & 1) != 0;
    }


    private void sendNextData() {
        log.info("MultiFrame Send Request Next Data ");
        // send get next data
        byte[] header = ByteUtil.buildHeader(CmdEnum.SEND_NEXT_DATA.getCmd(), 0, Constants.DID, Constants.NON_PARAM);
        broadcastLockClient.sendCommandOfLock(header, CmdEnum.SEND_NEXT_DATA, true);
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Channel Exception Caught  Mess :{}", cause.getMessage());
        cause.printStackTrace();
        releaseLockAndClear(); // 必须要释放可能的锁
        // ctx.close(); 暂时不需要直接关闭 可能是业务导致的错误
    }

    /**
     * 封装UDP raed()消息
     * 此对象是规范协议的解析封装 解析成十六进制(string表示)
     * <p>
     * 此对象设计成充血模型
     */
    @Data
    @ToString
    public static class UDPOfBroadCastMes {

        private String type;
        /**
         * 设备ID，4字节，某些命令下可为0
         */
        private String did;

        /**
         * 帧序列号，4字节，用于跟踪帧的顺序
         */
        private Integer sn;

        /**
         * Cmd: 命令字，1字节，定义操作类型。
         */
        private Integer cmd;

        /**
         * Para: 参数，1字节，与命令字结合使用。
         */
        private String para;

        /**
         * content内容长度
         */
        private Integer len;

        /**
         * 解决UPD 基于sn 模式的不稳定
         * key -> sn
         * value -> content
         * <p>
         * maybe is an Empty Map of contentTreeMap
         */
        private TreeMap<Integer, String> contentTreeMap;

        /**
         * 此UDPOfBroadCastMes 是否可用
         */
        private Boolean isValid;

        /**
         * 错误日志 如果存在
         */
        private String errorStr;

        /**
         * 此处命令的接受状态
         */
        private Byte acceptValue;

        public UDPOfBroadCastMes(ByteBuf buf) {
            contentTreeMap = new TreeMap<>();
            if (!(isValid = parseData(buf))) {
                disHandler();
            }
        }

        private int isExistsSpecifyFrameNum(Integer frameNum) {
            return contentTreeMap.containsKey(frameNum) ? frameNum : -1;
        }

        /**
         * 最后一帧会破坏 keys 的连续性。
         * 此方法检查除最后一帧外的所有帧是否连续。
         *
         * @return 如果除最后一帧外的所有帧都连续，则返回 true；否则返回 false。
         */
        private boolean checkMultiFrameCompleteness() {
            if (contentTreeMap.size() <= 1) {
                return true;
            }
            Set<Integer> keys = new HashSet<>(contentTreeMap.keySet());
            keys.remove(contentTreeMap.lastKey());
            int previous = -1;
            for (Integer key : keys) {
                if (previous >= 0 && key != previous + 1) {
                    return false;
                }
                previous = key;
            }
            return true;
        }

        public void pushContent(UDPOfBroadCastMes udpOfBroadCastMes) {
            if (!udpOfBroadCastMes.isExistsContent()) {
                return;
            }
            //type需要更新 其余的桢头信息是一样
            this.type = udpOfBroadCastMes.type;
            this.contentTreeMap.putAll(udpOfBroadCastMes.contentTreeMap);
            udpOfBroadCastMes.disHandler();
        }

        public String getUpdWholeContent() {
            assert isValid;
            StringBuilder str = new StringBuilder();
            if (CollectionUtil.isEmpty(contentTreeMap)) {
                return Strings.EMPTY;
            }
            //contentTreeMap 保证了有序性
            contentTreeMap.forEach((x, y) -> str.append(y));
            return str.toString();
        }

        private boolean isExistsContent() {
            return isValid && len > 0;
        }

        public void disHandler() {
            isValid = false;
            contentTreeMap = null; //help GC
        }

        /**
         * 操作buff 根据规则和标位读取解析构建this
         *
         * @param buf buf
         * @return bool
         */
        private boolean parseData(ByteBuf buf) {
            //测试输出
            testPrint(buf);
            //寻桢头开始符 如果开始符不在就存在网络传输问题 无法处理
            if (!findMessageOfStartBytes(buf)) {
                this.errorStr = "UDPOfBroadCastMes no valid data was found.";
                return false;
            }
            try {
                /*
                 * accept,checksum,len 为验证字段
                 * 提供快速反应
                 */
                int acceptOffset = getOffset(OFFSET_AND_LENGTH_OF_ACCEPT);
                byte acceptValue = this.acceptValue = buf.getByte(acceptOffset);
                if (acceptValue != 0) {
                    this.errorStr = "Invalid accept value, discarding the data frame. Info: " + ByteUtil.parseAcceptByte(acceptValue);
                    return false;
                }
                if (!checkChecksum(buf)) {
                    this.errorStr = "Invalid checksum, discarding the data frame.";
                    return false;
                }
                // type, DID, SN, Cmd, Para 统一先解析成16进制字串
                String sn, cmd, len;
                this.type = readField(buf, OFFSET_AND_LENGTH_OF_TYPE);
                this.did = readField(buf, OFFSET_AND_LENGTH_OF_DID);
                sn = readField(buf, OFFSET_AND_LENGTH_OF_SN);
                cmd = readField(buf, OFFSET_AND_LENGTH_OF_CMD);
                this.para = readField(buf, OFFSET_AND_LENGTH_OF_PARA);
                len = readField(buf, OFFSET_AND_LENGTH_OF_LEN);
                this.sn = Integer.parseInt(sn, 16);
                this.cmd = Integer.parseInt(cmd, 16);
                // 根据 Len 字段处理数据内容
                int length = this.len = Integer.parseInt(len, 16);
                log.info("Analyze Buf Message typeStr:[{}], didStr:[{}], snStr:[{}], snInt:[{}], cmdStr:[{}], cmdInt:[{}], paraStr:[{}], lenStr:[{}], lenInt:[{}]",
                        type, did, sn, this.sn, cmd, this.cmd, para, len, this.len);
                if (length == 0) {
                    return true;
                }
                // 设置readerIndex到内容开始的位置 , readerIndex 是紧跟checkSum
                buf.readerIndex(getOffset(OFFSET_AND_LENGTH_OF_CHECKSUM) + 1);
                if (buf.readableBytes() < length) {
                    this.errorStr = "the data length does not match ,give up this Data [" + this.cmd + ":" + this.para + "]";
                    return false;
                }
                byte[] content = new byte[length];
                buf.readBytes(content);
                //
                String put = contentTreeMap.put(this.sn, ByteUtil.toHexStringLittleEndian(content));
                if (null != put) {
                    this.errorStr = "put content failed ,give up this Data [" + this.cmd + ":" + this.para + "]";
                    return false;
                }
                return true;
            } catch (Exception e) {
                e.printStackTrace(); //输出栈错误信息
                this.errorStr = "parsing data error : " + e.getMessage() + ",give up this Data [" + this.cmd + ":" + this.para + "]";
                return false;
            }
        }

        private void testPrint(ByteBuf buf) {
            int originalReaderIndex = buf.readerIndex();
            // 读取数据用于日志记录
            if (buf.isReadable()) {
                byte[] bytes = new byte[buf.readableBytes()];
                buf.readBytes(bytes);
                log.info("Received Data: {}", bytesToBinaryString(bytes)); //监控二进制
            }
            // 恢复原始的读取位置
            buf.readerIndex(originalReaderIndex);
        }

        private static String bytesToBinaryString(byte[] data) {
            log.info("bytesToBinaryString data len :[{}]", data.length);
            StringBuilder binaryStr = new StringBuilder();
            for (byte b : data) {
                String binaryByte = Integer.toBinaryString(b & 0xFF);
                String paddedBinaryByte = String.format("%8s", binaryByte).replace(' ', '0'); // 确保每个字节都是8位
                binaryStr.append(paddedBinaryByte).append(" "); // 添加空格以分隔字节
            }
            return binaryStr.toString();
        }

        private boolean checkChecksum(ByteBuf buf) {
            int offset = getOffset(OFFSET_AND_LENGTH_OF_CHECKSUM);
            int calculatedChkSum = 0;
            for (int i = 0; i < offset; i++) {
                calculatedChkSum += buf.getUnsignedByte(i) & 0xFF;
            }
            calculatedChkSum &= 0xFF; // 取低8位
            int chkSumValue = buf.getUnsignedByte(offset) & 0xFF;
            return calculatedChkSum == chkSumValue;
        }

        /**
         * readField of offsetAndLen
         */
        private String readField(ByteBuf buf, int offsetAndLength) {
            byte[] bytes = new byte[getLen(offsetAndLength)];
            buf.getBytes(getOffset(offsetAndLength), bytes);
            return ByteUtil.toHexStringLittleEndian(bytes);
        }

        private int getOffset(int offsetAndLen) {
            return (offsetAndLen >> 16) & 0xFFFF;
        }

        private int getLen(int offsetAndLen) {
            return offsetAndLen & 0xFFFF;
        }

        private boolean findMessageOfStartBytes(ByteBuf buf) {
            // 确保缓冲区至少有3个字节可读
            if (buf.readableBytes() >= 3) {
                // 直接检查前三个字节是否匹配
                return buf.getByte(buf.readerIndex()) == (byte) BROAD_CAST_MESSAGE_START_BYTES[0] &&
                        buf.getByte(buf.readerIndex() + 1) == (byte) BROAD_CAST_MESSAGE_START_BYTES[1] &&
                        buf.getByte(buf.readerIndex() + 2) == (byte) BROAD_CAST_MESSAGE_START_BYTES[2];
            }
            return false; // 如果字节不足，返回 false
        }
    }
}
