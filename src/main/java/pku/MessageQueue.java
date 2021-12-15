package pku;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


//一个队列对应一个MessageQueue对象
public class MessageQueue {
    public static final int BUFFER_SIZE = 4096;//TODO 块大小 待测试
    private ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

    private int writtenMessageNumber = 0;  // 写入磁盘的总量
    private int bufferMessageNumber = 0;   // buffer中剩余的消息数量

    private FileChannel channel;
    private AtomicInteger writePointer;//写指针or刷盘次数or写入块号
    private Index index;

    public MessageQueue(FileChannel channel, AtomicInteger writePointer){
        this.channel = channel;
        this.writePointer = writePointer;
        index = new Index();
    }


    public void putMessage(byte[] message) {
        //如果剩余空间不够写
        if (message.length + 2 > buffer.remaining()) {//2 ： short字节数
            flushDisk();
            buffer.clear();
        }
        buffer.putShort((short) message.length);
        buffer.put(message);
        writtenMessageNumber++;
        bufferMessageNumber++;
    }

    //刷盘
    private void flushDisk() {
        buffer.position(0);
        int blockNumber = writePointer.getAndIncrement();
        index.addIndexObject(blockNumber, writtenMessageNumber-bufferMessageNumber, bufferMessageNumber);
        try {
            channel.write(buffer,blockNumber * BUFFER_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.clear();
        bufferMessageNumber = 0;
    }


    private volatile boolean isFirstGet = true;
    public Collection<byte[]> getMessage(long firstIndex,long totalIndex) {
        //将当前队列缓存写道磁盘
        if (isFirstGet) {
            flushDisk();
            isFirstGet = false;
        }
        List<byte[]> result = new ArrayList<>();
        ArrayList<Index.IndexObject> allWantedIndexs = index.getIndexs(firstIndex, totalIndex);
        if (allWantedIndexs == null || allWantedIndexs.size() == 0)
            return DefaultQueueStoreImpl.EMPTY;
        int indexRemained = (int)totalIndex;//剩余index计数器
        boolean isFirst = true;
        try {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            for (Index.IndexObject indexIter : allWantedIndexs) {
                buffer.clear();
                channel.read(buffer,indexIter.blockNumber * BUFFER_SIZE);
                buffer.position(0);
                int startIndex = 0;
                if (isFirst) {
                    startIndex = (int) firstIndex - indexIter.blockFirstIndex;
                    isFirst = false;
                }
                int indexUsed = indexRemained < indexIter.blockThisIndex - startIndex ? indexRemained : indexIter.blockThisIndex - startIndex;
                for (int i = 0; i < startIndex; i++) {
                    short msgLen = buffer.getShort();
                    int oldPos = buffer.position();
                    buffer.position(oldPos + msgLen);
                }
                for (int i = 0; i < indexUsed; i++) {
                    short msgLen = buffer.getShort();
                    byte[] message = new byte[msgLen];
                    buffer.get(message);
                    result.add(message);
                }
                indexRemained -= indexUsed;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
