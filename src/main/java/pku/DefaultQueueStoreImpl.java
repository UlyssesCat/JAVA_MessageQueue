package pku;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {

    //空队列
    public static Collection<byte[]> EMPTY = new ArrayList<>();
    //队列名 映射 队列
    private ConcurrentHashMap<String, MessageQueue> queueMap;
    //配置根目录
    public static final String ROOT_DIR = "data/";

    private FileChannel[] channelList;
    private AtomicInteger[] writePointerList;

    //文件个数
    private static final int FILES_TOTAL_NUMBER = 1<<7;



    public DefaultQueueStoreImpl() throws IOException {
        queueMap =  new ConcurrentHashMap<>();
        channelList = new FileChannel[FILES_TOTAL_NUMBER];
        writePointerList = new AtomicInteger[FILES_TOTAL_NUMBER];

        RandomAccessFile raf;
        for (int i = 0; i < FILES_TOTAL_NUMBER; i++) {
            raf = new RandomAccessFile(ROOT_DIR + i+".data" , "rw");
            this.channelList[i] = raf.getChannel();
            this.writePointerList[i] = new AtomicInteger(0);
        }

    }



    public void put(String queueName, byte[] message) {
        if (!queueMap.containsKey(queueName)){
            int file_number = Integer.valueOf(queueName.split("-")[1])&(FILES_TOTAL_NUMBER-1);
            queueMap.put(queueName,new MessageQueue(channelList[file_number],writePointerList[file_number]));
        }
        queueMap.get(queueName).putMessage(message);
    }
    public Collection<byte[]> get(String queueName, long firstIndex, long totalIndex) {
        return queueMap.get(queueName)==null?EMPTY:queueMap.get(queueName).getMessage(firstIndex,totalIndex);
    }
}
