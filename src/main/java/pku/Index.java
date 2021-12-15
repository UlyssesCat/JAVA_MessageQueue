package pku;

import java.util.ArrayList;

public class Index{

    //索引队列 一个消息队列对应一个索引Index
    //private List<IndexObject> indexList = new LinkedList<>();
    private IndexObject[] indexList2 = new IndexObject[600];//参数改成10 本地跑不通 ，但是评测玄学高分 建议用上面的链表
    private int IndexNumber = 0;

    //为Index队列添加一个IndexObject
    public void addIndexObject(int blockNumber,int startMessageIndex,int msgNum){
        //this.indexList.add(new IndexObject(blockNumber,startMessageIndex,msgNum));
        indexList2[IndexNumber++]=new IndexObject(blockNumber,startMessageIndex,msgNum);

    }

//leetcode 第一个错误版本
    private int binarySearch(long firstIndex){
        int left = 0, right = IndexNumber;
        while (left < right) { // 循环直至区间左右端点相同
            int mid = left + (right - left) / 2; // 防止计算时溢出
            if (firstIndex<indexList2[mid].blockFirstIndex) {
                right = mid; // 答案在区间 [left, mid] 中
            } else {
                left = mid + 1; // 答案在区间 [mid+1, right] 中
            }
        }
        // 此时有 left == right，区间缩为一个点，即为答案
        return left-1;



//        //二分查找
//        int out = 0;
//        int left = 0;
//        int right = IndexNumber - 1;
//        while (left <= right) {
//            int mid = (left + right) / 2;
//            if (indexList2[mid].blockFirstIndex == firstIndex) {
//                out = mid;
//                break;
//            } else if (indexList2[mid].blockFirstIndex <= firstIndex) {
//                left = mid + 1;
//            } else {
//                right = mid - 1;
//            }
//        }
//        out = right;
//
//
//        while(totalIndex>0&&out<IndexNumber){
//            IndexObject indexIterator = indexList2[out];
//            results.add(indexIterator);
//            totalIndex -= (indexIterator.blockThisIndex - (firstIndex - indexIterator.blockFirstIndex));
//            out++;
//        }
//        return results;
    }

    //TODO 取出所需的IndexObejct队列
    /**
     * offset 索引号（x） 消息的偏移量 （byte）
     * num 读取消息个数  （x） 消息长度的偏移量
     **/
    public ArrayList<IndexObject> getIndexs(long firstIndex, long totalIndex) {
//        ArrayList<IndexObject> results = new ArrayList<>();
//        boolean hasFind = false;
//        for (IndexObject indexObject : this.indexList) {
//            if (indexObject.blockFirstIndex <= firstIndex && indexObject.blockFirstIndex + indexObject.blockThisIndex > firstIndex) {
//                results.add(indexObject);
//                totalIndex -= (indexObject.blockThisIndex - (firstIndex - indexObject.blockFirstIndex));
//                hasFind = true;
//                continue;
//            }
//            if (hasFind && totalIndex > 0) {
//                results.add(indexObject);
//                totalIndex -= indexObject.blockThisIndex;
//                continue;
//            }
//            if (totalIndex <= 0)
//                return results;
//        }
//        return results;

        ArrayList<IndexObject> results = new ArrayList<>();
        boolean hasFind = false;
        int start = binarySearch(firstIndex);


        for (int i =start ; i < IndexNumber ; ++i) {
            IndexObject indexObject = indexList2[i];
            if (indexObject.blockFirstIndex <= firstIndex && indexObject.blockFirstIndex + indexObject.blockThisIndex > firstIndex) {
                results.add(indexObject);
                totalIndex -= (indexObject.blockThisIndex - (firstIndex - indexObject.blockFirstIndex));
                hasFind = true;
                continue;
            }
            if (hasFind && totalIndex > 0) {
                results.add(indexObject);
                totalIndex -= indexObject.blockThisIndex;
                continue;
            }
            if (totalIndex <= 0)
                return results;
        }
        return results;




    }


    //Index队列的内部类 Index对象
    class IndexObject{
        /**
         *
         * @param blockNumber 刷盘次数or块号
         * @param blockFirstIndex 该盘块第一条消息的索引号
         * @param blockThisIndex 消息在该盘块的索引号
         * 如果一个盘快存放10索引，37号就是(3*1024 30 7)
         */
        IndexObject(int blockNumber,int blockFirstIndex,int blockThisIndex){
            this.blockNumber=blockNumber;
            this.blockFirstIndex=blockFirstIndex;
            this.blockThisIndex=blockThisIndex;
        }


        public long blockNumber;
        public int blockFirstIndex;
        public int blockThisIndex;

    }
}
