import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.*;

public class OrderedConsumer {

    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();
    public static void main(String[] args) throws MQClientException {


        DefaultMQPullConsumer consumer=new DefaultMQPullConsumer();
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setInstanceName("Consumer");
        consumer.setConsumerGroup("please_rename_unique_group_name");
        consumer.start();
        List<MessageExt> result = new ArrayList<MessageExt>();//最终存放全局有序的消息
    try{
        Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues("TopicTest");
        
        int queueNum = messageQueues.size();
        for (int i = 0; i < queueNum; i++){
            List<MessageExt> res = new ArrayList<MessageExt>();
        }
        for(MessageQueue messageQueue:messageQueues){
            int index = 0;
            PullResult pullResult = consumer.pullBlockIfNotFound(messageQueue,null,getMessageQueueOffset(messageQueue), 32);
            System.out.println(messageQueue.getTopic());
            System.out.println(messageQueue.getQueueId());
            List<MessageExt> res = pullResult.getMsgFoundList();
            //根据bornTimestamp判断消息的创建时间，时间戳小的被放入最终的结果列表中，接着对比下一个元素。
            if (res.get(index).getBornTimestamp() < res.get(index+1).getBornTimestamp()){
                result.add(res.get(index));
                index++;
            }else{
                result.add(res.get(index+1));
                index++;
            }


        }
        
    } catch (Exception e){
        e.printStackTrace();

    }


    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }

}