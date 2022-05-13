package edu.virginia.sqsjson;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;

public class TranslateWorker implements Runnable {

	List<Message> incomingMessages;
	JsonToXMLConverter converter;
	private final SQSQueueDriver readerThread;
    private final static Logger logger = Logger.getLogger(TranslateWorker.class);
    private final BlockingQueue<Message> readQ;
    private int threadCount;
    private boolean doneWorking = false;
    private boolean interrupted = false;

    public TranslateWorker(SQSQueueDriver readerThread, BlockingQueue<Message> readQ, int threadCount)
    {
        this.readQ = readQ;
        this.readerThread = readerThread;
        this.threadCount = threadCount;
        this.doneWorking = false;
    }

    public boolean isDoneWorking()
    {
        return doneWorking;
    }

    public void setInterrupted()
    {
        interrupted = true;
    }

    public boolean isInterrupted()
    {
        return(interrupted);
    }

	@Override
	public void run() 
	{
        Thread.currentThread().setName("TranslateWorker-Thread-"+threadCount);
        List<SendMessageBatchRequestEntry> messageBatchReq = new ArrayList<SendMessageBatchRequestEntry>(10);
        String messageSizes[] = new String[10];
        String messageBatchHandles[] = new String[10];
        converter = new JsonToXMLConverter();
        int numInBatch = 0;
        int messageBatchSize = 0;
        SendMessageBatchRequestEntry messageReq;

        while ((! readerThread.isDoneReading() || !readQ.isEmpty()) && !readerThread.isInterrupted() && !isInterrupted() )
        {
            try
            {
                Message message = readQ.poll(2000, TimeUnit.MILLISECONDS);

	            if (message != null)
	            {
	            	String id = message.getMessageAttributes().get("id").getStringValue(); 
		            final String messageReceiptHandle = message.getReceiptHandle();
		            XMLNode xmltree = converter.parseInput(message);
		            StringWriter sw = new StringWriter();
		            PrintWriter out = new PrintWriter(sw);
		            xmltree.traverse(out);
		            String xmlMessageBody = sw.toString();
	                // The attributes here must be the same (in size at least) as those added below note id is include twice since it is used as an attribute and as the batch id
	                int curMessageSize = getTotalMessageSize(xmlMessageBody, id, "id", id, "type", "application/xml");
	                if (alreadyContainsId(messageBatchReq, id))
	                {
	                    logger.info("Message batch already contains id: " + id + " flushing batch");
	                    sendMessageBatch(numInBatch, messageBatchReq, messageSizes, messageBatchHandles, messageBatchSize);
	                    messageBatchReq = new ArrayList<SendMessageBatchRequestEntry>(10);
	                    messageSizes = new String[10];
	                    messageBatchHandles = new String[10];
	                    numInBatch = 0;
	                    messageBatchSize = 0;
	                }
	                else if (numInBatch > 0 && messageBatchSize + curMessageSize >= AwsSqsSingleton.SQS_SIZE_LIMIT)
	                {
	                    logger.info("Message batch would be too large, only sending " + (numInBatch) + " messages in batch");
	                    sendMessageBatch(numInBatch, messageBatchReq, messageSizes, messageBatchHandles, messageBatchSize);
	                    messageBatchReq = new ArrayList<SendMessageBatchRequestEntry>(10);
	                    messageSizes = new String[10];
	                    messageBatchHandles = new String[10];
	                    numInBatch = 0;
	                    messageBatchSize = 0;
	                }
	                messageSizes[numInBatch] = id + " : " + curMessageSize;
	                messageBatchHandles[numInBatch] = messageReceiptHandle;
	                messageReq = new SendMessageBatchRequestEntry(readerThread.getOutputQueueUrl(), xmlMessageBody).withId(id)
	                            .addMessageAttributesEntry("id", new MessageAttributeValue().withDataType("String").withStringValue(id))
	                            .addMessageAttributesEntry("type", new MessageAttributeValue().withDataType("String").withStringValue("application/xml"));
	                messageBatchReq.add(messageReq);
	                numInBatch++;
	                messageBatchSize += curMessageSize;
	                if (numInBatch == 10)
	                {
	                	logger.debug("Sending batch of " + (numInBatch) + " messages that are a total of "+messageBatchSize+" bytes in size");
	                	sendMessageBatch(numInBatch, messageBatchReq, messageSizes, messageBatchHandles, messageBatchSize);
	                    messageBatchReq = new ArrayList<SendMessageBatchRequestEntry>(10);
	                    messageSizes = new String[10];
	                    messageBatchHandles = new String[10];
	                    numInBatch = 0;
	                    messageBatchSize = 0;
	                }
	            }
	            else
	            {
	            	if (numInBatch > 0)
	            	{
	                	logger.info("Input queue empty, flushing remaining " + (numInBatch) + " messages that are a total of "+messageBatchSize+" bytes in size");
	                	sendMessageBatch(numInBatch, messageBatchReq, messageSizes, messageBatchHandles, messageBatchSize);
	                    messageBatchReq = new ArrayList<SendMessageBatchRequestEntry>(10);
	                    messageSizes = new String[10];
	                    messageBatchHandles = new String[10];
	                    numInBatch = 0;
	                    messageBatchSize = 0;
	            	}
	            }
            }
            catch (InterruptedException e)
            {
                logger.warn("Interrupted while waiting for records to appear in the read queue.");
                interrupted = true;
                Thread.currentThread().interrupt();
            }
	    }

        doneWorking = true;
        if (numInBatch > 0)
        {
        	sendMessageBatch(numInBatch, messageBatchReq, messageSizes, messageBatchHandles, messageBatchSize);
        }
	}

    private boolean alreadyContainsId(List<SendMessageBatchRequestEntry> messageBatchReq, String id)
    {
        for (SendMessageBatchRequestEntry entry : messageBatchReq)
        {
        	if (entry.getId().equals(id))
            {
                return true;
            }
        }
        return false;
    }

	private void sendMessageBatch(int numInBatch, List<SendMessageBatchRequestEntry> messageBatchReq, String[] messageSizes, String[] messageReceiptHandles, int messageBatchSize)
	{
	    List<DeleteMessageBatchRequestEntry> toDelete = new ArrayList<DeleteMessageBatchRequestEntry>(10);
        Map<String, String> batchIdtoMessageHandleMap = new LinkedHashMap<String, String>(10);
        for (int i = 0; i < numInBatch; i++)
        {
        	batchIdtoMessageHandleMap.put(messageBatchReq.get(i).getId(), messageReceiptHandles[i]);
        }
        SendMessageBatchRequest sendBatchRequest = new SendMessageBatchRequest().withQueueUrl(readerThread.getOutputQueueUrl())
                .withEntries(messageBatchReq);
        try {
            SendMessageBatchResult result = readerThread.getAws_sqs().getSQS().sendMessageBatch(sendBatchRequest);
            for (SendMessageBatchResultEntry success : result.getSuccessful())
            {
                String messageHandleToDelete = batchIdtoMessageHandleMap.get(success.getId());
                if (messageHandleToDelete != null)
            	{
                	toDelete.add(new DeleteMessageBatchRequestEntry(success.getId(), messageHandleToDelete));
            	}
                else
                {
                	logger.error("Can't find message handle for message with id "+success.getId());
                }
            }
            for (BatchResultErrorEntry errresult : result.getFailed())
            {
                logger.error("Error sending message with ID: " + errresult.getId());
                logger.error("  message is : " + errresult.getMessage());
            	logger.error("  message receipt was: "+ batchIdtoMessageHandleMap.get(errresult.getId()));
            }
            DeleteMessageBatchRequest delrequest = new DeleteMessageBatchRequest(readerThread.getInputQueueUrl()).withEntries(toDelete);
            if (toDelete.size() > 0)
            {
                DeleteMessageBatchResult delresult = readerThread.getAws_sqs().getSQS().deleteMessageBatch(delrequest);
                for (BatchResultErrorEntry errresult : delresult.getFailed())
                {
                	logger.error("Error deleting message with ID "+ errresult.getId());
                	logger.error("  message is : " + errresult.getMessage());
                }
            }
        }
        catch (com.amazonaws.services.sqs.model.BatchRequestTooLongException tooBig)
        {
            logger.error("Amazon sez I cannot handle that batch, it is too big. Perhaps I could handle a smaller one though.");
            for (int j = 0; j < numInBatch; j++)
            {
                logger.error("message : "+ messageSizes[j]);
            }
            logger.error("My computed batch size was "+ messageBatchSize, tooBig);
        }
    }

    public final static Charset utf8 = Charset.forName("UTF-8");

    private int getTotalMessageSize(String message, String batchId, String ... attributes)
    {
        int len = message.getBytes(utf8).length;
        int padFactor = 3; // a guess at the padding for each string in the attribute set
        len += batchId.length() + padFactor;
        for (String attribute : attributes)
        {
            len += attribute.length() + padFactor;
        }
        len += 1000;  // fudge factor.    MMMmm  fudge.
        return(len);
    }

}
