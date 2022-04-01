package edu.virginia.sqsjson;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

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
	                if (numInBatch > 0 && messageBatchSize + curMessageSize >= AwsSqsSingleton.SQS_SIZE_LIMIT)
	                {
	                    logger.info("Message batch would be too large, only sending " + (numInBatch) + " messages in batch");
	                    sendMessageBatch(numInBatch, messageBatchReq, messageSizes, messageBatchSize);
	                    messageBatchReq = new ArrayList<SendMessageBatchRequestEntry>(10);
	                    messageSizes = new String[10];
	                    numInBatch = 0;
	                    messageBatchSize = 0;
	                }
	                messageSizes[numInBatch] = id + " : " + curMessageSize;
	                messageReq = new SendMessageBatchRequestEntry(readerThread.getOutputQueueUrl(), xmlMessageBody).withId(id)
	                            .addMessageAttributesEntry("id", new MessageAttributeValue().withDataType("String").withStringValue(id))
	                            .addMessageAttributesEntry("type", new MessageAttributeValue().withDataType("String").withStringValue("application/xml"));
	                messageBatchReq.add(messageReq);
	                readerThread.getAws_sqs().add(readerThread.getInputQueueUrl(), id, messageReceiptHandle);
	                numInBatch++;
	                messageBatchSize += curMessageSize;
	                if (numInBatch == 10)
	                {
	                	logger.debug("Sending batch of " + (numInBatch) + " messages that are a total of "+messageBatchSize+" bytes in size");
	                	sendMessageBatch(numInBatch, messageBatchReq, messageSizes, messageBatchSize);
	                    messageBatchReq = new ArrayList<SendMessageBatchRequestEntry>(10);
	                    messageSizes = new String[10];
	                    numInBatch = 0;
	                    messageBatchSize = 0;
	                }
	            }
	            else
	            {
	            	if (numInBatch > 0)
	            	{
	                	logger.info("Inpout queue empty, flushing remaining " + (numInBatch) + " messages that are a total of "+messageBatchSize+" bytes in size");
	                	sendMessageBatch(numInBatch, messageBatchReq, messageSizes, messageBatchSize);
	                    messageBatchReq = new ArrayList<SendMessageBatchRequestEntry>(10);
	                    messageSizes = new String[10];
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
        	sendMessageBatch(numInBatch, messageBatchReq, messageSizes, messageBatchSize);
        }
	}
	
	private void sendMessageBatch(int numInBatch, List<SendMessageBatchRequestEntry> messageBatchReq, String[] messageSizes, int messageBatchSize)
	{
        List<String> deleteBatchIds = new ArrayList<String>(10);
        SendMessageBatchRequest sendBatchRequest = new SendMessageBatchRequest().withQueueUrl(readerThread.getOutputQueueUrl())
                .withEntries(messageBatchReq);
        try {
            SendMessageBatchResult result = readerThread.getAws_sqs().getSQS().sendMessageBatch(sendBatchRequest);
            for (SendMessageBatchResultEntry success : result.getSuccessful())
            {
                deleteBatchIds.add(success.getId());
            }
            readerThread.getAws_sqs().removeBatch(deleteBatchIds);   
        }
        catch (com.amazonaws.services.sqs.model.BatchRequestTooLongException tooBig)
        {
            logger.warn("Amazon sez I cannot handle that batch, it is too big. Perhaps I could handle a smaller one though.");
            for (int j = 0; j < numInBatch; j++)
            {
                logger.warn("message : "+ messageSizes[j]);
            }
            logger.warn("My computed batch size was "+ messageBatchSize, tooBig);
        }
	}
	
    private int getTotalMessageSize(String message, String batchId, String ... attributes)
    {
        int len = message.length();
       // len += batchId.length();
        for (String attribute : attributes)
        {
            len += attribute.length() + 3;
        }
        len += 500;  // fudge factor.    MMMmm  fudge.
        return(len);
    }

}
