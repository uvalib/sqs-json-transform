package edu.virginia.sqsjson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

public class AwsSqsSingleton
{
    private static boolean alwaysThroughS3 = false;
    public static final int SQS_SIZE_LIMIT = 262144;
    private AmazonSQS sqs;
    private AmazonS3 s3;
    private boolean shutdown = false;
    private Map<String, MapEntry> recHandleMap = null;
    
    private class MapEntry {
        String queueUrl;
        String messageHandle;
        MapEntry chain;
        public MapEntry(String queueUrl, String messageHandle)
        {
            this.queueUrl = queueUrl;
            this.messageHandle = messageHandle;
            this.chain = null;
        }
    };
    
    private final static Logger logger = Logger.getLogger(AwsSqsSingleton.class);

    private static AwsSqsSingleton instance = null;
    
    private AwsSqsSingleton() {} ; // private to ensure its a singleton
    
    public static AwsSqsSingleton getInstance(String s3BucketName)
    {
        if (instance != null) return (instance);
        AwsSqsSingleton local = new AwsSqsSingleton();
        AmazonSQS sqstmp = AmazonSQSClientBuilder.defaultClient();
        if (s3BucketName != null)
        {
            local.s3 = AmazonS3ClientBuilder.standard().build();
            ExtendedClientConfiguration extendedClientConfig = new ExtendedClientConfiguration();
            extendedClientConfig.withLargePayloadSupportEnabled(local.s3, s3BucketName)
                .withAlwaysThroughS3(alwaysThroughS3).withMessageSizeThreshold(SQS_SIZE_LIMIT);
            
            final AmazonSQSExtendedClient sqsx = new AmazonSQSExtendedClient(sqstmp, extendedClientConfig);
            local.sqs = sqsx;
        }
        else  // Use the normal, non-extended client
        {
            local.sqs = sqstmp;
        }
        local.recHandleMap = new LinkedHashMap<String, MapEntry>();
        instance = local;
        return(instance);
    }
    
    public AmazonSQS getSQS()
    {
        return(sqs);     
    }
    
    public String getQueueUrlForName(String queueName, boolean createQueueIfNotExists)
    {
        logger.debug("Listing all queues in your account.");
        try { 
            GetQueueUrlResult queueUrlResult = sqs.getQueueUrl(queueName);
            if (queueUrlResult != null)
            {
                return (queueUrlResult.getQueueUrl());
            }
            logger.debug("SQS queue named "+ queueName+ " not found");
            throw new RuntimeException("SQS queue named "+ queueName+ " not found");
        }
        catch (QueueDoesNotExistException NoQ)
        {
            if (createQueueIfNotExists)
            {
                logger.debug("Creating a new SQS queue called "+ queueName);
                final CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
                CreateQueueResult createResult = sqs.createQueue(createQueueRequest);
                if (createResult != null) 
                {
                    return (createResult.getQueueUrl());
                }
            }
            logger.debug("SQS queue named "+ queueName+ " not found");
            throw new RuntimeException("SQS queue named "+ queueName+ " not found");
        }
    }
    
    public void add(String queueUrl, String id, String messageHandle)
    {
        String key = id;
        synchronized (recHandleMap) 
        {
            if (recHandleMap.containsKey(key))
            {
                logger.warn("Already Have a stored message handle with id "+ id + " in queue " + queueUrl + " chaining existing one");
                
                MapEntry existing = recHandleMap.get(key);
                while (existing.chain != null)
                {
                    existing = existing.chain;
                }
                existing.chain = new MapEntry(queueUrl, messageHandle);
               // getSQS().deleteMessage(new DeleteMessageRequest(mapEntry.queueUrl, mapEntry.messageHandle));
            }
            else 
            {
                recHandleMap.put(key, new MapEntry(queueUrl, messageHandle));
            }
        }
    }
    
    public void remove(String id)
    {
        String key = id;
        MapEntry entry = getRecHandleToDelete(key);
        if (entry != null)
        {
            getSQS().deleteMessage(new DeleteMessageRequest(entry.queueUrl, entry.messageHandle));            
        }
    }

    private MapEntry getRecHandleToDelete(String key)
    {
        MapEntry mapEntry = null;
        synchronized (recHandleMap) 
        {
            if (!recHandleMap.containsKey(key))
            {
                logger.warn("Do not have a stored message handle with id "+ key  );
            }
            else 
            {
                mapEntry = recHandleMap.get(key);
                MapEntry chained = mapEntry.chain;
                if (chained == null)
                {
                    recHandleMap.remove(key);
                }
                else
                {
                    recHandleMap.put(key,  chained);
                }
            }
        }
        return(mapEntry);
    }
    
    public void removeBatch(List<String> deleteBatchIds)
    {
        Iterator<String> iter = deleteBatchIds.iterator();
        while (iter.hasNext())
        {
            List<DeleteMessageBatchRequestEntry> toDelete = new ArrayList<DeleteMessageBatchRequestEntry>(10);
            String queueUrl = null;
            for (int i = 0; i < 10 && iter.hasNext(); i++)
            {
                String id = iter.next();
                String key = id;
                MapEntry entry = getRecHandleToDelete(key);
                if (entry != null)
                {
                    toDelete.add(new DeleteMessageBatchRequestEntry(id, entry.messageHandle));
                    queueUrl = entry.queueUrl;
                }
            }
            @SuppressWarnings("unused")
            DeleteMessageBatchResult res = getSQS().deleteMessageBatch(new DeleteMessageBatchRequest(queueUrl).withEntries(toDelete));
        }
    }

    public void shutdown()
    {
        if (!shutdown) 
        {
            sqs.shutdown();
            shutdown = true;
        }
    }
}
