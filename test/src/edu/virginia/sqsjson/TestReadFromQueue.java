package edu.virginia.sqsjson;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;


public class TestReadFromQueue
{
    @Test
    public void testPushRecords()
    {
        String queueName= "mandala-ingest-kmterms-json-convert-staging";
        String s3BucketName = "mandala-ingest-staging-messages";
        
        String queueUrl = AwsSqsSingleton.getInstance(s3BucketName).getQueueUrlForName(queueName, true);
        
        // Make Sure The Queue is empty
       // PurgeQueueResult purgeResult = AwsSqsSingleton.getInstance(s3BucketName).getSQS().purgeQueue(new PurgeQueueRequest(queueUrl));
        
        pushRecordsToQueue("places-30", "data/30.json", queueUrl, s3BucketName);
        pushRecordsToQueue("places-204", "data/204.json", queueUrl, s3BucketName);
        pushRecordsToQueue("places-433", "data/433.json", queueUrl, s3BucketName);
        pushRecordsToQueue("terms-110070", "data/110070.json", queueUrl, s3BucketName);
               
    }
    
    public void pushRecordsToQueue(String id, String filename, String queueUrl, String s3BucketName)
    {
        try
        {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
            StringBuilder buffer = new StringBuilder();
            String line;
            while ((line = reader.readLine())!= null)
            {
                buffer.append(line);
            }
                
            SendMessageRequest request = new SendMessageRequest(queueUrl, buffer.toString())
                .addMessageAttributesEntry("id", new MessageAttributeValue().withDataType("String").withStringValue(id))
                .addMessageAttributesEntry("type", new MessageAttributeValue().withDataType("String").withStringValue("application/json"));
            SendMessageResult result = AwsSqsSingleton.getInstance(s3BucketName).getSQS().sendMessage(request);
            reader.close();
        }
        catch (FileNotFoundException e)
        {
            fail("Unable to find or open file "+ new File(filename).getAbsolutePath());
        } 
        catch (IOException e) 
        {
            fail("Unable to read from file "+ new File(filename).getAbsolutePath());
        }
    }
}
