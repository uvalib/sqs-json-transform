package edu.virginia.sqsjson;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.virginia.sqsjson.AwsSqsSingleton;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Uses the command-line arguments to 
 *
 * @author rh9ec
 *
 */
public class SQSQueueDriver 
{
    private static Logger logger = null;

    public final static String SQS_JSON_TRANSFORM_IN_QUEUE =  "SQS_JSON_TRANSFORM_IN_QUEUE";
    public final static String SQS_JSON_TRANSFORM_OUT_QUEUE = "SQS_JSON_TRANSFORM_OUT_QUEUE";
    public final static String SQS_JSON_TRANSFORM_MESSAGE_BUCKET =  "SQS_JSON_TRANSFORM_MESSAGE_BUCKET";
    public final static int SQS_JSON_TRANSFORM_QUEUE_POLL_TIMEOUT = 20; // in seconds
//    private boolean reconfigurable = false;
    private AwsSqsSingleton aws_sqs = null;

	private String inputQueueName; 
    private String inputQueueUrl;
    private String outputQueueUrl;
    String s3BucketName;
    private boolean createQueueIfNotExists = false;
    private boolean alreadyWaiting = false;
    private List<Message> curMessages;
    private int curMessageIndex;
    private int messagesSinceLastSleep = 0;
    private OptionSet options;
    private ReceiveMessageRequest receiveMessageRequest;
    JsonToXMLConverter converter;
    private BlockingQueue<Message> readQ;

	private boolean multithreaded;
	int numTransformWorkers;

	private boolean doneReading = false;

	private TranslateWorker[] workers;

   
    /**
     * The main entry point of the SolrMarc indexing process. Typically called by the Boot class.
     *
     * @param args - The command-line arguments passed to the program
     */
    public static void main(String[] args)
    {
        SQSQueueDriver driver = new SQSQueueDriver(args);
        driver.execute();
    }

    /**
     *
     * @param args - The command-line arguments passed to the program
     */
    public SQSQueueDriver(String[] args)
    {
        logger = Logger.getLogger(SQSQueueDriver.class);

        initialize(args);
    }
    
    protected void initialize(String[] args)
    {
        OptionParser parser = new OptionParser(  );
        parser.accepts("sqs-in", "sqs queue to read records from").withRequiredArg().ofType( String.class );
        parser.accepts("sqs-out", "sqs queue to write solr docs to").withRequiredArg().ofType( String.class );
        parser.accepts("s3", "s3 bucket to use for oversize records").withRequiredArg().ofType( String.class );
        parser.accepts("threads", "number of separate writer threads to create").withRequiredArg().ofType( String.class );
        options = null;
        try {
            options = parser.parse(args );
        }
        catch (OptionException uoe)
        {
            try
            {
                System.err.println(uoe.getMessage());
                parser.printHelpOn(System.err);
            }
            catch (IOException e)
            {
            }
            System.exit(1);
        }
        
        s3BucketName = getSqsParm(options, "s3", SQS_JSON_TRANSFORM_MESSAGE_BUCKET);
        aws_sqs = AwsSqsSingleton.getInstance(s3BucketName);
        this.configureInput(options);
        this.configureOutput(options);
        int buffersize = 640;
        doneReading = false;
        readQ = new ArrayBlockingQueue<Message>(buffersize);
    }

    private void configureInput(OptionSet options)
    {
        inputQueueName = getSqsParm(options, "sqs-in", SQS_JSON_TRANSFORM_IN_QUEUE);
        logger.info("Opening input queue: "+ inputQueueName + ((s3BucketName != null) ? " (with S3 bucket: "+ s3BucketName + " )" : ""));
        inputQueueUrl = aws_sqs.getQueueUrlForName(inputQueueName, createQueueIfNotExists);
        receiveMessageRequest = new ReceiveMessageRequest(inputQueueUrl).withMaxNumberOfMessages(10).withMessageAttributeNames("All").withWaitTimeSeconds(20);
    }

    protected void configureOutput(OptionSet options)
    {
        String sqsOutQueue = getSqsParm(options, "sqs-out", SQS_JSON_TRANSFORM_OUT_QUEUE);
        String s3Bucket = getSqsParm(options, "s3", SQS_JSON_TRANSFORM_MESSAGE_BUCKET);
        logger.info("Opening output queue: "+ sqsOutQueue + ((s3Bucket != null) ? " (with S3 bucket: "+ s3Bucket + " )" : ""));
        outputQueueUrl = aws_sqs.getQueueUrlForName(sqsOutQueue, createQueueIfNotExists);
        String numTransformWorkerStr = options.has("threads") ? options.valueOf("threads").toString() : "0";
        try {
        	numTransformWorkers = Integer.parseInt(numTransformWorkerStr);
        }
        catch (NumberFormatException nfe)
        {
            logger.warn("threads argument must be integer value from 0 (single threaded) to 4 (1 reader thread, 4 writer threads)");
            logger.warn("using value of 0 (AKA single thread for reading and writing) ");
            numTransformWorkers = 0;
        }
        multithreaded = (numTransformWorkers > 0) ? true : false;
    }

    private void execute()
    {
        if (multithreaded )
        {
        	ExecutorService transformExecutor = Executors.newFixedThreadPool(numTransformWorkers);
            workers = new TranslateWorker[numTransformWorkers];
            for (int i = 0; i < numTransformWorkers; i++)
            {
                workers[i] = new TranslateWorker(this, readQ, i);
            }
            for (int i = 0; i < numTransformWorkers; i++)
            {
            	transformExecutor.execute(workers[i]);
            }

        	while (!doneReading && ! Thread.currentThread().isInterrupted())
            {
                try {
                    List <Message> curMessages = aws_sqs.getSQS().receiveMessage(receiveMessageRequest).getMessages();
                    if (curMessages.size() > 0)
                    {
                        messagesSinceLastSleep += curMessages.size();
                        if (alreadyWaiting) 
                        {
                            logger.info("Read queue " + this.inputQueueName + " active again. Getting to work.");
                            alreadyWaiting = false;
                        }
                        for (Message message : curMessages) 
                        {
                        	readQ.add(message);
                        }
                    }
                    else // timed out without finding any records.  
                    {
                        if (!alreadyWaiting)
                        {
                            logger.info("Read queue " + this.inputQueueName + " is empty. Waiting for more records");
                            logger.info(messagesSinceLastSleep + " messages received since waking up.");
                            alreadyWaiting = true;
                            messagesSinceLastSleep = 0;
                        }
                    }
                }
                // this is sent when the sqs object is shutdown.  It causes the reader thread to terminate cleanly.
                // although at present it should actually be triggered.
                catch(com.amazonaws.AbortedException abort)
                {
                    doneReading = true;
                }
                catch(com.amazonaws.services.s3.model.AmazonS3Exception s3e)
                {
                    logger.error("Read queue " + this.inputQueueName + " Failed to get the S3 object associated with large SQS message. ");
                }
                catch(com.amazonaws.AmazonServiceException s3e)
                {
                    logger.error("Read queue " + this.inputQueueName + " Failed to get the S3 object associated with large SQS message. ");
                }
                catch(com.amazonaws.SdkClientException cas)
                {
                    logger.error("Read queue " + this.inputQueueName + " Failed trying to read SQS message. ");
                    doneReading = true;
                }
            }
            if (Thread.currentThread().isInterrupted())
            {
                logger.info("Interrupt received, shutting down");
                doneReading = true;
            }
            transformExecutor.shutdown();
            boolean doneShuttingDown = false;
            logger.info("waiting for writer threads to shutdown cleanly ... ");
            while (!doneShuttingDown)
            {
                try {
                	doneShuttingDown = transformExecutor.awaitTermination(2, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                	doneShuttingDown = false;
                }
            }
            logger.info(".... finished!");
        }
        else // not multi threaded
        {
            converter = new JsonToXMLConverter();
            while (hasNext())
	        {
	            Message message = curMessages.get(curMessageIndex++);
	            String id = message.getMessageAttributes().get("id").getStringValue(); 
	            final String messageReceiptHandle = message.getReceiptHandle();
	            XMLNode xmltree = converter.parseInput(message);
	            StringWriter sw = new StringWriter();
	            PrintWriter out = new PrintWriter(sw);
	            xmltree.traverse(out);
	            String messageBody = sw.toString();
	            sendMessage(id, messageBody);
	            aws_sqs.getSQS().deleteMessage(new DeleteMessageRequest(inputQueueUrl, messageReceiptHandle));            
	        }
        }
        aws_sqs.shutdown();
    }
    
    public boolean hasNext()
    {
        if (this.curMessages == null && this.curMessageIndex == 0 || this.curMessageIndex >= this.curMessages.size())
        {
            // make blocking call
            fetchMessages();
        }
        return(curMessages == null ? false : this.curMessageIndex < this.curMessages.size());
    }

    private void fetchMessages()
    {
        curMessageIndex = -1;
        while (curMessageIndex == -1 && ! Thread.currentThread().isInterrupted())
        {
            try {
                curMessages = aws_sqs.getSQS().receiveMessage(receiveMessageRequest).getMessages();
                if (curMessages.size() > 0)
                {
                    curMessageIndex = 0;
                    messagesSinceLastSleep += curMessages.size();
                    if (alreadyWaiting) 
                    {
                        logger.info("Read queue " + this.inputQueueName + " active again. Getting to work.");
                        alreadyWaiting = false;
                    }
                }
//                else if (Boolean.parseBoolean(System.getProperty("solrmarc.sqsdriver.terminate.on.empty", "false")))
//                {
//                    logger.info("Read queue " + this.inputQueueName + " is empty and solrmarc.sqsdriver.terminate.on.empty it true.  Calling it a day.");
//                    curMessages = null;
//                    curMessageIndex = 0;
//                }
                else // timed out without finding any records.   If there is a partial chunk waiting to be sent, flush it out.
                {
                    if (!alreadyWaiting)
                    {
                        logger.info("Read queue " + this.inputQueueName + " is empty. Waiting for more records");
                        logger.info(messagesSinceLastSleep + " messages received since waking up.");
                        alreadyWaiting = true;
                        messagesSinceLastSleep = 0;
                    }
                }
            }
            // this is sent when the sqs object is shutdown.  It causes the reader thread to terminate cleanly.
            // although at present it should actually be triggered.
            catch(com.amazonaws.AbortedException abort)
            {
                curMessages = null;
                curMessageIndex = 0;
            }
            catch(com.amazonaws.services.s3.model.AmazonS3Exception s3e)
            {
                logger.error("Read queue " + this.inputQueueName + " Failed to get the S3 object associated with large SQS message. ");
            }
            catch(com.amazonaws.AmazonServiceException s3e)
            {
                logger.error("Read queue " + this.inputQueueName + " Failed to get the S3 object associated with large SQS message. ");
            }
            catch(com.amazonaws.SdkClientException cas)
            {
                logger.error("Read queue " + this.inputQueueName + " Failed trying to read SQS message. ");
                curMessages = null;
                curMessageIndex = 0;
            }
        }
        if (Thread.currentThread().isInterrupted())
        {
            curMessages = null;
            curMessageIndex = 0;
        }
    }

	public boolean isDoneReading()
	{
		return doneReading;
	}

	public void setDoneReading(boolean doneReading)
	{
		this.doneReading = doneReading;
	}
	
    public AwsSqsSingleton getAws_sqs()
    {
		return aws_sqs;
	}

	public String getOutputQueueUrl()
	{
		return outputQueueUrl;
	}

	public String getInputQueueUrl()
	{
		return inputQueueUrl;
	}

    private String getSqsParm(OptionSet options, String clOptName, String propertyOrEnvName)
    {
        return (options.has(clOptName) ? options.valueOf(clOptName).toString() : 
            System.getProperty(propertyOrEnvName)!= null ?  System.getProperty(propertyOrEnvName) :
                System.getenv(propertyOrEnvName));
    }
    
    public int sendMessage(String id, String messageBody)
    {
        SendMessageRequest message = new SendMessageRequest(outputQueueUrl, messageBody)
                .addMessageAttributesEntry("id", new MessageAttributeValue().withDataType("String").withStringValue(id))
                .addMessageAttributesEntry("type", new MessageAttributeValue().withDataType("String").withStringValue("application/xml"));
        aws_sqs.getSQS().sendMessage(message);

        return(1);
    }

	public boolean isInterrupted() 
	{
		// TODO Auto-generated method stub
		return false;
	}

}
