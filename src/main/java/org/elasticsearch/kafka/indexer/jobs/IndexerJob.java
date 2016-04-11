package org.elasticsearch.kafka.indexer.jobs;

import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;

import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.exception.IndexerESException;
import org.elasticsearch.kafka.indexer.exception.KafkaClientNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.KafkaClientRecoverableException;
import org.elasticsearch.kafka.indexer.service.ConsumerConfigService;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.elasticsearch.kafka.indexer.service.KafkaClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Callable;

public class IndexerJob implements Callable<IndexerJobStatus> {

	private static final Logger logger = LoggerFactory.getLogger(IndexerJob.class);
	private ConsumerConfigService configService;
	private IMessageHandler messageHandlerService ;
	public KafkaClientService kafkaClient;
	private long offsetForThisRound;
	private long nextOffsetToProcess;
	private boolean isStartingFirstTime;
	private final int currentPartition;
	private final String currentTopic;
    private IndexerJobStatus indexerJobStatus;
    private volatile boolean shutdownRequested = false;


	public IndexerJob(ConsumerConfigService configService, IMessageHandler messageHandlerService, 
			KafkaClientService kafkaClient, int partition) 
			throws Exception {
		this.configService = configService;
		this.currentPartition = partition;
		this.currentTopic = configService.getTopic();
		this.messageHandlerService = messageHandlerService ;
		indexerJobStatus = new IndexerJobStatus(-1L, IndexerJobStatusEnum.Created, partition);
		isStartingFirstTime = true;
		this.kafkaClient = kafkaClient;
		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Initialized);
		logger.info("Created IndexerJob for topic={}, partition={};  messageHandlerService={}; kafkaClient={}", 
			currentTopic, partition, messageHandlerService, kafkaClient);
	}

	public void requestShutdown() {
		shutdownRequested = true;
	}

	public IndexerJobStatus call() {
		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Started);
        while(!shutdownRequested){
        	try{
                if (Thread.currentThread().isInterrupted()){
                    Thread.currentThread().interrupt();
                    throw new InterruptedException(
                    	"Cought interrupted event in IndexerJob for partition=" + currentPartition + " - stopping");
                }
        		logger.debug("******* Starting a new batch of events from Kafka for partition {} ...", currentPartition);
        		
        		processBatch();
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.InProgress);	
        		Thread.sleep(configService.getConsumerSleepBetweenFetchsMs() * 1000);
        		logger.debug("Completed a round of indexing into ES for partition {}",currentPartition);
        	} catch (IndexerESException e) {
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
        		stopClients();
        		break;
        	} catch (InterruptedException e) {
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Stopped);
        		stopClients();
        		break;
        	} catch (KafkaClientNotRecoverableException e) {
        		// this is a non-recoverable error - stop the indexer job
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
        		stopClients();
        		break;
        	} catch (Exception e){
        		logger.error("Exception when starting a new round of kafka Indexer job for partition {} - will try to re-init Kafka " ,
        				currentPartition, e);
        		try {
        			kafkaClient.reInitKafka();
        		} catch (Exception e2) {
            		logger.error("Exception when starting a new round of kafka Indexer job, partition {}, exiting: "
            				+ e2.getMessage(), currentPartition);
            		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
            		stopClients();  
            		break;
        		}
        	}       
        }
		logger.warn("******* Indexing job was stopped, indexerJobStatus={} - exiting", indexerJobStatus);
		return indexerJobStatus;
	}
	
		
	public void processBatch() throws Exception {
		long jobStartTime = 0l;
		if (configService.isPerfReportingEnabled())
			jobStartTime = System.currentTimeMillis();
		if (!isStartingFirstTime) {
			offsetForThisRound = nextOffsetToProcess;
		} else {
			indexerJobStatus.setJobStatus(IndexerJobStatusEnum.InProgress);
			offsetForThisRound = kafkaClient.computeInitialOffset();
			isStartingFirstTime = false;
			nextOffsetToProcess = offsetForThisRound;
		}
		indexerJobStatus.setLastCommittedOffset(offsetForThisRound);
		

		FetchResponse fetchResponse = kafkaClient.getMessagesFromKafka(offsetForThisRound);
		if (fetchResponse.hasError()) {

			short errorCode = fetchResponse.errorCode(currentTopic, currentPartition);
			Long newNextOffsetToProcess = kafkaClient.handleErrorFromFetchMessages(errorCode, offsetForThisRound);
			if (newNextOffsetToProcess != null) {
				// this is the case when we have to re-set the nextOffsetToProcess
				nextOffsetToProcess = newNextOffsetToProcess;
			}
			// return - will try to re-process the batch again 
			return;
		}
		
		ByteBufferMessageSet byteBufferMsgSet = fetchResponse.messageSet(currentTopic, currentPartition);
		if (configService.isPerfReportingEnabled()) {
			long timeAfterKafkaFetch = System.currentTimeMillis();
			logger.debug("Completed MsgSet fetch from Kafka. Approx time taken is {} ms for partition {}",
				(timeAfterKafkaFetch - jobStartTime) ,currentPartition);
		}
		if (byteBufferMsgSet.validBytes() <= 0) {
			logger.debug("No events were read from Kafka - finishing this round of reads from Kafka for partition {}",currentPartition);
			// TODO re-review this logic
			// check a corner case when consumer did not read any events form Kafka from the last current offset - 
			// but the latestOffset reported by Kafka is higher than what consumer is trying to read from;
			long latestOffset = kafkaClient.getLastestOffset();
			if (latestOffset != offsetForThisRound) {
				logger.warn("latestOffset={} for partition={} is not the same as the offsetForThisRound for this run: {}" + 
					" - returning; will try reading messages form this offset again ", 
					latestOffset, currentPartition, offsetForThisRound);

			}
			return;
		}
		logger.debug("Starting to prepare for post to ElasticSearch for partition {}",currentPartition);
		long proposedNextOffsetToProcess = iterateAndProcessMessage(byteBufferMsgSet);

		if (configService.isPerfReportingEnabled()) {
			long timeAtPrepareES = System.currentTimeMillis();
			logger.debug("Completed preparing for post to ElasticSearch. Approx time taken: {}ms for partition {}",
					(timeAtPrepareES - jobStartTime),currentPartition );
		}
		if (configService.isDryRun()) {
			logger.info("**** This is a dry run, NOT committing the offset in Kafka nor posting to ES for partition {}****",currentPartition);
			return;
		}

		try {
			logger.info("About to post messages to ElasticSearch for partition={}, offsets {}-->{} ", 
				currentPartition, offsetForThisRound, proposedNextOffsetToProcess-1);
			messageHandlerService.postToElasticSearch();
		} catch (IndexerESException e) {
			logger.error("Error posting messages to Elastic Search for offsets {}-->{} " +
				" in partition={} - will re-try processing the batch; error: {}", 
				offsetForThisRound, proposedNextOffsetToProcess-1, currentPartition, e.getMessage());			
			return;
		} catch (ElasticsearchException e) {
			logger.error("Error posting messages to ElasticSearch for offset {}-->{} in partition {} skipping them: ",
					offsetForThisRound, proposedNextOffsetToProcess-1, currentPartition, e);
			FailedEventsLogger.logFailedEvent(offsetForThisRound, proposedNextOffsetToProcess-1, currentPartition, e.getDetailedMessage(), null);
		}
		
		nextOffsetToProcess = proposedNextOffsetToProcess;
		
		if (configService.isPerfReportingEnabled()) {
			long timeAfterEsPost = System.currentTimeMillis();
			logger.debug("Approx time to post of ElasticSearch: {} ms for partition {}",
					(timeAfterEsPost - jobStartTime),currentPartition);
		}
		logger.info("Commiting offset={} for partition={}", nextOffsetToProcess, currentPartition);
		try {
			kafkaClient.saveOffsetInKafka(nextOffsetToProcess, ErrorMapping.NoError());
		} catch (Exception e) {
			logger.error("Failed to commit nextOffsetToProcess={} after processing and posting to ES for partition={}: ",
				nextOffsetToProcess, currentPartition, e);
			throw new KafkaClientRecoverableException("Failed to commit nextOffsetToProcess=" + nextOffsetToProcess + 
				" after processing and posting to ES; partition=" + currentPartition + "; error: " + e.getMessage(), e);
		}

		if (configService.isPerfReportingEnabled()) {
			long timeAtEndOfJob = System.currentTimeMillis();
			logger.info("*** This round of IndexerJob took about {} ms for partition {} ",
					(timeAtEndOfJob - jobStartTime),currentPartition);
		}
		logger.info("*** Finished current round of IndexerJob, processed messages with offsets [{}-{}] for partition {} ****",
				offsetForThisRound, nextOffsetToProcess, currentPartition);
	}

	private long iterateAndProcessMessage(ByteBufferMessageSet byteBufferMsgSet) {
		int numProcessedMessages = 0;
		int numSkippedIndexingMessages = 0;
		int numMessagesInBatch = 0;
		long offsetOfNextBatch = 0;
		Iterator<MessageAndOffset> messageAndOffsetIterator = byteBufferMsgSet.iterator();
		while(messageAndOffsetIterator.hasNext()){
			numMessagesInBatch++;
			MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
			offsetOfNextBatch = messageAndOffset.nextOffset();
			Message message = messageAndOffset.message();
			ByteBuffer payload = message.payload();
			byte[] bytesMessage = new byte[payload.limit()];
			payload.get(bytesMessage);
			try {
				messageHandlerService.processMessage(bytesMessage);
				numProcessedMessages++;
			} catch (Exception e) {
				numSkippedIndexingMessages++;
				String msgStr = new String(bytesMessage);
				logger.error("ERROR processing message at offset={} - skipping it: {}",messageAndOffset.offset(), msgStr, e);
				FailedEventsLogger.logFailedToTransformEvent(messageAndOffset.offset(), e.getMessage(), msgStr);
			}
		}
		logger.info("Total # of messages in this batch: {}; " +
						"# of successfully transformed and added to Index: {}; # of skipped from indexing: {}; offsetOfNextBatch: {}",
				numMessagesInBatch, numProcessedMessages, numSkippedIndexingMessages, offsetOfNextBatch);
		return offsetOfNextBatch;
	}

	public void stopClients() {
		logger.info("About to stop Kafka client for topic {}, partition {}", currentTopic, currentPartition);
		if (kafkaClient != null)
			kafkaClient.close();
		logger.info("Stopped Kafka client for topic {}, partition {}", currentTopic, currentPartition);
	}
	
	public IndexerJobStatus getIndexerJobStatus() {
		return indexerJobStatus;
	}

}
