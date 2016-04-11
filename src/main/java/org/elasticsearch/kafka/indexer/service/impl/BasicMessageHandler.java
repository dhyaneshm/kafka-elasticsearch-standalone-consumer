package org.elasticsearch.kafka.indexer.service.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;

import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.exception.IndexerESException;
import org.elasticsearch.kafka.indexer.service.ElasticSearchClientService;
import org.elasticsearch.kafka.indexer.service.IIndexHandler;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class BasicMessageHandler implements IMessageHandler {

    @Autowired
	@Qualifier("elasticSearchClientService")
    private ElasticSearchClientService elasticSearchClientService;
   /* @Autowired
	@Qualifier("indexHandler")
    private IIndexHandler elasticIndexHandler;*/
    
	private static final Logger logger = LoggerFactory.getLogger(BasicMessageHandler.class);
	private Map<String, BulkRequestBuilder> bulkRequestBuilders;

    @PostConstruct
    public void init() {
	   logger.info("BasicMessageHandler initialized Ok; using ElasticSearchClientService instance = {}", 
			   elasticSearchClientService);
    }

	public void processMessage(byte[] bytesMessage) throws Exception {
		// customize this behavior as needed in your own MessageHandler implementation class
		byte[] transformedBytesMessage = transformMessage(bytesMessage, null);
		String indexName = elasticIndexHandler.getIndexName(null);
		String indexType = elasticIndexHandler.getIndexType(null);
		boolean needsRouting = false;
		String messageUUID = null;
		String messageStr = new String(transformedBytesMessage);
		logger.debug("Adding event to the ES Index builder for indexName={}; event=[[{}]]", 
				indexName, messageStr);
		addEventToBulkRequest(indexName, indexType, messageUUID, needsRouting, null, messageStr);
	}


	public void addEventToBulkRequest(String indexName, String indexType, 
			String eventUUID, boolean needsRouting, String routingValue, String jsonEvent) throws ExecutionException {
 		BulkRequestBuilder builderForThisIndex = getBulkRequestBuilder(indexName);
 		IndexRequestBuilder indexRequestBuilder = null;
 		// if uuid for messages is provided - index with uuid
 		if (StringUtils.isNotEmpty(eventUUID)) {
 			indexRequestBuilder = elasticSearchClientService.prepareIndex(indexName, indexType, eventUUID);
 		} else {
 			indexRequestBuilder = elasticSearchClientService.prepareIndex(indexName, indexType);			
 		}
		indexRequestBuilder.setSource(jsonEvent);
		if(needsRouting){
			indexRequestBuilder.setRouting(routingValue);
		}
		builderForThisIndex.add(indexRequestBuilder);
    }

    public BulkRequestBuilder getBulkRequestBuilder(String key){
 	   // add initialization of the hashmap here as well, to enable unit testing of individual methods of this class
 	   // without calling prepareForPostToElasticSearch() as the first method always
    	if (bulkRequestBuilders == null) 
    		bulkRequestBuilders = new HashMap<>();
		BulkRequestBuilder bulkRequestBuilder = bulkRequestBuilders.get(key);
		if (bulkRequestBuilder == null) {
			bulkRequestBuilder = elasticSearchClientService.prepareBulk();
			bulkRequestBuilders.put(key, bulkRequestBuilder);
		}
		return bulkRequestBuilder;
	}
	
	@Override
	public boolean postToElasticSearch() throws Exception {
		for (Map.Entry<String, BulkRequestBuilder> entry: bulkRequestBuilders.entrySet()){
			BulkRequestBuilder bulkRequestBuilder = entry.getValue();
			postOneBulkRequestToES(bulkRequestBuilder);
			logger.info("Bulk-posting to ES for index: {} # of messages: {}",
					entry.getKey(), bulkRequestBuilder.numberOfActions());
		}
		return true;
	}

    public void postOneBulkRequestToES(BulkRequestBuilder bulkRequestBuilder) 
    		throws InterruptedException, IndexerESException {
        BulkResponse bulkResponse = null;
        BulkItemResponse bulkItemResp = null;
        //Nothing/NoMessages to post to ElasticSearch
        if(bulkRequestBuilder.numberOfActions() <= 0){
            logger.warn("No messages to post to ElasticSearch - returning");
            return;
        }
        try{
            bulkResponse = bulkRequestBuilder.execute().actionGet();
        } catch (NoNodeAvailableException e) {
        	// ES cluster is unreachable or down. Re-try up to the configured number of times
        	// if fails even after then - throw an exception out to retry indexing the batch
        	logger.error("Error posting messages to ElasticSearch: " + 
				"NoNodeAvailableException - ES cluster is unreachable, will try to re-connect after sleeping ... ", e);		
        	elasticSearchClientService.reInitElasticSearch();
        	//even if re-init of ES succeeded - throw an Exception to re-process the current batch
        	throw new IndexerESException("Recovering after an NoNodeAvailableException posting messages to Elastic Search " + 
    			" - will re-try processing current batch");
        } catch(ElasticsearchException e){
            logger.error("Failed to post messages to ElasticSearch: " + e.getMessage(), e);
            throw e;
        }
        logger.debug("Time to post messages to ElasticSearch: {} ms", bulkResponse.getTookInMillis());
        if(bulkResponse.hasFailures()){
            logger.error("Bulk Message Post to ElasticSearch has errors: {}",
                    bulkResponse.buildFailureMessage());
            int failedCount = 0;
            Iterator<BulkItemResponse> bulkRespItr = bulkResponse.iterator();
            //TODO research if there is a way to get all failed messages without iterating over
            // ALL messages in this bulk post request
            while (bulkRespItr.hasNext()){
                bulkItemResp = bulkRespItr.next();
                if (bulkItemResp.isFailed()) {
                    failedCount++;
                    String errorMessage = bulkItemResp.getFailure().getMessage();
                    String restResponse = bulkItemResp.getFailure().getStatus().name();
                    logger.error("Failed Message #{}, REST response:{}; errorMessage:{}",
                            failedCount, restResponse, errorMessage);
                    // TODO: there does not seem to be a way to get the actual failed event
                    // until it is possible - do not log anything into the failed events log file
                    //FailedEventsLogger.logFailedToPostToESEvent(restResponse, errorMessage);
                }
            }
            logger.info("# of failed to post messages to ElasticSearch: {} ", failedCount);
        } else {
        	logger.info("Bulk Post to ElasticSearch finished OK");
        }
    }

	/* (non-Javadoc)
	 * @see org.elasticsearch.kafka.indexer.service.IMessageHandler#transformMessage(byte[], java.lang.Long)
	 */
	@Override
	public byte[] transformMessage(byte[] inputMessage, Long offset)
			throws Exception {
		// TODO customize this if necessary
		return inputMessage;
	}

	/* (non-Javadoc)
	 * @see org.elasticsearch.kafka.indexer.service.IMessageHandler#getEsTransportClient()
	 */
	@Override
	public TransportClient getEsTransportClient() {
		return elasticSearchClientService.getEsTransportClient();
	}

}
