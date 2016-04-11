package org.elasticsearch.kafka.indexer.service;

import org.elasticsearch.client.transport.TransportClient;

import java.util.concurrent.ExecutionException;

/**
 * Created by dhyan on 1/28/16.
 */
public interface IMessageHandler {
	/**
	 * This is usually the only method one would want to customize - to do message transformation if needed
	 * @param inputMessage - Kafka message
	 * @param offset - offset of the message
	 * @return
	 * @throws Exception
	 */
    public byte[] transformMessage( byte[] inputMessage, Long offset) throws Exception;
    /**
     * This method should rarely by customized; if you think you need to - take a look at the implementation in the
     * BasicMessageHandler class
     * 
     * @param inputMessage - Kafka message
     * @throws Exception
     */
    public void processMessage(byte[] inputMessage) throws Exception;
    
    // below methods shoudl almost never be customized - just delegate to the BasicMessageHandler implementation
    
    /**
     * In most cases - do not customize this method, just delegate to the BasicMessageHandler implementation
     * @return
     * @throws Exception
     */
    public boolean postToElasticSearch() throws Exception;
    
    /**
     * In most cases - do not customize this method, just delegate to the BasicMessageHandler implementation
     * @param indexName
     * @param indexType
     * @param eventUUID
     * @param needsRouting
     * @param routingValue
     * @param jsonEvent
     * @throws ExecutionException
     */
    public void addEventToBulkRequest(String indexName, String indexType, 
		String eventUUID, boolean needsRouting, String routingValue, String jsonEvent) throws ExecutionException;
    /**
     * Do not customize this method, just delegate to the BasicMessageHandler implementation
     * @return
     */
    public TransportClient getEsTransportClient();
}
