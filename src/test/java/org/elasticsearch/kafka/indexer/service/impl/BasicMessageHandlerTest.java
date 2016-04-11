/**
  * @author marinapopova
  * Mar 29, 2016
 */
package org.elasticsearch.kafka.indexer.service.impl;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESException;
import org.elasticsearch.kafka.indexer.service.ElasticSearchClientService;
import org.elasticsearch.kafka.indexer.service.IIndexHandler;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.*;

/**
 * @author marinapopova
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicMessageHandlerTest {

	@Mock
    private ElasticSearchClientService elasticSearchClientService;
	@Mock
    private IIndexHandler elasticIndexHandler;

	@InjectMocks
	private BasicMessageHandler basicMessageHandler = new BasicMessageHandler();
	
	private BulkRequestBuilder mockedBulkRequestBuilder = Mockito.mock(BulkRequestBuilder.class);
	private IndexRequestBuilder mockedIndexRequestBuilder = Mockito.mock(IndexRequestBuilder.class);
	private ListenableActionFuture<BulkResponse> mockedActionFuture = Mockito.mock(ListenableActionFuture.class);
	private BulkResponse mockedBulkResponse = Mockito.mock(BulkResponse.class);
	private String testIndexName = "unitTestsIndex";
	private String testIndexType = "unitTestsType";
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// Mock all required ES classes and methods
	       Mockito.when(elasticSearchClientService.prepareBulk()).thenReturn(mockedBulkRequestBuilder);
	       Mockito.when(elasticSearchClientService.prepareIndex(Matchers.anyString(), Matchers.anyString()))
	       		.thenReturn(mockedIndexRequestBuilder);
	       Mockito.when(elasticSearchClientService.prepareIndex(Matchers.anyString(), Matchers.anyString(), Matchers.anyString()))
      			.thenReturn(mockedIndexRequestBuilder);
	       Mockito.when(mockedIndexRequestBuilder.setSource(Matchers.anyString())).thenReturn(mockedIndexRequestBuilder);
	       Mockito.when(mockedBulkRequestBuilder.execute()).thenReturn(mockedActionFuture);
	       Mockito.when(mockedActionFuture.actionGet()).thenReturn(mockedBulkResponse);
	       // mock the number of messages in the bulk index request to be 1
	       Mockito.when(mockedBulkRequestBuilder.numberOfActions()).thenReturn(1);
	       
	       Mockito.when(elasticIndexHandler.getIndexName(null)).thenReturn(testIndexName);
	       Mockito.when(elasticIndexHandler.getIndexType(null)).thenReturn(testIndexType);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}


	/**
	 * Test method for {@link org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler#processMessage(byte[])}.
	 */
	@Test
	public void testProcessMessage() {
		String message = "test message";
		try {
			basicMessageHandler.processMessage(message.getBytes());
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		Mockito.verify(elasticIndexHandler, Mockito.times(1)).getIndexName(null);
		Mockito.verify(elasticIndexHandler, Mockito.times(1)).getIndexType(null);
		Mockito.verify(elasticSearchClientService, Mockito.times(1)).prepareIndex(testIndexName, testIndexType);
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(1)).setSource(message);
		// be default, no routing should be set
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(0)).setRouting(Mockito.anyString());
	}

	@Test
	public void testProcessMessageAndPostToEs_Success() {
	    // test happy path - no failures from indexing
	    Mockito.when(mockedBulkResponse.hasFailures()).thenReturn(false);
		// make sure mock is set to a successful state
	    Mockito.when(mockedBulkRequestBuilder.execute()).thenReturn(mockedActionFuture);

		String message = "test message";
		try {
			basicMessageHandler.processMessage(message.getBytes());
			basicMessageHandler.postToElasticSearch();
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		
		Mockito.verify(elasticIndexHandler, Mockito.times(1)).getIndexName(null);
		Mockito.verify(elasticIndexHandler, Mockito.times(1)).getIndexType(null);
		Mockito.verify(elasticSearchClientService, Mockito.times(1)).prepareIndex(testIndexName, testIndexType);
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(1)).setSource(message);
		// be default, no routing should be set
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(0)).setRouting(Mockito.anyString());
		
		Mockito.verify(mockedBulkRequestBuilder, Mockito.times(1)).execute();
		try {
			Mockito.verify(elasticSearchClientService, Mockito.times(0)).reInitElasticSearch();
		} catch (InterruptedException | IndexerESException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
	}

	/**
	 * Test method for {@link org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler#addEventToBulkRequest(java.lang.String, java.lang.String, java.lang.String, boolean, java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testAddEventToBulkRequest_withUUID_withRouting() {
		String message = "test message";
		String eventUUID = "eventUUID";
		boolean needsRouting = true;
		
		try {
			basicMessageHandler.addEventToBulkRequest(
				testIndexName, testIndexType, eventUUID, needsRouting, eventUUID, message);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		// verify we called the prepareIndex with eventUUID 
		Mockito.verify(elasticSearchClientService, Mockito.times(1)).prepareIndex(testIndexName, testIndexType, eventUUID);
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(1)).setSource(message);
		// verify that routing is set to use eventUUID
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(1)).setRouting(eventUUID);
	}

	/**
	 * Test method for {@link org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler#postToElasticSearch()}.
	 */
	@Test
	public void testPostOneBulkRequestToES_NoNodeException() {
		// simulate failure due to ES cluster (or part of it) not being available
	    Mockito.when(mockedBulkRequestBuilder.execute()).thenThrow( new NoNodeAvailableException("Unit Test Exception"));
		try {
			basicMessageHandler.postOneBulkRequestToES(mockedBulkRequestBuilder);
		} catch (InterruptedException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		} catch (IndexerESException e) {
			System.out.println("Cought expected NoNodeAvailableException/IndexerESException from unit test: " + e.getMessage());
		}
				
		Mockito.verify(mockedBulkRequestBuilder, Mockito.times(1)).execute();
		// verify that reInitElasticSearch() was called
		try {
			Mockito.verify(elasticSearchClientService, Mockito.times(1)).reInitElasticSearch();
		} catch (InterruptedException | IndexerESException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
	}

	@Test
	public void testPostOneBulkRequestToES_postFailures() {
		// make sure index request itself does not fail
	    Mockito.when(mockedBulkRequestBuilder.execute()).thenReturn(mockedActionFuture);
	    // mock failures from ES indexing
	    Mockito.when(mockedBulkResponse.hasFailures()).thenReturn(true);
	    BulkItemResponse bulkItemResponse = Mockito.mock(BulkItemResponse.class);
	    Iterator<BulkItemResponse> bulkRespItr = Mockito.mock(Iterator.class);
	    Mockito.when(bulkRespItr.hasNext()).thenReturn(true, false);
	    Mockito.when(bulkRespItr.next()).thenReturn(bulkItemResponse); 
	    Mockito.when(mockedBulkResponse.iterator()).thenReturn(bulkRespItr);
	    Mockito.when(bulkItemResponse.isFailed()).thenReturn(true);
	    
	    // mock the returned failure results
	    Failure mockedFailure = Mockito.mock(Failure.class);
	    Mockito.when(bulkItemResponse.getFailure()).thenReturn(mockedFailure);
	    String failureMessage = "mocked bulkResponse failure message";
	    Mockito.when(mockedFailure.getMessage()).thenReturn(failureMessage);
	    RestStatus mockRestStatus = RestStatus.BAD_REQUEST;
	    Mockito.when(mockedFailure.getStatus()).thenReturn(mockRestStatus);
	    
		try {
			basicMessageHandler.postOneBulkRequestToES(mockedBulkRequestBuilder);
		} catch (InterruptedException | IndexerESException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
				
		Mockito.verify(mockedBulkRequestBuilder, Mockito.times(1)).execute();
		// verify that reInit was not called
		try {
			Mockito.verify(elasticSearchClientService, Mockito.times(0)).reInitElasticSearch();
		} catch (InterruptedException | IndexerESException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		
		// verify that getFailure() is called twice
		Mockito.verify(bulkItemResponse, Mockito.times(2)).getFailure();
	}

}
