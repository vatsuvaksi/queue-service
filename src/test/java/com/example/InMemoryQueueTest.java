package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InMemoryQueueTest {
	private QueueService qs;
	private String queueUrl = "myQueueURL";
	
	@Before
	public void setup() {
		qs = new InMemoryQueueService();
	}
	
	
	@Test
	public void testSendMessageAndPull(){
		qs.push(queueUrl, "{ \"value\":\"Vatsal\", \"age\":30, \"priority\":3 }");
		qs.push(queueUrl, "{ \"value\":\"Vatsal\", \"age\":30, \"priority\":1 }");
		qs.push(queueUrl, "{ \"value\":\"Vatsal\", \"age\":30, \"priority\":2 }");


		Message msg1 = qs.pull(queueUrl);
		qs.delete(queueUrl , msg1.getReceiptId());
		Message msg2 = qs.pull(queueUrl);
		qs.delete(queueUrl , msg2.getReceiptId());
		Message msg3 = qs.pull(queueUrl);
		qs.delete(queueUrl , msg3.getReceiptId());

		// * Messages are pulled based on priority * //
		// * Further Logic can be written to remove priority key from the message object* //
		assertNotNull(msg1);
		assertNotNull(msg2);
		assertNotNull(msg3);

		assertEquals("{ \"value\":\"Vatsal\", \"age\":30, \"priority\":1 }", msg1.getBody());
		assertEquals("{ \"value\":\"Vatsal\", \"age\":30, \"priority\":2 }", msg2.getBody());
		assertEquals("{ \"value\":\"Vatsal\", \"age\":30, \"priority\":3 }", msg3.getBody());
	}

	@Test
	public void testCheckForReceiptId(){
		String msgBody = "{ \"value\":\"Vatsal\", \"age\":30, \"priority\":1 }";

		qs.push(queueUrl, msgBody);
		Message msg = qs.pull(queueUrl);

		assertEquals(msgBody, msg.getBody());
		assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);
	}

	@Test
	public void testPullEmptyQueue(){
		Message msg = qs.pull(queueUrl);
		assertNull(msg);
	}

	@Test
	public void testDeleteMessage(){
		String msgBody = "{ \"value\":\"Vatsal\", \"age\":30, \"priority\":5 }";

		qs.push(queueUrl, msgBody);
		Message msg = qs.pull(queueUrl);

		qs.delete(queueUrl, msg.getReceiptId());
		msg = qs.pull(queueUrl);

		assertNull(msg);
	}

	@Test
	public void pushShouldThrowRuntimeExceptionWhenPriorityIsMissing() {
		// Prepare a message body without the "priority" field.
		String msgBodyWithoutPriority = "{\"message\":\"This is a test message\"}";
		boolean exceptionThrown = false;

		try {
			// Call push method with the message body that lacks the "priority" field.
			qs.push(queueUrl, msgBodyWithoutPriority);
		} catch (RuntimeException e) {
			exceptionThrown = true;
			// Assert that the exception message is as expected.
			assertEquals("Priority is needed", e.getMessage());
		}

		// Assert that the RuntimeException was thrown.
		assertTrue("A RuntimeException was expected to be thrown when priority is missing", exceptionThrown);
	}
}
