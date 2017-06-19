package by.segg3r.mongoqueue;

import by.segg3r.testng.util.mongo.MongoStartupListener;
import by.segg3r.testng.util.spring.SpringContextListener;
import com.mongodb.DBCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static by.segg3r.expectunit.Expect.expect;
import static by.segg3r.mongoqueue.MessageIndex.byMessageId;
import static by.segg3r.mongoqueue.ReadTimings.defaultTimings;
import static by.segg3r.mongoqueue.ReadTimings.maxAcknowledgePeriod;
import static java.lang.Boolean.TRUE;
import static java.lang.Integer.parseInt;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Listeners({SpringContextListener.class, MongoStartupListener.class})
public class MessageQueueTemplateTest {

	@Autowired
	private MongoDbFactory mongoFactory;
	@Autowired
	private MongoTemplate mongoTemplate;

	private MessageQueueTemplate template;

	@BeforeClass
	public void initTest() {
		template = new MessageQueueTemplate(mongoTemplate, "queue");
	}

	@Test(description = "should read typed message")
	public void testSaveReadTyped() throws Exception {
		SimpleMessage expectedMessage = new SimpleMessage("pavel", "dzunovich");
		template.put(expectedMessage);

		SimpleMessage actualMessage = template.read(SimpleMessage.class, defaultTimings().withAcknowledgePeriod(1));
		expect(actualMessage.getKey()).toEqual("pavel");
		expect(actualMessage.getValue()).toEqual("dzunovich");
		expect(actualMessage.getId()).not().toBeNull();
	}

	@Test(description = "should read child message")
	public void testReadChildTypeMessage() throws Exception {
		ChildMessage childMessage = new ChildMessage();
		childMessage.setValue(10);
		template.put(childMessage);

		expect(((ChildMessage) template.read(BaseMessage.class)).getValue()).toBe(10);
	}

	@Test(description = "should count number of messages")
	public void testReadCount() {
		template.put(new SimpleMessage("pavel", "dzunovich"));
		template.put(new SimpleMessage("pavel", "dzunovich2"));

		expect(template.count()).toBe(2L);
		expect(template.count(query(where("key").is("pavel")))).toBe(2L);
		expect(template.count(query(where("value").is("dzunovich")))).toBe(1L);
		expect(template.count(query(where("value").is("dzunovich2")))).toBe(1L);
	}

	@Test(description = "should not read same message twice without ack")
	public void testAckReadTwiceFails() {
		template.put(new SimpleMessage("pavel", "dzunovich"));

		expect(template.read(SimpleMessage.class, maxAcknowledgePeriod())).not().toBeNull();
		expect(template.read(SimpleMessage.class, maxAcknowledgePeriod())).toBeNull();
	}

	@Test(description = "should read same message twice if it was not ack in given time")
	public void testAckReadTwice() throws Exception {
		template.put(new SimpleMessage("pavel", "dzunovich"));

		expect(template.read(SimpleMessage.class, defaultTimings().withAcknowledgePeriod(1))).not().toBeNull();
		sleep(1000);
		expect(template.read(SimpleMessage.class, defaultTimings().withAcknowledgePeriod(1))).not().toBeNull();
	}

	@Test(description = "should not read same message twice if it was ack in given time")
	public void testAckGivenTime() throws Exception {
		template.put(new SimpleMessage("pavel", "dzunovich"));

		SimpleMessage actualMessage = template.read(SimpleMessage.class, defaultTimings().withAcknowledgePeriod(1));
		expect(actualMessage).not().toBeNull();
		template.acknowledge(actualMessage);
		sleep(1000);
		expect(template.read(SimpleMessage.class, maxAcknowledgePeriod())).toBeNull();
	}

	@Test(description = "should not read same message twice if it was ack in given time (immediately)")
	public void testAckImmediately() throws Exception {
		template.put(new SimpleMessage("pavel", "dzunovich"));

		expect(template.read(SimpleMessage.class)).not().toBeNull();
		sleep(1000);
		expect(template.read(SimpleMessage.class)).toBeNull();
	}

	@Test(description = "should correctly read messages in multiple threads")
	public void stressTest() throws Exception {
		int amount = 10000;
		int threadsCount = 20;

		Map<Integer, Boolean> result = new ConcurrentHashMap<>();
		template.ensureIndex(byMessageId());

		for (int i = 0; i < amount; i++) {
			template.put(new SimpleMessage("message", String.valueOf(i)));
		}

		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < threadsCount; i++) {
			Thread thread = new Thread(() -> {
				while (true) {
					MessageQueueTemplate threadTemplate = new MessageQueueTemplate(mongoTemplate, "queue");

					SimpleMessage actualThreadMessage = threadTemplate.read(SimpleMessage.class, defaultTimings().withAcknowledgePeriod(1));

					if (actualThreadMessage == null) {
						System.out.println(currentThread() + " finished.");
						return;
					}
					int id = parseInt(actualThreadMessage.getValue());
					if (TRUE.equals(result.get(id))) {
						throw new RuntimeException("Same message was read twice.");
					}

					result.put(id, true);
					threadTemplate.acknowledge(actualThreadMessage);

					System.out.println(currentThread() + " : " + id);
				}
			});
			threads.add(thread);
			thread.start();
		}

		for (Thread thread : threads) {
			thread.join();
		}

		for (int i = 0; i < amount; i++) {
			if (!TRUE.equals(result.get(i))) System.out.println("Missing " + i);
		}
		expect(result.size()).toEqual(amount);
	}

}
