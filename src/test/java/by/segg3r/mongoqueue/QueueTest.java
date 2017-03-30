package by.segg3r.mongoqueue;

import by.segg3r.testng.util.mongo.MongoStartupListener;
import by.segg3r.testng.util.spring.SpringContextListener;
import com.mongodb.BasicDBObject;
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

@Listeners({SpringContextListener.class, MongoStartupListener.class})
public class QueueTest {

	@Autowired
	private MongoDbFactory mongoFactory;
	@Autowired
	private MongoTemplate mongoTemplate;

	private Queue queue;

	@BeforeClass
	public void initTest() {
		DBCollection collection = mongoFactory.getDb().getCollection("queue");
		queue = new Queue(collection);
	}

	@Test(description = "should read saved message")
	public void testSaveRead() throws Exception {
		SimpleMessage message = new SimpleMessage("pavel", "dzunovich");
		Object messageObject = mongoTemplate.getConverter().convertToMongoType(message);
		queue.send((BasicDBObject) messageObject);

		BasicDBObject firstMessage = queue.get((BasicDBObject) messageObject, 10);
		expect(firstMessage.getString("key")).toEqual("pavel");
		expect(firstMessage.getString("value")).toEqual("dzunovich");
	}

	@Test(description = "should not read same message twice without ack")
	public void testAckReadTwiceFails() {
		SimpleMessage message = new SimpleMessage("pavel", "dzunovich");
		Object messageObject = mongoTemplate.getConverter().convertToMongoType(message);
		queue.send((BasicDBObject) messageObject);

		BasicDBObject firstMessage = queue.get((BasicDBObject) messageObject, Integer.MAX_VALUE);
		BasicDBObject secondMessage = queue.get((BasicDBObject) messageObject, Integer.MAX_VALUE);
		expect(firstMessage).not().toBeNull();
		expect(secondMessage).toBeNull();
	}

	@Test(description = "should read same message twice if it was not ack in given time")
	public void testAckReadTwice() throws Exception {
		SimpleMessage message = new SimpleMessage("pavel", "dzunovich");
		Object messageObject = mongoTemplate.getConverter().convertToMongoType(message);
		queue.send((BasicDBObject) messageObject);

		BasicDBObject firstMessage = queue.get((BasicDBObject) messageObject, 1);
		Thread.sleep(1000);
		BasicDBObject secondMessage = queue.get((BasicDBObject) messageObject, Integer.MAX_VALUE);
		expect(firstMessage).not().toBeNull();
		expect(secondMessage).not().toBeNull();
	}

	@Test(description = "should not read same message twice if it was ack in given time")
	public void testAckGivenTime() throws Exception {
		SimpleMessage message = new SimpleMessage("pavel", "dzunovich");
		Object messageObject = mongoTemplate.getConverter().convertToMongoType(message);
		queue.send((BasicDBObject) messageObject);

		BasicDBObject firstMessage = queue.get((BasicDBObject) messageObject, 1);
		queue.ack(firstMessage);
		Thread.sleep(1000);
		BasicDBObject secondMessage = queue.get((BasicDBObject) messageObject, Integer.MAX_VALUE);
		expect(firstMessage).not().toBeNull();
		expect(secondMessage).toBeNull();
	}

	@Test(description = "should correctly read messages in multiple threads")
	public void stressTest() throws Exception {
		int amount = 100000;
		int threadsCount = 10;

		Map<Integer, Boolean> result = new ConcurrentHashMap<>();
		queue.ensureGetIndex(new BasicDBObject().append("key", 1));

		for (int i = 0; i < amount; i++) {
			SimpleMessage message = new SimpleMessage("message", String.valueOf(i));
			Object messageObject = mongoTemplate.getConverter().convertToMongoType(message);
			queue.send((BasicDBObject) messageObject);
		}

		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < threadsCount; i++) {
			Thread thread = new Thread(() -> {
				while (true) {
					DBCollection collection = mongoFactory.getDb().getCollection("queue");
					queue = new Queue(collection);

					BasicDBObject query = new BasicDBObject().append("key", "message");
					BasicDBObject object = queue.get(query, 1);

					if (object == null) {
						System.out.println(Thread.currentThread() + " finished.");
						return;
					}
					int id = Integer.parseInt(object.getString("value"));
					if (Boolean.TRUE.equals(result.get(id))) {
						throw new RuntimeException("Same message was read twice.");
					}

					result.put(id, true);
					queue.ack(object);

					System.out.println(Thread.currentThread() + " : " + id);
				}
			});
			threads.add(thread);
			thread.start();
		}

		for (Thread thread : threads) {
			thread.join();
		}

		for (int i = 0; i < amount; i++) {
			if (!Boolean.TRUE.equals(result.get(i))) System.out.println("Missing " + i);
		}
		expect(result.size()).toEqual(amount);
	}

}
