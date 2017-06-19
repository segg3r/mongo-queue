package by.segg3r.mongoqueue;

import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Wrapper upon {@link MessageQueue}. Provides typed access to the queue collection.
 * Created by Pavel_Dzunovich on 6/16/2017.
 */
public class MessageQueueTemplate {

	private MessageQueue queue;
	private MongoConverter converter;
	private QueryMapper queryMapper;

	public MessageQueueTemplate(MongoTemplate mongoTemplate, String name) {
		this.queue = new MessageQueue(mongoTemplate.getDb().getCollection(name));
		this.converter = mongoTemplate.getConverter();
		this.queryMapper = new QueryMapper(this.converter);
	}

	/**
	 * Puts message to the end of queue to be received as soon as possible and 0.0 priority.
	 * @see MessageQueue#send(BasicDBObject)
	 * @param message message. Should not be null.
	 */
	public void put(Message message) {
		BasicDBObject basicDBObject = convertToMongoType(message);
		queue.send(basicDBObject);
	}

	/**
	 * Reads top message from the queue, using empty filter, and immediately acknowledges it.
	 * @see MessageQueue#get(BasicDBObject, int)
	 * @see MessageQueueTemplate#acknowledge(Message)
	 * @param <T> message type.
	 * @return message from the top of the queue, or <b>null</b>, if queue is empty.
	 */
	public <T extends Message> T read(Class<T> clazz) {
		return read(clazz, new BasicDBObject());
	}

	/**
	 * Reads top message from the queue, using query as a filter, and immediately acknowledges it.
	 * @see MessageQueue#get(BasicDBObject, int)
	 * @see MessageQueueTemplate#acknowledge(Message)
	 * @param <T> message type.
	 * @param query query filter.
	 * @return message from the top of the queue, or <b>null</b>, if queue is empty.
	 */
	public <T extends Message> T read(Class<T> clazz, Query query) {
		return read(clazz, convertQuery(query));
	}

	/**
	 * Reads top message from the queue, using query as a filter, and immediately acknowledges it.
	 * @see MessageQueue#get(BasicDBObject, int)
	 * @see MessageQueueTemplate#acknowledge(Message)
	 * @param <T> message type.
	 * @param query query filter.
	 * @return message from the top of the queue, or <b>null</b>, if queue is empty.
	 */
	public <T extends Message> T read(Class<T> clazz, BasicDBObject query) {
		T result = read(clazz, ReadTimings.maxAcknowledgePeriod(), query);
		if (result != null) acknowledge(result);

		return result;
	}

	/**
	 * Reads top message from the queue, using empty filter, or <b>null</b>, if queue is empty.
	 * @see MessageQueue#get(BasicDBObject, int)
	 * @param <T> message type.
	 * @param timings read timings for the operation. {@link ReadTimings}
	 * @return message from the top of the queue, or <b>null</b>, if queue is empty.
	 */
	public <T extends Message> T read(Class<T> clazz, ReadTimings timings) {
		return read(clazz, timings, new BasicDBObject());
	}

	/**
	 * Reads top message from the queue, using query as a filter, or <b>null</b>, if no matching message found.
	 * @see MessageQueue#get(BasicDBObject, int)
	 * @param <T> message type.
	 * @param timings read timings for the operation. {@link ReadTimings}
	 * @param query query filter.
	 * @return message from the top of the queue, or <b>null</b>, if no matching message found.
	 */
	public <T extends Message> T read(Class<T> clazz, ReadTimings timings, Query query) {
		BasicDBObject queryDBObject = convertQuery(query);
		return read(clazz, timings, queryDBObject);
	}

	/**
	 * Reads top message from the queue, using query as a filter, or <b>null</b>, if no matching message found.
	 * @see MessageQueue#get(BasicDBObject, int)
	 * @param <T> message type.
	 * @param timings read timings for the operation. {@link ReadTimings}
	 * @param query query filter.
	 * @return message from the top of the queue, or <b>null</b>, if no matching message found.
	 */
	public <T extends Message> T read(Class<T> clazz, ReadTimings timings, BasicDBObject query) {
		BasicDBObject basicDBObject = queue.get(query, timings.getAcknowledgePeriod(), timings.getWaitDuration());
		if (basicDBObject == null) return null;

		T result = convertFromMongoType(clazz, basicDBObject);
		result.setId(basicDBObject.getObjectId("id").toHexString());

		return result;
	}

	/**
	 * Counts all messages in queue, using no filter.
	 * @see MessageQueue#count(BasicDBObject)
	 * @return number of messages in queue.
	 */
	public long count() {
		return count(new BasicDBObject());
	}

	/**
	 * Counts all messages in queue, using query as a filter.
	 * @see MessageQueue#count(BasicDBObject)
	 * @return number of messages in queue, matching provided filter.
	 */
	public long count(Query query) {
		BasicDBObject queryDBObject = convertQuery(query);
		return count(queryDBObject);
	}

	/**
	 * Counts all messages in queue, using query as a filter.
	 * @see MessageQueue#count(BasicDBObject)
	 * @return number of messages in queue, matching provided filter.
	 */
	public long count(BasicDBObject query) {
		return queue.count(query);
	}

	/**
	 * Acknowledges given message. {@link Message#id} should be populated.
	 * @see MessageQueue#ack(BasicDBObject)
	 * @param message message with a given id.
	 * @throws IllegalArgumentException if {@link Message#id} is not populated.
	 */
	public void acknowledge(Message message) {
		if (message.getId() == null)
			throw new IllegalArgumentException("Could not find message id to acknowledge.");

		BasicDBObject basicDBObject = new BasicDBObject().append("id", new ObjectId(message.getId()));
		acknowledge(basicDBObject);
	}

	/**
	 * Acknowledges message with given objectId hex representation (should not be null).
	 * @see MessageQueue#ack(BasicDBObject)
	 * @param hexObjectId message's id hex representation.
	 */
	public void acknowledge(String hexObjectId) {
		acknowledge(new ObjectId(hexObjectId));
	}

	/**
	 * Acknowledges message with given objectId (should not be null).
	 * @see MessageQueue#ack(BasicDBObject)
	 * @param objectId message's id.
	 */
	public void acknowledge(ObjectId objectId) {
		BasicDBObject basicDBObject = new BasicDBObject().append("id", objectId);
		acknowledge(basicDBObject);
	}

	/**
	 * Verifies that <b>beforeSort</b> read index exists for provided field set, otherwise creates it.
	 * @see MessageIndex
	 * @see MessageQueue#ensureGetIndex(BasicDBObject)
	 * @param beforeSort before sort index descriptor
	 */
	public void ensureIndex(MessageIndex beforeSort) {
		BasicDBObject beforeSortIndexObject = convertIndex(beforeSort);
		queue.ensureGetIndex(beforeSortIndexObject);
	}

	/**
	 * Verifies that <b>beforeSort</b> and <b>afterSort</b> read indexes both exist for provided field set,
	 * otherwise creates it.
	 * @see MessageIndex
	 * @see MessageQueue#ensureGetIndex(BasicDBObject, BasicDBObject)
	 * @param beforeSort before sort index descriptor
	 * @param afterSort after sort index descriptor
	 */
	public void ensureIndex(MessageIndex beforeSort, MessageIndex afterSort) {
		BasicDBObject beforeSortIndexObject = convertIndex(beforeSort);
		BasicDBObject afterSortIndexObject = convertIndex(afterSort);
		queue.ensureGetIndex(beforeSortIndexObject, afterSortIndexObject);
	}

	private BasicDBObject convertIndex(MessageIndex index) {
		BasicDBObject basicDBObject = new BasicDBObject();
		for (MessageIndex.Entry entry : index.getEntries()) {
			basicDBObject.put(entry.getFieldName(), entry.getOrder().value);
		}

		return basicDBObject;
	}

	private void acknowledge(BasicDBObject query) {
		queue.ack(query);
	}

	private BasicDBObject convertQuery(Query query) {
		return (BasicDBObject) queryMapper.getMappedObject(query.getQueryObject(), null);
	}

	private <T> T convertFromMongoType(Class<T> clazz, BasicDBObject dbObject) {
		return converter.read(clazz, dbObject);
	}

	private <T> BasicDBObject convertToMongoType(T message) {
		BasicDBObject basicDBObject = (BasicDBObject) converter.convertToMongoType(message);
		basicDBObject.put("_class", message.getClass().getName());

		return basicDBObject;
	}

}
