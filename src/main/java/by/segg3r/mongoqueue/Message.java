package by.segg3r.mongoqueue;

/**
 * Base type to be put in {@link MessageQueueTemplate}. All messages should extend this type.
 * {@link Message#id} is a hex representation of {@link org.bson.types.ObjectId}.
 * Created by Pavel_Dzunovich on 6/16/2017.
 */
public class Message {

	private String id;

	public Message() {
	}

	public Message(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
