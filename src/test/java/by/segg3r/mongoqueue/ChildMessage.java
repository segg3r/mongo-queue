package by.segg3r.mongoqueue;

/**
 * Created by Pavel_Dzunovich on 6/19/2017.
 */
public class ChildMessage extends BaseMessage {

	private int value;

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

}
