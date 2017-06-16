package by.segg3r.mongoqueue;

import org.springframework.data.mongodb.core.query.Query;

/**
 * Class describing timings for {@link MessageQueueTemplate#read(Class, ReadTimings, Query)} methods.
 * It is possible to specify {@link ReadTimings#acknowledgePeriod} and <b>waitDuration</b>.
 * Created by Pavel_Dzunovich on 6/16/2017.
 */
public class ReadTimings {

	private static final int DEFAULT_ACKNOWLEDGE_PERIOD = 60; // 1 minute
	private static final int DEFAULT_WAIT_DURATION = 5000; // 5 seconds

	public static ReadTimings defaultTimings() {
		return new ReadTimings()
				.withAcknowledgePeriod(DEFAULT_ACKNOWLEDGE_PERIOD)
				.withWaitDuration(DEFAULT_WAIT_DURATION);
	}

	public static ReadTimings maxAcknowledgePeriod() {
		return defaultTimings()
				.withAcknowledgePeriod(Integer.MAX_VALUE);
	}

	public static ReadTimings waitingFor(int waitDuration) {
		return defaultTimings()
				.withWaitDuration(waitDuration);
	}

	/**
	 * Period in seconds after which message should be acknowledge, otherwise it will be put back queue.
	 */
	private int acknowledgePeriod;

	/**
	 * Period in milliseconds given to the read operation for a message.
	 */
	private int waitDuration;

	private ReadTimings() {
	}

	public int getAcknowledgePeriod() {
		return acknowledgePeriod;
	}

	public int getWaitDuration() {
		return waitDuration;
	}

	public ReadTimings withAcknowledgePeriod(int acknowledgePeriod) {
		this.acknowledgePeriod = acknowledgePeriod;
		return this;
	}

	public ReadTimings withWaitDuration(int waitDuration) {
		this.waitDuration = waitDuration;
		return this;
	}

}
