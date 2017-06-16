package by.segg3r.mongoqueue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Created by Pavel_Dzunovich on 6/16/2017.
 */
public class MessageIndex {

	private final List<Entry> entries;

	public static MessageIndex byMessageId() {
		return new MessageIndex(emptyList());
	}

	public static MessageIndex byFields(Entry... entries) {
		return new MessageIndex(asList(entries));
	}

	public MessageIndex(List<Entry> entries) {
		this.entries = entries;
	}

	public List<Entry> getEntries() {
		return entries;
	}

	public static class Entry {

		public static Entry field(String fieldName) {
			return new Entry(fieldName);
		}

		public static Entry field(String fieldName, Order order) {
			return new Entry(fieldName, order);
		}

		private final String fieldName;
		private final Order order;

		public Entry(String fieldName) {
			this(fieldName, Order.ASC);
		}

		public Entry(String fieldName, Order order) {
			this.fieldName = fieldName;
			this.order = order;
		}

		public String getFieldName() {
			return fieldName;
		}

		public Order getOrder() {
			return order;
		}
	}

	public static enum Order {

		ASC(1), DESC(-1);

		public final int value;

		Order(int value) {
			this.value = value;
		}

	}


}
