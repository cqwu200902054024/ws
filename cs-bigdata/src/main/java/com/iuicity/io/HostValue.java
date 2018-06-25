package com.iuicity.io;

public class HostValue implements Comparable<HostValue> {
	private Long value;

	public HostValue(Long value) {
		this.value = value;
	}

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}

	@Override
	public int compareTo(HostValue o) {
		return value.compareTo(o.getValue());
	}

}
