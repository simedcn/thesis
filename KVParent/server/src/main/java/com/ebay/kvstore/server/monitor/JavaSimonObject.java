package com.ebay.kvstore.server.monitor;

import org.javasimon.Split;
import org.javasimon.Stopwatch;

public class JavaSimonObject implements IMonitorObject {

	private Stopwatch watch = null;
	private Split split;

	public JavaSimonObject(Stopwatch watch) {
		this.watch = watch;
	}

	@Override
	public void start() {
		if (watch == null) {
			return;
		}
		if (split != null) {
			throw new IllegalStateException("The monitor object has already started");
		}
		split = watch.start();
	}

	@Override
	public void stop() {
		if (watch == null) {
			return;
		}
		if (split == null) {
			throw new IllegalStateException("The monitor object has not started yet");
		}
		split.stop();
	}

}
