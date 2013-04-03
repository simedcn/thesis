package com.ebay.kvstore.server.master.task;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterTaskManager implements IMasterTask {

	private Set<IMasterTask> tasks;

	private static Logger logger = LoggerFactory.getLogger(MasterTaskManager.class);

	public MasterTaskManager() {
		tasks = new HashSet<>();
	}

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	public void registerTask(IMasterTask task) {
		tasks.add(task);
	}

	@Override
	public void start() {
		for (IMasterTask task : tasks) {
			try {
				task.start();
			} catch (Exception e) {
				logger.error("Error occured when starting task", e);
			}
		}
	}

	@Override
	public void stop() {
		for (IMasterTask task : tasks) {
			try {
				task.stop();
			} catch (Exception e) {
				logger.error("Error occured when stopping task", e);
			}
		}
	}

	public void unregisterAll() {
		tasks.clear();
	}

	public void unregisterTask(IMasterTask task) {
		tasks.remove(task);
	}

}
