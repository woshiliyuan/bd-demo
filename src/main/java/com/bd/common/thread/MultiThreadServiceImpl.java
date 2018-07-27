package com.bd.common.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author yuan.li
 *
 */
public class MultiThreadServiceImpl implements MultiThreadService,
		InitializingBean, DisposableBean, BeanNameAware {

	private Logger logger = LoggerFactory
			.getLogger(MultiThreadServiceImpl.class);

	private String beanName;
	private ThreadPoolExecutor threadPoolExecutor;

	private int minPoolSize;
	private int maxPoolSize;
	private int maxQueueSize;

	@Override
	public <V> List<V> syncInvokeAll(List<Callable<V>> callables) {
		checkReject(callables.size());
		List<V> responses = null;
		try {
			List<Future<V>> invokeAll = threadPoolExecutor.invokeAll(callables);
			responses = new ArrayList<V>();
			for (Future<V> future : invokeAll) {
				responses.add(future.get());
			}
			logger.debug("responses:{}", responses);
		} catch (Exception e) {
			logger.info("error:{}", e);
		}
		return responses;
	}

	/**
	 * 激活数等于最大线程数,说明线程已经用光了,拒绝执行
	 */
	private void checkReject(int n) {
		// 不需要同步,并不需要非常精确的队列大小限制
		if (threadPoolExecutor.getQueue().size() + n >= maxQueueSize) {
			logger.error("执行队列已满");
		}
	}

	@Override
	public void setBeanName(String name) {
		beanName = name;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		threadPoolExecutor = new ThreadPoolExecutor(minPoolSize, maxPoolSize,
				60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
				new DefaultThreadFactory(beanName));
	}

	@Override
	public void destroy() throws Exception {
		threadPoolExecutor.shutdown();
	}

	/**
	 * 默认的线程工厂
	 */
	public static class DefaultThreadFactory implements ThreadFactory {
		static final AtomicInteger poolNumber = new AtomicInteger(1);
		final ThreadGroup group;
		final AtomicInteger threadNumber = new AtomicInteger(1);
		final String namePrefix;

		DefaultThreadFactory(String name) {
			SecurityManager s = System.getSecurityManager();
			group = (s != null) ? s.getThreadGroup() : Thread.currentThread()
					.getThreadGroup();
			namePrefix = name + "-" + poolNumber.getAndIncrement() + "-thread-";
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(group, r, namePrefix
					+ threadNumber.getAndIncrement(), 0);
			if (t.isDaemon()) {
				t.setDaemon(false);
			}
			if (t.getPriority() != Thread.NORM_PRIORITY) {
				t.setPriority(Thread.NORM_PRIORITY);
			}
			return t;
		}
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public void setMaxQueueSize(int maxQueueSize) {
		this.maxQueueSize = maxQueueSize;
	}

	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}
}
