package com.bd.common.thread;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

/**
 * @author yuan.li
 *
 */
public interface MultiThreadService {

	/**
	 * 多线程异步并行执行,同步返回 要么全部调度,要么全部拒绝,不能一部分拒绝,一部分调度
	 * 
	 * @param callables
	 * @return
	 * @throws RejectedExecutionException
	 */
	public <V> List<V> syncInvokeAll(List<Callable<V>> callables)
			throws RejectedExecutionException;
}
