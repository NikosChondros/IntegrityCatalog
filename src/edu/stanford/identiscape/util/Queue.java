package edu.stanford.identiscape.util;

import java.util.Stack;
import java.util.Vector;

/**
 * Queue.java. This implementation is based on the implementation of
 * java.util.Stack by Sun Microsystems's Jonathan Payne, version 1.24. It
 * further allows the unwinding of a stack into a queue.
 */
public class Queue extends Vector {
	/** Creates an empty Queue. */
	public Queue() {
	}

	/**
	 * Pushes an item into the queue. This has exactly the same event as:
	 * <blockquote>
	 * 
	 * <pre>
	 * addElement(item)
	 * </pre>
	 * 
	 * </blockquote>
	 * 
	 * @param item
	 *            the item to be pushed into this queue.
	 * @return the <code>item</code> argument.
	 * @see java.util.Vector#addElement
	 */
	public synchronized Object push(Object item) {
		addElement(item);
		notify();

		return item;
	}

	/**
	 * Removes the object at the end of this queue and returns that object as
	 * the value of this function.
	 * 
	 * @return The object at the end of this queue (the first item of the
	 *         <tt>Vector</tt> object).
	 * @exception EmptyQueueException
	 *                if this queue is empty.
	 */
	public synchronized Object pop() {
		Object obj;

		obj = peek();
		removeElementAt(0);

		return obj;
	}

	/**
	 * Removes the object at the end of this queue and returns that object as
	 * the value of this function. If the queue is empty, block until it no
	 * longer is.
	 * 
	 * @return The object at the end of this queue (the first item of the
	 *         <tt>Vector</tt> object).
	 */
	public synchronized Object blockingPop() {
		while (size() == 0) {
			try {
				wait();
			} catch (InterruptedException ie) {
				// Pretend that I just woke up and do nothing
			}
			if (size() != 0) {
				break;
			}
		}

		Object obj = peek();
		removeElementAt(0);

		return obj;
	}

	/**
	 * Looks at the object at the end of this queue without removing it.
	 * 
	 * @return the object at the end of this queue (the first item of the
	 *         <tt>Vector</tt> object).
	 * @exception EmptyQueueException
	 *                if this queue is empty.
	 */
	public synchronized Object peek() {
		if (size() == 0) {
			throw new EmptyQueueException();
		}
		return elementAt(0);
	}

	/**
	 * Tests if this queue is empty.
	 * 
	 * @return <code>true</code> if and only if this queue contains no items;
	 *         <code>false</code> otherwise.
	 */
	public boolean empty() {
		return size() == 0;
	}

	/**
	 * Unwind stack. Take all the elements of the given stack and puts them into
	 * the queue in the order of removal.
	 * 
	 * @param stack
	 *            the source stack
	 */
	public synchronized void unwindStack(Stack stack) {
		while (!stack.empty()) {
			push(stack.pop());
		}
	}
}
