package edu.stanford.identiscape.util;

import java.util.HashSet;
import java.util.LinkedList;

/**
 * This class maintains an implementation of a free list, for use in object
 * allocator classes. It maintains two complementary lists of content objects.
 * When one list is incremented by one element, the other is decremented by one.
 */
public class FreeList {
	/** My list of free entries */
	private LinkedList free_;

	/** My directory of free objects */
	private HashSet index_;

	/** The factory that creates new objects for this free list */
	private Factory factory_;

	/**
	 * Creates the free list. Constructs the empty free list and the object
	 * index.
	 */
	public FreeList(Factory factory) {
		free_ = new LinkedList();
		index_ = new HashSet();
		factory_ = factory;
	}

	// / The outside interface

	/**
	 * Allocates a new object. If we don't have a free object available, the
	 * object factory is asked to create a new one.
	 */
	public Object allocate() {
		// Does the free list have any items?
		Object object;
		if (!free_.isEmpty()) {
			// Get the first item
			object = free_.removeFirst();
			index_.remove(object);
		} else {
			// Don't have any. Create a new one
			object = factory_.create();
		}

		// Return the object
		return object;
	}

	/**
	 * Frees up an object. If that object is already free, an exception is
	 * thrown.
	 */
	public void free(Object object) {
		// Is the object there?
		if (index_.contains(object)) {
			throw new RuntimeException("Freeing free object " + object);
		}

		// Free it up, by adding it into the index and the list
		free_.addLast(object);
		index_.add(object);
	}

	// The object factory interface

	/**
	 * This interface represents a method for creating new content objects when
	 * we don't have any available in the free list
	 */
	public interface Factory {
		/** Create a new object */
		Object create();
	}
}
