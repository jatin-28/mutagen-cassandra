package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;

/**
 *
 *
 * @author Todd Fast
 */
public class CassandraCoordinator implements Coordinator<Integer> {

	/**
	 * 
	 * 
	 */
	public CassandraCoordinator(Session session) {
		super();
		if (session==null) {
			throw new IllegalArgumentException(
				"Parameter \"session\" cannot be null");
		}

		this.session =session;
	}

	/**
	 * 
	 * 
	 */
	@Override
	public boolean accept(Subject<Integer> subject,
			State<Integer> targetState) {
		State<Integer> currentState=subject.getCurrentState();
		return targetState.getID() > currentState.getID();
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private Session session;
}
