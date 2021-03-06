package com.toddfast.mutagen.cassandra.test.mutations;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;

/**
 *
 * @author Todd Fast
 */
public class V003 extends AbstractCassandraMutation {

	/**
	 *
	 *
	 */
	public V003(Session session) {
		super(session);
		state=new SimpleState<Integer>(3);
	}


	@Override
	public State<Integer> getResultingState() {
		return state;
	}



	/**
	 * Return a canonical representative of the change in string form
	 *
	 */
	@Override
	protected String getChangeSummary() {
		return "update Test1 set value1='chicken', value2='sneeze' where key='row2';";
	}

	@Override
	protected void performMutation(Context context) {
		context.debug("Executing mutation {}",state.getID());

        String query = getChangeSummary();

        try {
            getSession().execute(query);
        } catch (Exception e) {
            throw new MutagenException("Could not update columnfamily Test1",e);
        }
	}

	private State<Integer> state;
}
