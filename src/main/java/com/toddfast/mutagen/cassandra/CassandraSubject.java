package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.SimpleState;

/**
 *
 * @author Todd Fast
 */
public class CassandraSubject implements Subject<Integer> {

	/**
	 *
	 *
	 */
	public CassandraSubject(Session session) {
		super();
		if (session ==null) {
			throw new IllegalArgumentException(
				"Parameter \"session\" cannot be null");
		}

		this.session = session;
	}

	/**
	 *
	 *
	 */                   // TODO do we need write quorom - ALL consistency?
	private void createSchemaVersionTable() {
        try {
            session.execute(VERSION_CF);
        } catch (AlreadyExistsException e) {
            // horrible hack should check to see if table exists.
        }

        try {
            session.execute(CURRENT_VERSION_CF);
        } catch (AlreadyExistsException e) {
            // horrible hack should check to see if table exists.
        }
    }


	/**
	 * 
	 * 
	 */
	@Override
	public State<Integer> getCurrentState() {
        createSchemaVersionTable();

        String query = "select * from schema_current_version where state = ?";

        PreparedStatement prepare = session.prepare(query);
        BoundStatement boundStatement = prepare.bind(ROW_KEY);
        ResultSet resultSet = session.execute(boundStatement);
        Row row = resultSet.one();

        int version = 0;
        if( row != null) {
            version = row.getInt(VERSION_COLUMN);
        }

		return new SimpleState<Integer>(version);
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////


    // table stores processed versions
    private static final String VERSION_CF = "create table schema_version ( version int primary key, change text, hash text  );";
    private static final String CURRENT_VERSION_CF = "create table schema_current_version ( state text primary key, version int ); ";

	public static final String ROW_KEY="state";
	public static final String VERSION_COLUMN="version";

	private Session session;
}
