package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.*;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import org.junit.*;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 *
 * @author Todd Fast
 */
public class CassandraMutagenImplTest {

    private static final String KEY_SPACE = "TEST_KEYSPACE";

    public CassandraMutagenImplTest() {
	}


	@BeforeClass
	public static void setUpClass()
			throws Exception {
		defineKeyspace();
		createKeyspace();
	}

	private static void defineKeyspace() {
        int maxConnections = 1;

        PoolingOptions pools = new PoolingOptions();
        pools.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 2);
        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);

        Cluster cluster = new Cluster.Builder()
                .withPoolingOptions(pools)
                .addContactPoint("localhost")
                //.withPort(9160)
                .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
                .build();

        session = cluster.connect();
	}

	private static void createKeyspace() {

		System.out.println("Creating session "+ session +"...");

        session.execute("create keyspace " + KEY_SPACE + " with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

        session.execute("use " + KEY_SPACE + ";");

		System.out.println("Created session "+ session);
	}


	@AfterClass
	public static void tearDownClass()
			throws Exception {
		session.execute("drop keyspace " + KEY_SPACE);
		System.out.println("Dropped session "+ session);
	}


	@Before
	public void setUp() {
	}


	@After
	public void tearDown() {
	}


	/**
	 * This is it!
	 *
	 */
	private Plan.Result<Integer> mutate()
			throws IOException {

		// Get an instance of CassandraMutagen
		// Using Nu: CassandraMutagen mutagen=$(CassandraMutagen.class);
		CassandraMutagen mutagen=new CassandraMutagenImpl();

		// Initialize the list of mutations
		String rootResourcePath="com/toddfast/mutagen/cassandra/test/mutations";
		mutagen.initialize(rootResourcePath);

		// Mutate!
		Plan.Result<Integer> result=mutagen.mutate(session);

		return result;
	}


	/**
	 *
	 *
	 */
	@Test
	public void testInitialize() throws Exception {

		Plan.Result<Integer> result = mutate();

		// Check the results
		State<Integer> state=result.getLastState();

		System.out.println("Mutation complete: "+result.isMutationComplete());
		System.out.println("Exception: "+result.getException());
		if (result.getException()!=null) {
			result.getException().printStackTrace();
		}
		System.out.println("Completed mutations: "+result.getCompletedMutations());
		System.out.println("Remining mutations: "+result.getRemainingMutations());
		System.out.println("Last state: "+(state!=null ? state.getID() : "null"));

		assertTrue(result.isMutationComplete());
		assertNull(result.getException());
		assertEquals((state!=null ? state.getID() : (Integer)(-1)),(Integer)4);
	}


	/**
	 *
	 *
	 */
	@Test
	public void testData() throws Exception {

        String query = "select * from Test1 where key = ?";
        PreparedStatement prepare = session.prepare(query);


        ResultSet resultSet = session.execute(prepare.bind("row1"));
        Row row = resultSet.one();

        assertEquals("foo", row.getString("value1"));
		assertEquals("bar", row.getString("value2"));

        resultSet = session.execute(prepare.bind("row2"));
        row = resultSet.one();

		assertEquals("chicken",row.getString("value1"));
		assertEquals("sneeze", row.getString("value2"));

        resultSet = session.execute(prepare.bind("row3"));
        row = resultSet.one();

		assertEquals("bar",row.getString("value1"));
		assertEquals("baz",row.getString("value2"));
	}

	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private static Session session;

}
