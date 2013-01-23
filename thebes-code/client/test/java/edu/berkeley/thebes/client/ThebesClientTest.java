package edu.berkeley.thebes.client;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple ThebesClient.
 */
public class ThebesClientTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ThebesClientTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( ThebesClientTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testThebesClient()
    {
        assertTrue( true );
    }
}
