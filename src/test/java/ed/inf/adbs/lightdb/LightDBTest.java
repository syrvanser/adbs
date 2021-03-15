package ed.inf.adbs.lightdb;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Test;

import javax.xml.stream.FactoryConfigurationError;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.*;

/**
 * Unit test for simple LightDB.
 */
public class LightDBTest {

	public boolean fileEquals(String file1, String file2, boolean maintainOrder) throws FileNotFoundException {
		Scanner s1 = new Scanner(new File(file1));
		Scanner s2 = new Scanner(new File(file2));
		List<String> l1 = new LinkedList<>();
		List<String> l2 = new LinkedList<>();

		while (s1.hasNext()) {
			l1.add(s1.nextLine());
		}

		while (s2.hasNext()) {
			l2.add(s2.nextLine());
		}

		if (!maintainOrder){
			Collections.sort(l1);
			Collections.sort(l2);
		}

		return l1.equals(l2);

	}

	public void testQuery(int query, boolean maintainOrder)  {
		String [] args = {"samples/db", "samples/input/query"+query+".sql", "samples/output/query"+ query + ".csv"};

		try {
			LightDB.main(args);
			assertTrue(fileEquals("samples/expected_output/query" + query + ".csv", "samples/output/query" + query + ".csv", maintainOrder));
		} catch (Exception e) {
			fail();
		}
	}

	@After
	public void clear(){
		DatabaseCatalog.getInstance().reset();
		QueryPlanExecutor.getInstance().reset();
	}

	@Test
	public void query1() {
		testQuery(1, false);
	}

	@Test
	public void query2() {
		testQuery(2, false);
	}

	@Test
	public void query3() {
		testQuery(3, false);
	}

	@Test
	public void query4() {
		testQuery(4, false);
	}

	@Test
	public void query5() {
		testQuery(5, false);
	}

	@Test
	public void query6() {
		testQuery(6, false);
	}

	@Test
	public void query7() {
		testQuery(7, true);
	}

	@Test
	public void query8() {
		testQuery(8, true);
	}

	@Test
	public void query9() {
		testQuery(9, false);
	}

	@Test
	public void query10() {
		testQuery(10, false);
	}

	@Test
	public void query11() {
		testQuery(11, false);
	}

	@Test
	public void query12() {
		testQuery(12, false);
	}

	@Test
	public void query13() {
		testQuery(13, false);
	}

	@Test
	public void query14() {
		testQuery(14, false);
	}

	@Test
	public void query15() {
		testQuery(15, false);
	}

	@Test
	public void query16() {
		testQuery(16, false);
	}

	@Test
	public void query17() {
		testQuery(17, false);
	}

	@Test
	public void query18() {
		testQuery(18, false);
	}

	@Test
	public void query19() {
		testQuery(19, false);
	}

	@Test
	public void query20() {
		testQuery(20, true);
	}

	@Test
	public void query21() {
		testQuery(21, true);
	}

	@Test
	public void query22() {
		testQuery(22, true);
	}

	@Test
	public void query23() {
		testQuery(23, false);
	}

	@Test
	public void query24() {
		testQuery(24, true);
	}

	@Test
	public void query25() {
		testQuery(25, true);
	}

	@Test
	public void query26() {
		testQuery(26, true);
	}

}
