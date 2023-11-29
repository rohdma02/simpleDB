package simpledb.query;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import simpledb.metadata.StatInfo;
import simpledb.plan.Plan;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.record.Layout;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author roman
 */
public class SelectScanUnitTest {

  private SimpleDB db = new SimpleDB("collegedb");

  public SelectScanUnitTest() {
  }

  @BeforeAll
  public static void setUpClass() {
  }

  @AfterAll
  public static void tearDownClass() {
  }

  @BeforeEach
  public void setUp() {
  }

  @AfterEach
  public void tearDown() {
  }

  @Test
  public void testSelectAllScan() {
    System.out.println("SELECT ALL");
    Transaction tx = db.newTx();
    String qry = "select sid from student";
    Plan p = db.planner().createQueryPlan(qry, tx);
    tx.commit();
    assertEquals(9, p.recordsOutput());
  }

  @Test
  public void testSelectByConstantScan() {
  System.out.println("SELECT BY CONSTANT");
  Transaction tx = db.newTx();
  String qry = "select sname from student where MajorId=10";
  Plan p = db.planner().createQueryPlan(qry, tx);
  tx.commit();
  assertEquals(2, p.recordsOutput());
  }

}
