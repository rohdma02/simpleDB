package simpledb.query;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.SemijoinPlan;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * Demo of the scan class corresponding to the <i>semijoin</i> relational
 * algebra operator.
 * 
 * @author roman
 */
public class SemijoinScanDemo {
    public static void main(String[] args) {
        System.out.println("SEMIJOIN");
        SimpleDB db = new SimpleDB("collegedb");
        MetadataMgr mdm = db.mdMgr();
        Transaction tx = db.newTx();
        Plan studentTblPlan = new TablePlan(tx, "student", mdm);
        Plan enrollTblPlan = new TablePlan(tx, "enroll", mdm);
        tx.commit();
        Plan plan = new SemijoinPlan(studentTblPlan, enrollTblPlan,
                new Predicate(
                        new Term(
                                new Expression("sid"),
                                new Expression("studentid"))));
        for (String field : plan.schema().fields()) {
            System.out.printf("%10s", field);
        }
        System.out.println();
        Scan scan = plan.open();
        while (scan.next()) {
            for (String field : plan.schema().fields()) {
                System.out.printf("%10s", scan.getVal(field).toString());
            }
            System.out.println();
        }
    }
}
