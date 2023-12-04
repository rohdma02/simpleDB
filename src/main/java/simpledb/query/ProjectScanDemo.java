package simpledb.query;

import java.util.Arrays;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.ProjectPlan;
import simpledb.plan.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * Demo of the scan class corresponding to the <i>project</i> relational
 * algebra operator.
 * 
 * @author roman
 */
public class ProjectScanDemo {
    public static void main(String[] args) {
        System.out.println("PROJECT");
        SimpleDB db = new SimpleDB("collegedb");
        MetadataMgr mdm = db.mdMgr();
        Transaction tx = db.newTx();
        Plan studentTblPlan = new TablePlan(tx, "student", mdm);
        tx.commit();
        Plan plan = new ProjectPlan(studentTblPlan, Arrays.asList("sid", "sname"));
        Scan scan = plan.open();
        for (String field : plan.schema().fields()) {
            System.out.printf("%10s", field);
        }
        System.out.println();
        while (scan.next()) {
            for (String field : plan.schema().fields()) {
                System.out.printf("%10s", scan.getVal(field).toString());
            }
            System.out.println();
        }
    }
}
