package simpledb.query;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.plan.ExtendPlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class ExtendScanDemo {
    public static void main(String[] args) {
        System.out.println("EXTEND");
        SimpleDB db = new SimpleDB("collegedb");
        MetadataMgr mdm = db.mdMgr();
        Transaction tx = db.newTx();
        Plan studentTblPlan = new TablePlan(tx, "student", mdm);
        tx.commit();
        Plan plan0 = new ExtendPlan(studentTblPlan, "gradclass", INTEGER, 4);
        Plan plan = new ExtendPlan(plan0, "coolclass", VARCHAR, 4);
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
