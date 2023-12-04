package simpledb.query;

public class UnionScan implements Scan {
    private Scan firstScan;
    private Scan secondScan;
    private boolean scanUnion;

    public UnionScan(Scan firstScan, Scan secondScan) {
        this.firstScan = firstScan;
        this.secondScan = secondScan;
        this.scanUnion = true;
    }

    public void beforeFirst() {
        firstScan.beforeFirst();
        secondScan.beforeFirst();
        scanUnion = true;
    }

    public boolean next() {
        if (scanUnion && firstScan.next()) {
            return true;
        } else {
            scanUnion = false;
            return secondScan.next();
        }
    }

    public int getInt(String fldname) {
        return scanUnion ? firstScan.getInt(fldname) : secondScan.getInt(fldname);
    }

    public String getString(String fldname) {
        return scanUnion ? firstScan.getString(fldname) : secondScan.getString(fldname);
    }

    public Constant getVal(String fldname) {
        return scanUnion ? firstScan.getVal(fldname) : secondScan.getVal(fldname);
    }

    public boolean hasField(String fldname) {
        return firstScan.hasField(fldname) || secondScan.hasField(fldname);
    }

    public void close() {
        firstScan.close();
        secondScan.close();
    }
}
