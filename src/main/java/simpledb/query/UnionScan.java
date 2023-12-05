package simpledb.query;

public class UnionScan implements Scan {
    private Scan[] scans;
    private int currentScanIndex;

    public UnionScan(Scan firstScan, Scan secondScan) {
        this.scans = new Scan[] { firstScan, secondScan };
        this.currentScanIndex = 0;
    }

    public void beforeFirst() {
        for (Scan scan : scans) {
            scan.beforeFirst();
        }
        currentScanIndex = 0;
    }

    public boolean next() {
        while (currentScanIndex < scans.length) {
            if (scans[currentScanIndex].next()) {
                return true;
            } else {
                currentScanIndex++;
            }
        }
        return false;
    }

    public int getInt(String fldname) {
        return scans[currentScanIndex].getInt(fldname);
    }

    public String getString(String fldname) {
        return scans[currentScanIndex].getString(fldname);
    }

    public Constant getVal(String fldname) {
        return scans[currentScanIndex].getVal(fldname);
    }

    public boolean hasField(String fldname) {
        for (Scan scan : scans) {
            if (scan.hasField(fldname)) {
                return true;
            }
        }
        return false;
    }

    public void close() {
        for (Scan scan : scans) {
            scan.close();
        }
    }
}
