package common;

import heap.HeapFile;
import heap.HeapPage;
import heap.HeapPageId;
import storage.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class Utility {

    public static Type[] getTypes(int len) {
        Type[] types = new Type[len];
        for (int i = 0; i < len; ++i)
            types[i] = Type.INT_TYPE;
        return types;
    }


    public static String[] getStrings(int len, String val) {
        String[] strings = new String[len];
        for (int i = 0; i < len; ++i)
            strings[i] = val + i;
        return strings;
    }


    public static TupleDesc getTupleDesc(int n, String name) {
        return new TupleDesc(getTypes(n), getStrings(n, name));
    }


    public static TupleDesc getTupleDesc(int n) {
        return new TupleDesc(getTypes(n));
    }


    public static Tuple getHeapTuple(int n) {
        Tuple tup = new Tuple(getTupleDesc(1));
        tup.setRecordId(new RecordId(new HeapPageId(1, 2), 3));
        tup.setField(0, new IntField(n));
        return tup;
    }

    public static Tuple getHeapTuple(int[] tupdata) {
        Tuple tup = new Tuple(getTupleDesc(tupdata.length));
        tup.setRecordId(new RecordId(new HeapPageId(1, 2), 3));
        for (int i = 0; i < tupdata.length; ++i)
            tup.setField(i, new IntField(tupdata[i]));
        return tup;
    }


    public static Tuple getHeapTuple(int n, int width) {
        Tuple tup = new Tuple(getTupleDesc(width));
        tup.setRecordId(new RecordId(new HeapPageId(1, 2), 3));
        for (int i = 0; i < width; ++i)
            tup.setField(i, new IntField(n));
        return tup;
    }


    public static Tuple getTuple(int[] tupledata, int width) {
        if(tupledata.length != width) {
            System.out.println("get Hash Tuple has the wrong length~");
            System.exit(1);
        }
        Tuple tup = new Tuple(getTupleDesc(width));
        for (int i = 0; i < width; ++i)
            tup.setField(i, new IntField(tupledata[i]));
        return tup;
    }


    public static HeapFile createEmptyHeapFile(String path, int cols) throws IOException {
        File f = new File(path);
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(new byte[0]);
        fos.close();

        HeapFile hf = openHeapFile(cols, f);
        HeapPageId pid = new HeapPageId(hf.getId(), 0);

        HeapPage page = null;
        try {
            page = new HeapPage(pid, HeapPage.createEmptyPageData());
        } catch (IOException e) {
            throw new RuntimeException("failed to create empty page in HeapFile");
        }

        hf.writePage(page);
        return hf;
    }


    public static HeapFile openHeapFile(int cols, File f) {
        TupleDesc td = getTupleDesc(cols);
        HeapFile hf = new HeapFile(f, td);
        Database.getCatalog().addTable(hf, UUID.randomUUID().toString());
        return hf;
    }

    public static HeapFile openHeapFile(int cols, String colPrefix, File f, TupleDesc td) {
        HeapFile hf = new HeapFile(f, td);
        Database.getCatalog().addTable(hf, UUID.randomUUID().toString());
        return hf;
    }

    public static HeapFile openHeapFile(int cols, String colPrefix, File f) {
        TupleDesc td = getTupleDesc(cols, colPrefix);
        return openHeapFile(cols, colPrefix, f, td);
    }

    public static String listToString(List<Integer> list) {
        StringBuilder out = new StringBuilder();
        for (Integer i : list) {
            if (out.length() > 0) out.append("\t");
            out.append(i);
        }
        return out.toString();
    }
}
