package heap;

import common.Type;
import common.Utility;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public class HeapFileEncoder {

    public static void convert(List<List<Integer>> tuples, File outFile, int npagebytes, int numFields) throws IOException {
        File tempInput = File.createTempFile("tempTable", ".txt");
        tempInput.deleteOnExit();
        BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput));
        for (List<Integer> tuple : tuples) {
            int writtenFields = 0;
            for (Integer field : tuple) {
                writtenFields++;
                if (writtenFields > numFields) {
                    throw new RuntimeException("Tuple has more than " + numFields + " fields: (" + Utility.listToString(tuple) + ")");
                }
                bw.write(String.valueOf(field));
                if (writtenFields < numFields) {
                    bw.write(',');
                }
            }
            bw.write('\n');
        }
        bw.close();
        convert(tempInput, outFile, npagebytes, numFields);
    }

    public static void convert(File inFile, File outFile, int npagebytes,
                               int numFields) throws IOException {
        Type[] ts = new Type[numFields];
        Arrays.fill(ts, Type.INT_TYPE);
        convert(inFile,outFile,npagebytes,numFields,ts);
    }

    public static void convert(File inFile, File outFile, int npagebytes,
                               int numFields, Type[] typeAr)
            throws IOException {
        convert(inFile,outFile,npagebytes,numFields,typeAr,',');
    }


    public static void convert(File inFile, File outFile, int npagebytes,
                               int numFields, Type[] typeAr, char fieldSeparator)
            throws IOException {

        int nrecbytes = 0;
        for (int i = 0; i < numFields ; i++) {
            nrecbytes += typeAr[i].getLen();
        }
        int nrecords = (npagebytes * 8) /  (nrecbytes * 8 + 1);  //floor comes for free


        int nheaderbytes = (nrecords / 8);
        if (nheaderbytes * 8 < nrecords)
            nheaderbytes++;  //ceiling
        int nheaderbits = nheaderbytes * 8;

        BufferedReader br = new BufferedReader(new FileReader(inFile));
        FileOutputStream os = new FileOutputStream(outFile);

        char[] buf = new char[1024];

        int curpos = 0;
        int recordcount = 0;
        int npages = 0;
        int fieldNo = 0;

        ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(nheaderbytes);
        DataOutputStream headerStream = new DataOutputStream(headerBAOS);
        ByteArrayOutputStream pageBAOS = new ByteArrayOutputStream(npagebytes);
        DataOutputStream pageStream = new DataOutputStream(pageBAOS);

        boolean done = false;
        boolean first = true;
        while (!done) {
            int c = br.read();

            // Ignore Windows/Notepad special line endings
            if (c == '\r')
                continue;

            if (c == '\n') {
                if (first)
                    continue;
                recordcount++;
                first = true;
            } else
                first = false;
            if (c == fieldSeparator || c == '\n' || c == '\r') {
                String s = new String(buf, 0, curpos);
                if (typeAr[fieldNo] == Type.INT_TYPE) {
                    try {
                        pageStream.writeInt(Integer.parseInt(s.trim()));
                    } catch (NumberFormatException e) {
                        System.out.println ("BAD LINE : " + s);
                    }
                }
                else   if (typeAr[fieldNo] == Type.STRING_TYPE) {
                    s = s.trim();
                    int overflow = Type.STRING_LEN - s.length();
                    if (overflow < 0) {
                        s  = s.substring(0,Type.STRING_LEN);
                    }
                    pageStream.writeInt(s.length());
                    pageStream.writeBytes(s);
                    while (overflow-- > 0)
                        pageStream.write((byte)0);
                }
                curpos = 0;
                if (c == '\n')
                    fieldNo = 0;
                else
                    fieldNo++;

            } else if (c == -1) {
                done = true;

            } else {
                buf[curpos++] = (char)c;
                continue;
            }

            if (recordcount >= nrecords
                    || done && recordcount > 0
                    || done && npages == 0) {
                int i = 0;
                byte headerbyte = 0;

                for (i=0; i<nheaderbits; i++) {
                    if (i < recordcount)
                        headerbyte |= (1 << (i % 8));

                    if (((i+1) % 8) == 0) {
                        headerStream.writeByte(headerbyte);
                        headerbyte = 0;
                    }
                }

                if (i % 8 > 0)
                    headerStream.writeByte(headerbyte);


                for (i=0; i<(npagebytes - (recordcount * nrecbytes + nheaderbytes)); i++)
                    pageStream.writeByte(0);

                headerStream.flush();
                headerBAOS.writeTo(os);
                pageStream.flush();
                pageBAOS.writeTo(os);

                headerBAOS = new ByteArrayOutputStream(nheaderbytes);
                headerStream = new DataOutputStream(headerBAOS);
                pageBAOS = new ByteArrayOutputStream(npagebytes);
                pageStream = new DataOutputStream(pageBAOS);

                recordcount = 0;
                npages++;
            }
        }
        br.close();
        os.close();
    }
}
