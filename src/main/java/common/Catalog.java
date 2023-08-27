package common;

import lombok.AllArgsConstructor;
import lombok.Data;
import storage.DbFile;
import storage.HeapFile;
import storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Catalog {
    private List<Table> tables;

    @AllArgsConstructor
    @Data
    public class Table {

        private DbFile file;
        private String name;
        private String pkeyField;

        @Override
        public String toString() {
            return "table{" +
                    "file=" + file +
                    ", name='" + name + '\'' +
                    ", pkeyField='" + pkeyField + '\'' +
                    '}';
        }
    }

    public Catalog() {
        this.tables = new ArrayList<>();
    }

    public void addTable(DbFile file, String name, String pkeyField) {
        Table table = new Table(file, name, pkeyField);
        for(int i=0;i<this.tables.size();i++){
            Table tmp = this.tables.get(i);
            if (tmp.getName()==null){
                continue;
            }
            if(tmp.getName().equals(name) || tmp.getFile().getId()==file.getId()){
                this.tables.set(i,table);
                return;
            }
        }
        this.tables.add(table);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }


    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }


    public int getTableId(String name) throws NoSuchElementException {

        if(name!=null){
            for(int i=0;i<this.tables.size();i++){
                if(this.tables.get(i).getName()==null){
                    continue;
                }
                if(this.tables.get(i).getName().equals(name)){
                    return this.tables.get(i).getFile().getId();
                }
            }
        }
        throw new NoSuchElementException();
    }


    public Table getTableById(int tableId){
        for(int i=0;i<this.tables.size();i++) {
            Table table = this.tables.get(i);
            if(table.getFile().getId()==tableId){
                return table;
            }

        }
        return null;
    }


    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        Table table = this.getTableById(tableid);
        if(table!=null){
            return table.getFile().getTupleDesc();
        }
        throw new NoSuchElementException();
    }


    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        Table table = this.getTableById(tableid);
        if(table!=null){
            return table.getFile();
        }
        return null;
    }

    public String getPrimaryKey(int tableid) {
        Table table = this.getTableById(tableid);
        if(table!=null){
            return table.getPkeyField();
        }
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        List<Integer> res = new ArrayList<>();
        for(int i=0;i<this.tables.size();i++){
            res.add(this.tables.get(i).getFile().getId());
        }
        return res.iterator();
    }

    public String getTableName(int id) {
        Table table = this.getTableById(id);
        if(table!=null){
            return table.getName();
        }
        throw new NoSuchElementException();
    }

    public void clear() {
        this.tables.clear();
    }


    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            while ((line = br.readLine()) != null) {
                String name = line.substring(0, line.indexOf("(")).trim();
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }


}
