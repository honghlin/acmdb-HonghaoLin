package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    
    private ArrayList<TDItem> contents;

    
    public Iterator<TDItem> iterator() {
        // some code goes here
        //return null;
    	return contents.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	int len = typeAr.length;
    	contents = new ArrayList<>();
    	for (int i = 0; i < len; ++i) {
            contents.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	int len = typeAr.length;
    	contents = new ArrayList<>();
    	for (int i = 0; i < len; ++i) {
            contents.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        //return 0;
    	return contents.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
    	if (i < 0 || i >= contents.size()) throw new NoSuchElementException();
        return contents.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
    	if (i < 0 || i >= contents.size()) throw new NoSuchElementException();
        return contents.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        //return 0;
    	int len = contents.size();
    	for (int i = 0; i < len; i++) {
            if (Objects.equals(name, contents.get(i).fieldName)) return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        //return 0;
    	
    	int ans = 0;
    	for (TDItem i : contents) ans += i.fieldType.getLen();
    	return ans;
    	
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        //return null;
    	int len1 = td1.numFields();
        int len2 = td2.numFields();
        int len = len1 + len2;
        Type[] typeAr = new Type[len];
        String[] nameAr = new String[len];
        for (int i = 0; i < len1; i++) {
            TDItem t = td1.contents.get(i);
            typeAr[i] = t.fieldType;
            nameAr[i] = t.fieldName;
        }
        for (int i = 0; i < len2; i++) {
            TDItem t = td2.contents.get(i);
            typeAr[len1 + i] = t.fieldType;
            nameAr[len1 + i] = t.fieldName;
        }
        return new TupleDesc(typeAr, nameAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        //return false;
    	
    	if (!(o instanceof TupleDesc)) return false;
        else if (((TupleDesc) o).contents.size() != contents.size()) return false;
        else {
        	for (int i = 0; i < contents.size(); i++) {
        		if (!Objects.equals(contents.get(i).fieldType, ((TupleDesc) o).contents.get(i).fieldType)) return false;                
            }
            return true;
        }
    	
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        //return "";
    	String s = "";
    	for (TDItem i : contents) {
    		if(!s.equals("")) s += ", ";
    		s += i.fieldType.name();
    		if (i.fieldName != null) s += "(" + i.fieldName + ")" ;
    		
    	}
    	return s;
    }
}
