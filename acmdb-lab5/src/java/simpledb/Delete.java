package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

	private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private boolean f;
    private static TupleDesc td;
	private int v;
    
	private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
        this.child = child;
        this.v = 0;
        this.f = false;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //return null;
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	f = false;
        super.open();
    }

    public void close() {
        // some code goes here
    	super.close();
        child.close();
        f = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
        f = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
    	if (v == -1) return null;
    	//int v = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
            	Database.getBufferPool().deleteTuple(tid, tuple);
            } 
            catch(IOException e) {}
            ++v;
        }
        Tuple t = new Tuple(td);
        t.setField(0, new IntField(v));
        v = -1;
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        //return null;
    	return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	child = children[0];
    }

}
