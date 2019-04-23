package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	
	private File f;
    private TupleDesc td;
	
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        //return null;
    	return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
    	return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	//return null;
        
    	try {
    		RandomAccessFile raf = new RandomAccessFile(f, "r");
            byte[] data = new byte[BufferPool.getPageSize()];
            raf.seek(pid.pageNumber() * BufferPool.getPageSize());
            raf.read(data);
            raf.close();
            return new HeapPage((HeapPageId) pid, data);
    	}
    	catch(Exception e){
    		
    	}
    	
    	throw new IllegalArgumentException();
    	
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	long div = f.length() / BufferPool.getPageSize();
    	if(f.length() % BufferPool.getPageSize() != 0) div += 1;
        return (int)div;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    public class HeapFileIterator implements DbFileIterator {
        
        private HeapFile f;
        private TransactionId tid;
        private int pid;
        private Iterator<Tuple> it;
        private int pgNo;

        public HeapFileIterator(HeapFile f, TransactionId tid) {
            this.f = f;
            this.tid = tid;
            pgNo = f.numPages();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null || pid >= pgNo) return false;
            if (it.hasNext()) return true;       
            if(pid + 1 >= pgNo) return false;    
            return ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(f.getId(), pid + 1), null)).iterator().hasNext();
            
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) throw new NoSuchElementException();
            while (!it.hasNext()) {
                if (pid + 1 >= pgNo) throw new NoSuchElementException();
                pid += 1;
                it = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(f.getId(), pid), null)).iterator();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {

            close();
            open();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pid = 0;
            this.it = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(f.getId(), this.pid), null)).iterator();
            
        }

        @Override
        public void close() {
            it = null;
        }
    }
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        //return null;
    	return new HeapFileIterator(this, tid);
    }

}

