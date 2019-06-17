package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

    private int numPages;
    private Map<PageId, Page> pageMap;
    //private Page[] pages;
    private LockManager lockManager = new LockManager();
    private Random R = new Random();



    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        //this.numPages = 1000000;
        this.pageMap = new LinkedHashMap<>();
        //this.pages = new Page[numPages];

    }




    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;

        long tle = 100 + R.nextInt(100);
        long begin = System.currentTimeMillis();

        while(true) {
            if(lockManager.apply(tid, pid, perm)) break;
            if (System.currentTimeMillis() - begin > tle) throw new TransactionAbortedException();

            try {
                wait(tle);
            }
            catch (Exception e) {}

        }

        if (pageMap.containsKey(pid)) {
            Page page = pageMap.get(pid);
            pageMap.remove(pid);
            pageMap.put(pid, page);
            return page;

        }
        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        if(pageMap.size() >= numPages) evictPage(); //throw new DbException("");
        pageMap.put(pid, page);
        return page;
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        //return false;
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        if (commit) flushPages(tid);
        else {
            //if(!lockManager.tidMap.containsKey(tid)) return;
            Map<PageId, Permissions> map = lockManager.tidMap.get(tid);
            if(map != null) for (PageId pid : map.keySet()) discardPage(pid);
        }

        lockManager.transactionComplete(tid);


    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        try {
            ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
            for (Page page: pages) {
                page.markDirty(true, tid);
                PageId pid = page.getId();
                if (pageMap.containsKey(pid)) {
                    pageMap.remove(pid);
                    pageMap.put(pid, page);
                }
                if(pageMap.size() >= numPages) evictPage();
                //throw new DbException("");
                pageMap.put(pid, page);
            }
        }
        catch (DbException | IOException e) {}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        try {
            ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
            for (Page page: pages) page.markDirty(true, tid);
            for (Page page: pages) {
                PageId pid = page.getId();
                if (pageMap.containsKey(pid)) {
                    pageMap.remove(pid);
                    pageMap.put(pid, page);
                }
                else if(pageMap.size() >= numPages) {
                    evictPage();
                    pageMap.put(pid, page);
                }
                //throw new DbException("");

            }
        } catch (DbException | IOException e) {}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid: pageMap.keySet()) {
            try {
                flushPage(pid);
            }
            catch (IOException e) {}
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pageMap.containsKey(pid)){
            pageMap.get(pid).markDirty(false, null);
            pageMap.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageMap.get(pid);
        if(page == null) return;
        if (page.isDirty() != null) {
            page.markDirty(false, null);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(!lockManager.tidMap.containsKey(tid)) return;
        Map<PageId, Permissions> map = lockManager.tidMap.get(tid);
        if(map.size() == 0) return;
        for (PageId pid : map.keySet()) flushPage(pid);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        assert pageMap.size() >= numPages;

        Iterator<PageId> it = pageMap.keySet().iterator();
        PageId pid = null;
        while(true) {
            if(!it.hasNext()) throw new DbException("");
            pid = it.next();
            Page page = pageMap.get(pid);
            //if(lockManager.pidMap.containsKey(pid) && lockManager.pidMap.get(pid).size() != 0) continue;
            if(page.isDirty() == null) break;
        }
        Page page = pageMap.get(pid);

        pageMap.remove(pid);

//    	Iterator<PageId> it = pageMap.keySet().iterator();
//    	PageId pid = it.next();
//    	Page page = pageMap.get(pid);
//    	try {
//    		flushPage(pid);
//    	}
//    	catch(IOException e) {}
//    	pageMap.remove(pid);
//

    }


    private class LockManager {

        private Map<TransactionId, Map<PageId, Permissions>> tidMap;
        private Map<TransactionId, Map<PageId, Permissions>> depMap;
        private Map<PageId, Map<TransactionId, Permissions>> pidMap;

        LockManager() {
            tidMap = new ConcurrentHashMap<>();
            pidMap = new ConcurrentHashMap<>();
            depMap = new ConcurrentHashMap<>();
        }


        private void addLock(TransactionId tid, PageId pid, Permissions perm){

            if (tidMap.containsKey(tid)){
                Map<PageId, Permissions> map = tidMap.get(tid);
                //Map<PageId, Permissions> e_map = depMap.get(tid);
                map.put(pid, perm);
                //e_map.put(pid, perm);
            }
            else {

                Map<PageId, Permissions> map = new ConcurrentHashMap<>();
                //Map<PageId, Permissions> e_map = new HashMap<>();
                map.put(pid, perm);
                //e_map.put(pid, perm);
                tidMap.put(tid, map);
                //depMap.put(tid, e_map);
            }

            if (pidMap.containsKey(pid)){
                Map<TransactionId, Permissions> map = pidMap.get(pid);
                map.put(tid, perm);

            }
            else {
                Map<TransactionId, Permissions> map = new ConcurrentHashMap<>();
                map.put(tid, perm);
                pidMap.put(pid, map);
            }

        }

        //        private void addEdge(TransactionId tid, PageId pid, Permissions perm) {
//
//        	//assert !pidMap.get(pid).containsKey(tid);
//
//            if (depMap.containsKey(tid)){
//                Map<PageId, Permissions> e_map = depMap.get(tid);
//                e_map.put(pid, perm);
//            }
//            else {
//                Map<PageId, Permissions> e_map = new ConcurrentHashMap<>();
//                e_map.put(pid, perm);
//                depMap.put(tid, e_map);
//            }
//
//        }
//
//        private void removeEdge(TransactionId tid, PageId pid) {
//
//            if (depMap.containsKey(tid)){
//                Map<PageId, Permissions> e_map = depMap.get(tid);
//                if (e_map.containsKey(pid)) e_map.remove(pid);
//            }
//
//        }

//        private boolean detect(TransactionId tid, PageId pid) {
//
//        }


        private synchronized boolean apply(TransactionId tid, PageId pid, Permissions perm) {


            assert perm == Permissions.READ_WRITE || perm == Permissions.READ_ONLY;

            if (!pidMap.containsKey(pid) || pidMap.get(pid).size() == 0){

                addLock(tid, pid, perm);
                return true;

            }
            else {
                Map<TransactionId, Permissions> map = pidMap.get(pid);

                if(perm == Permissions.READ_WRITE) {

//                	if(pidMap.get(pid).containsKey(tid)) {
//                		synchronized (this){
//                            addLock(tid, pid, perm);
//                            return true;
//                        }
//                	}

                	if (map.size() > 1) return false;
                    else {
                        if(map.containsKey(tid)) {

                            addLock(tid, pid, perm);
                            assert map.size() == 1;
                            return true;
                        }
                        return false;
                    }
                }

                else {

//                	if(pidMap.get(pid).containsKey(tid)) {
//
//                        //addLock(tid, pid, perm);
//                        return true;
//                	}

                    if (map.size() > 1){

                        addLock(tid, pid, perm);
                        return true;
                    }
                    else {
                        if(map.containsKey(tid)) return true;
                        Permissions tperm = null;
                        for (Permissions p : map.values()) tperm = p;

                        assert tperm == Permissions.READ_WRITE || perm == Permissions.READ_ONLY;
                        if(tperm == Permissions.READ_WRITE) {
                            return false;
                        }
                        else {
                            addLock(tid, pid, perm);
                            return true;
                        }
                    }
                }
            }


        }

        private void removeLock(TransactionId tid, PageId pid) {

            if (tidMap.containsKey(tid)){
                Map<PageId, Permissions> map = tidMap.get(tid);
                //Map<PageId, Permissions> e_map = depMap.get(tid);
                if (map.containsKey(pid)) map.remove(pid);
                //if (e_map.containsKey(pid)) e_map.remove(pid);
            }

            if (pidMap.containsKey(pid)){
                Map<TransactionId, Permissions> map = pidMap.get(pid);
                if (map.containsKey(tid)) map.remove(tid);
            }
        }

        private boolean holdsLock(TransactionId tid, PageId p) {

            return pidMap.containsKey(p) && pidMap.get(p).containsKey(tid);
        }

        private void releasePage(TransactionId tid, PageId pid) {

            removeLock(tid, pid);
        }


        public void transactionComplete(TransactionId tid) {

            if (tidMap.containsKey(tid)){
                Map<PageId, Permissions> map = tidMap.get(tid);
                for (PageId pid : map.keySet()) {

                    if (pidMap.containsKey(pid)){
                        Map<TransactionId, Permissions> tmap = pidMap.get(pid);
                        if (tmap.containsKey(tid)) tmap.remove(tid);
                    }
                }
                tidMap.remove(tid);
            }
        }


    }

}
