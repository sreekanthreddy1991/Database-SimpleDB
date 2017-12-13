package simpledb;

import java.io.*;

import java.util.HashMap;
import java.util.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

enum LockType{Slock, Xlock}

class LockData{
    LockType lockType;
    ArrayList<TransactionId> transactionsForThisPage;

    LockData(LockType lockType, ArrayList<TransactionId> t){
        this.lockType = lockType;
        this.transactionsForThisPage = t;
    }
}

class ConcurrencyControl{
    ConcurrentHashMap<TransactionId, ArrayList<PageId>> xactPageMap = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
    ConcurrentHashMap<PageId, LockData> pageLockInfoMap = new ConcurrentHashMap<PageId, LockData>();

    private synchronized void block(long start, long timeout)
            throws TransactionAbortedException {
        if (System.currentTimeMillis() - start > timeout) {
            throw new TransactionAbortedException();
        }

        try {
            wait(timeout);
            if (System.currentTimeMillis() - start > timeout) {
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, LockType type)
            throws TransactionAbortedException{
        long start = System.currentTimeMillis();
        Random rand = new Random();
        long timeout = rand.nextInt(2000);
        while(true) {
            if (pageLockInfoMap.containsKey(pid)) {
                if (pageLockInfoMap.get(pid).lockType == LockType.Slock) {
                    if (type == LockType.Slock) {
                        if (!( pageLockInfoMap.get(pid).transactionsForThisPage.contains(tid) )) {
                            pageLockInfoMap.get(pid).transactionsForThisPage.add(tid);
                        }
                        updateXactPageMap(tid, pid);
                        return;
                    } else {
                        if (( pageLockInfoMap.get(pid).transactionsForThisPage.size() == 1 ) && ( pageLockInfoMap.get(pid).transactionsForThisPage.get(0) == tid ) && ( xactPageMap.containsKey(tid) ) && ( xactPageMap.get(tid).contains(pid) )) {
                            pageLockInfoMap.get(pid).lockType = LockType.Xlock;
                            return;
                        } else {
                            block(start, timeout);
                        }
                    }
                } else {
                    if (pageLockInfoMap.get(pid).transactionsForThisPage.get(0) != tid) {
                        block(start, timeout);
                    }
                    else
                        return;
                }
            } else {
                updateXactPageMap(tid, pid);
                ArrayList<TransactionId> t = new ArrayList<TransactionId>();
                t.add(tid);
                pageLockInfoMap.put(pid, new LockData(type, t));
                return;
            }
        }
    }

    private synchronized void updateXactPageMap(TransactionId tid, PageId pid){
        if(xactPageMap.containsKey(tid)){
            if(xactPageMap.get(tid).contains(pid))
                return;
            else
                xactPageMap.get(tid).add(pid);
        }else{
            ArrayList pageList = new ArrayList<PageId>();
            pageList.add(pid);
            xactPageMap.put(tid, pageList);
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (xactPageMap.containsKey(tid)) {
            xactPageMap.get(tid).remove(pid);
            if (xactPageMap.get(tid).size() == 0) {
                xactPageMap.remove(tid);
            }
        }
        if (pageLockInfoMap.containsKey(pid)) {
            pageLockInfoMap.get(pid).transactionsForThisPage.remove(tid);
            if (pageLockInfoMap.get(pid).transactionsForThisPage.size() == 0) {
                pageLockInfoMap.remove(pid);
            } else {
                notifyAll();
            }
        }
    }

    public synchronized void releaseAllLocksForTransaction(TransactionId tid){
        if(xactPageMap.containsKey(tid)){
            PageId[] pages = new PageId[xactPageMap.get(tid).size()];
            PageId[] pagesToRelease = xactPageMap.get(tid).toArray(pages);
            for(PageId pid: pagesToRelease){
                releaseLock(tid, pid);
            }
        }
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        if (xactPageMap.containsKey(tid)) {
            if (xactPageMap.get(tid).contains(pid)) {
                return true;
            }
        }
        return false;
    }
}

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

    private int numPages = DEFAULT_PAGES;

    class Node{
        PageId key;
        Page page;
        Node pre;
        Node next;

        public Node(PageId key, Page value){
            this.key = key;
            this.page = value;
        }
    }

    public class PageBufferPool {
        int capacity;
        HashMap<PageId, Node> map = new HashMap<PageId, Node>();
        Node head=null;
        Node end=null;

        public PageBufferPool(int capacity) {
            this.capacity = capacity;
        }

        public Page get(PageId key) {
            if(map.containsKey(key)){
                Node n = map.get(key);
                remove(n);
                setHead(n);
                return n.page;
            }

            return null;
        }

        public boolean containsKey(PageId pageId){
            return map.containsKey(pageId);
        }

        public void remove(Node n){
            if(n.pre!=null){
                n.pre.next = n.next;
            }else{
                head = n.next;
            }

            if(n.next!=null){
                n.next.pre = n.pre;
            }else{
                end = n.pre;
            }

        }

        public void setHead(Node n){
            n.next = head;
            n.pre = null;

            if(head!=null)
                head.pre = n;

            head = n;

            if(end ==null)
                end = head;
        }

        public void set(PageId key, Page value) {
            if(map.containsKey(key)){
                Node old = map.get(key);
                old.page = value;
                remove(old);
                setHead(old);
            }else{
                Node created = new Node(key, value);
                if(map.size()>=capacity){
                    map.remove(end.key);
                    remove(end);
                    setHead(created);

                }else{
                    setHead(created);
                }

                map.put(key, created);
            }
        }

        public Page evictPage(){
            for(Map.Entry<PageId, Node> entry: map.entrySet()){
                PageId key = entry.getKey();
                Node oldPage = entry.getValue();
                if(oldPage.page.isDirty()==null){
                    remove(oldPage);
                    return oldPage.page;
                }
            }

            return null;
        }

        public int size(){
            return map.size();
        }
    }

//    private PageBufferPool pageBufferPool;

    private ConcurrencyControl ccControl;

    private ConcurrentHashMap<PageId, Page> pageBufferPool;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        pageBufferPool = new ConcurrentHashMap<PageId, Page>();
        ccControl = new ConcurrencyControl();
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        LockType type;
        if(perm == Permissions.READ_ONLY)
            type = LockType.Slock;
        else
            type = LockType.Xlock;

        ccControl.acquireLock(tid, pid, type);
        if(pageBufferPool.containsKey(pid)){
            return pageBufferPool.get(pid);
        } else {
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            if(pageBufferPool.size() >= numPages) {
                evictPage();
            }
            pageBufferPool.put(pid, page);
            return page;
        }
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
        ccControl.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return ccControl.holdsLock(tid, p);
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
        List<PageId> transactionLockedPages = ccControl.xactPageMap.get(tid);
        if(transactionLockedPages!=null){
            for(PageId pageId: transactionLockedPages){
                if(commit){
                    flushPage(pageId);
                } else if(pageBufferPool.get(pageId)!=null && pageBufferPool.get(pageId).isDirty()!=null){
                    discardPage(pageId);
                }
            }
        }
        ccControl.releaseAllLocksForTransaction(tid);
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
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pagesDirtied = dbFile.insertTuple(tid, t);
        for (Page dirtyPage : pagesDirtied) {
            dirtyPage.markDirty(true, tid);
            pageBufferPool.remove(dirtyPage.getId());
            pageBufferPool.put(dirtyPage.getId(), dirtyPage);
        }
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
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pagesDirtied = dbFile.deleteTuple(tid, t);
        for(Page dirtyPage: pagesDirtied){
            dirtyPage.markDirty(true, tid);
            pageBufferPool.remove(dirtyPage.getId());
            pageBufferPool.put(dirtyPage.getId(), dirtyPage);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Enumeration<PageId> it = pageBufferPool.keys();
        Iterator<PageId> iterator = pageBufferPool.keySet().iterator();
        while (iterator.hasNext()) {
            flushPage(iterator.next());
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
//        pageBufferPool.remove(pageBufferPool.map.get(pid));
        pageBufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        if(pageBufferPool.containsKey(pid)){
            Page oldPage = pageBufferPool.get(pid);
            if(oldPage.isDirty()!= null){
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(oldPage);
                oldPage.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        if(ccControl.xactPageMap.containsKey(tid)){
            ArrayList<PageId> pagesToBeFlushed = ccControl.xactPageMap.get(tid);
            for (PageId pidToBeFlushed : pagesToBeFlushed)
                flushPage(pidToBeFlushed);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        for (Map.Entry<PageId, Page> entry : pageBufferPool.entrySet()) {
            PageId pid = entry.getKey();
            Page   p   = entry.getValue();
            if (p.isDirty() == null) {
                // dont need to flushpage since all page evicted are not dirty
                // flushPage(pid);
                discardPage(pid);
                return;
            }
        }
        throw new DbException("BufferPool: evictPage: all pages are marked as dirty");
    }

}
