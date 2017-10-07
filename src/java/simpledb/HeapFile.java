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

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPageId pageId;
        HeapPage page = null;
        RandomAccessFile randomAccessFile;

        try{
            byte[] fileData = HeapPage.createEmptyPageData();
            randomAccessFile = new RandomAccessFile(file, "r");
            pageId = new HeapPageId(pid.getTableId(), pid.pageNumber());
            randomAccessFile.seek(pid.pageNumber()* Database.getBufferPool().getPageSize());
            randomAccessFile.read(fileData);
            page = new HeapPage(pageId, fileData);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pageId = page.getId();
        int pageNumber = pageId.pageNumber();
        int pageSize = Database.getBufferPool().getPageSize();
        byte[] pageData = page.getPageData();
        RandomAccessFile dbFile = new RandomAccessFile(this.file, "rws");
        dbFile.skipBytes(pageNumber * pageSize);
        dbFile.write(pageData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int fileSize = (int) file.length();
        int pageSize = Database.getBufferPool().getPageSize();
        return fileSize/pageSize;
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

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(this ,tid);
    }

    class HeapFileIterator implements DbFileIterator {
        private HeapFile heapFile;
        private TransactionId transactionId;
        private Iterator<Tuple> iterator;
        private Integer currentPage;

        public HeapFileIterator(HeapFile heapFile, TransactionId tId){
            this.heapFile = heapFile;
            transactionId = tId;
            currentPage = null;
            iterator = null;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            currentPage = 0;
            iterator = tupleIterator(currentPage);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (currentPage != null) {
                while (currentPage < numPages() - 1) {
                    if (iterator.hasNext()) {
                        return true;
                    } else {
                        currentPage += 1;
                        iterator = tupleIterator(currentPage);
                    }
                }
                return iterator.hasNext();
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(hasNext()){
                return iterator.next();
            } else {
                throw new NoSuchElementException("Reached end of iterator");
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            iterator = null;
            currentPage = null;
        }

        public Iterator<Tuple> tupleIterator(int pageNo) throws TransactionAbortedException, DbException {
            PageId heapPageId = new HeapPageId(heapFile.getId(), pageNo);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
            return heapPage.iterator();
        }
    }

}

