package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{

	private final HeapFile hf;
	private final TransactionId tid; 
	private int curPageNo;
	private Iterator<Tuple> pageIterator;
	
	public HeapFileIterator(HeapFile hf, TransactionId tid) {
		this.hf = hf;
		this.tid = tid;
	}
	
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		this.curPageNo = 0;
		this.pageIterator = this.getTupleIteratorByPageNo(curPageNo);
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		if(this.pageIterator==null) {
			return false;
		}
		while(!pageIterator.hasNext()) {
			++this.curPageNo;
			if(this.curPageNo>=hf.numPages()) {
				return false;
			}
			this.pageIterator = this.getTupleIteratorByPageNo(this.curPageNo);
			
		}
		return true;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		// TODO Auto-generated method stub
		if(!this.hasNext()) {
			throw new NoSuchElementException();
		}
		return this.pageIterator.next();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		this.curPageNo = 0;
		this.pageIterator = this.getTupleIteratorByPageNo(curPageNo);		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		this.pageIterator = null;		
	}

	private Iterator<Tuple> getTupleIteratorByPageNo(int pageNo) throws TransactionAbortedException, DbException{
		HeapPage page;
		page = (HeapPage)Database.getBufferPool().getPage(this.tid, new HeapPageId(this.hf.getId()/*table id*/,pageNo), Permissions.READ_ONLY);	
		return page.iterator();
	}
	
}
