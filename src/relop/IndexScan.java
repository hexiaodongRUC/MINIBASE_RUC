package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.BucketScan;

/**
 * wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
	private HashIndex hi;
	private HeapFile hf;
	private BucketScan bs;

	public IndexScan(Schema schema, HashIndex index, HeapFile file) {
		this.setSchema(schema);
		hf = file;
		hi = index;
		bs = index.openScan();
	}

	
	public void explain(int depth) {
		this.indent(depth);
		System.out.println(String.format("Hash Index Scan %s", hf.toString()));
	}
	
	public void restart() {
		this.close();
		bs = hi.openScan();
	}

	public boolean isOpen() {
		if(bs != null) return true;
		return false;
	}

	public void close() {
		if(this.isOpen()) {
			bs.close();
			bs = null;
		}
	}

	
	public boolean hasNext() {
		if(this.isOpen()) return bs.hasNext();
		return false;
	}

	
  	public Tuple getNext() {
  		if(this.isOpen()) {
			byte[] temp = hf.selectRecord(bs.getNext());
			if(temp != null) {
				return new Tuple(this.getSchema(), temp);
			}
			throw new IllegalStateException(String.format("File %s has no next tuple", hf.toString()));
		} else {
			throw new IllegalStateException(String.format("File %s is not open", hf.toString()));
		}
  	}
  	
  	public SearchKey getLastKey() {
  		if(this.isOpen()) return bs.getLastKey();
  		throw new IllegalStateException(String.format("File %s is not open", hf.toString()));
  	}
  	
  	public int getNextHash() {
  		if(this.isOpen()) return bs.getNextHash();
  		throw new IllegalStateException(String.format("File %s is not open", hf.toString()));
  	}
  	
  	public HashIndex getHashIndex() {
  		return this.hi;
  	}
  	
  	public HeapFile getHeapFile() {
  		return this.hf;
  	}
}
