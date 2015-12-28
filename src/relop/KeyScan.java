package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;;


/**
 * wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
	private HeapFile hf;
	private SearchKey sk;
	private HashIndex hi;
	private HashScan hs;
	
	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
		this.setSchema(schema);
		this.hf = file;
		this.hi = index;
		this.sk = key;
		this.hs = hi.openScan(key);	
	}

	public void explain(int depth) {
		indent(depth);
	    System.out.println(String.format("Key Scan %s", hf.toString()));
	}

	public void restart() {
		this.close();
		hs = hi.openScan(sk);
	}

	public boolean isOpen() {
		if(hs != null) return true;
		return false;
	}

	public void close() {
		if(this.isOpen()) {
			hs.close();
			hs = null;
		}
	}
	
	public boolean hasNext() {
		if(this.isOpen()) return hs.hasNext();
		return false;
	}

	public Tuple getNext() {
		if(this.isOpen()) {
			byte[] temp = hf.selectRecord(hs.getNext());
			if(temp != null) {
				return new Tuple(this.getSchema(), temp);
			}
			throw new IllegalStateException(String.format("File %s has no next tuple", hf.toString()));
		} else {
			throw new IllegalStateException(String.format("File %s is not open", hf.toString()));
		}
	}
	
	public HeapFile getFile(){
		return hf;
	}
	 
	public HashIndex getHashIndx(){
		return hi;
	}
}