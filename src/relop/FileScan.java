package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * construct file scan for a heap file
 */
public class FileScan extends Iterator {
	private HeapFile hf;
	private HeapScan hs;
	private RID rid;
	
	public FileScan(Schema schema, HeapFile file) {
		this.setSchema(schema);
		this.hf = file;
		this.rid = new RID();
		this.hs = file.openScan();
	}

	public void explain(int depth) {
		indent(depth);
		System.out.println(String.format("File Scan %s", this.hf.toString()));
	}

	public void restart() {
		this.close();
		hs = hf.openScan();
	}

	public boolean isOpen() {
		if(hs != null) return true;
		return false;
	}
	
	public void close() {
		if(this.isOpen()) {
			this.hs.close();
			this.hs = null;
		}
	}

	public boolean hasNext() {
		if(this.isOpen()) return hs.hasNext();
		return false;
	}
	
	public Tuple getNext() {
		if(this.isOpen()) {
			byte[] temp = hs.getNext(rid);
			if(temp != null) {
				return new Tuple(this.getSchema(), temp);
			}
			throw new IllegalStateException(String.format("File %s has no next tuple", hf.toString()));
		} else {
			throw new IllegalStateException(String.format("File %s is not open", hf.toString()));
		}
	}
	
	/**
	 * get the last obtained record's RID
	 * @return lastest RID
	 */
	public RID getLastRID() {
		return new RID(rid);
	}
	
	public HeapFile getFile(){
		return hf;
	}
}
