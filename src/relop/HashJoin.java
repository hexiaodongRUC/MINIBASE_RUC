package relop;

import global.SearchKey;
import global.RID;
import index.HashIndex;
import heap.HeapFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

/**
 * hash join operation
 */
public class HashJoin extends Iterator {
	private Iterator outerIt;
	private Iterator innerIt;
	private Integer outerCol;
	private Integer innerCol;
	private IndexScan outerScan;
	private IndexScan innerScan;
	
	private HashTableDup htInMem; // current in-memory HashTable with duplicates
	private int htInMemHashKey; // current hash key
	private Track track; // used when iterate through the current htInMem
	
	private class Track {
		public HashTableDup htInMem;
		public IndexScan innerScan;
		
		public SearchKey[] sklist;
		public int skIndex;
		public KeyScan ks;
		
		public Tuple[] tplist;
		public int tpIndex;
		
		private Tuple outerTuple;
		private Tuple nextTuple;
		
		public Track(HashTableDup htInMem, IndexScan innerScan) {
			this.htInMem = htInMem;
			this.innerScan = innerScan;
			
			// search key level
			SearchKey[] skl = new SearchKey[htInMem.keySet().size()];
			this.sklist = htInMem.keySet().toArray(skl);
			this.skIndex = 0;
			this.ks = new KeyScan(innerScan.getSchema(), innerScan.getHashIndex(), this.sklist[this.skIndex], innerScan.getHeapFile());
			
			// tuple level
			this.tplist = htInMem.get(this.sklist[this.skIndex]);
			this.tpIndex = 0;
			this.outerTuple = this.tplist[this.tpIndex];
		}
		
		public boolean hasNext() {
			while(true) {
				if(this.ks.hasNext() && this.outerTuple != null) {
					this.nextTuple = Tuple.join(this.outerTuple, this.ks.getNext(), HashJoin.this.getSchema());
					return true;
				}
				if(this.tpIndex + 1 < this.tplist.length) {
					this.tpIndex++;
					this.outerTuple = this.tplist[this.tpIndex];
					this.ks.restart();
					continue;
				}
				if(this.skIndex + 1 < this.sklist.length) {
					this.skIndex++;
					this.tplist = this.htInMem.get(this.sklist[this.skIndex]);
					this.tpIndex = 0;
					this.outerTuple = this.tplist[this.tpIndex];
					this.ks = new KeyScan(innerScan.getSchema(), innerScan.getHashIndex(), this.sklist[this.skIndex], innerScan.getHeapFile());
					continue;
				}
				break;
			}
			return false;
		}
		
		public Tuple getNext() {
			return nextTuple;
		}
	}
	
	public HashJoin(Iterator outer, Iterator inner, Integer outerCol, Integer innerCol) {
		this.setSchema(Schema.join(outer.getSchema(), inner.getSchema()));
		this.outerIt = outer;
		this.innerIt = inner;
		this.outerCol = outerCol;
		this.innerCol = innerCol;
		// build stage
		this.outerScan = build(outerIt, outerCol);
		this.innerScan = build(innerIt, innerCol);
	}
	
	public void explain(int depth) {
		indent(depth);
		System.out.println(String.format("Hash Join for %s and %s", this.outerIt.toString(), this.innerIt.toString()));
	}

	public void restart() {
		outerScan.restart();
		innerScan.restart();
		outerIt.restart();
		innerIt.restart();
		this.htInMem = null;
		this.track = null;
	}

	public boolean isOpen() {
		return outerIt.isOpen() && innerIt.isOpen() && outerScan.isOpen() && innerScan.isOpen();
	}

	public void close() {
		outerIt.close();
		innerIt.close();
		outerScan.close();
		innerScan.close();
	}

	public boolean hasNext() {
		if(this.isOpen()) {
			while(true) {
				while(this.htInMem != null && this.track != null && this.track.hasNext()) {
					return true;
				}
				if(!outerScan.hasNext()) break;
				this.htInMemHashKey = buildHashTab();
				this.track = new Track(this.htInMem, this.innerScan);
			}
		}		
		return false;
	}

	public Tuple getNext() {
		if(this.isOpen()) {
			return this.track.getNext();
		}
		throw new IllegalStateException(String.format("Iterator %s is not open", this.toString()));
	}
	
	/**
	 * build stage
	 * build hash index for child iterator
	 * @param it child iterator
	 * @param col field to be indexed
	 * @return index scan wrapper
	 */
	private IndexScan build(Iterator it, int col) {
		if(it instanceof IndexScan) {
			// have HashIndex
			return (IndexScan)it;
		} else if(it instanceof KeyScan) {
			// have HashIndex
			return new IndexScan(it.getSchema(), ((KeyScan) it).getHashIndx(), ((KeyScan) it).getFile());
		} else if(it instanceof FileScan) {
			// have File, have no HashIndex
			FileScan temp = (FileScan)it;
			HashIndex hi = new HashIndex(it.toString());
			while(temp.hasNext()) {
				Tuple tmp = temp.getNext();
				Object colum = tmp.getField(col);
				hi.insertEntry(new SearchKey(colum), temp.getLastRID());
			}
			return new IndexScan(temp.getSchema(), hi, temp.getFile());
		} else {
			// have no File nor HashIndex
			HeapFile hf = new HeapFile(null);
			HashIndex hi = new HashIndex(it.toString());
			while(it.hasNext()) {
				Tuple tmp = it.getNext();
				Object colum = tmp.getField(col);
				RID rid = hf.insertRecord(tmp.getData());
				hi.insertEntry(new SearchKey(colum), rid);
			}
			return new IndexScan(it.getSchema(), hi, hf);
		}
	}
	
	/**
	 * build a in-memory hash table for prob stage
	 * @return the hash key of current bucket
	 */
	private int buildHashTab() {
		this.htInMem = new HashTableDup();
		if(outerScan.hasNext()) {
			int lastHashKey = outerScan.getNextHash();
			while(outerScan.hasNext() && outerScan.getNextHash() == lastHashKey) {
				Tuple t = outerScan.getNext();
				Object colum = t.getField(outerCol);
				htInMem.add(new SearchKey(colum), t);
			}
			return lastHashKey;
		}
		throw new IllegalStateException(String.format("Iterator %s has no next tuple", this.toString())); 
	}
}
