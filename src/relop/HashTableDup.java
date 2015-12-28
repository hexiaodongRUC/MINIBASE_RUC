package relop;

import global.SearchKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

/**
 * an extension to java HashTable 
 * which allow duplicated tuples with same SearchKey
 */
class HashTableDup extends Hashtable<SearchKey, Tuple[]> {
	public void add(SearchKey key, Tuple value) {
		Tuple[] existing = get(key);
		if (existing == null) {
			put(key, new Tuple[] { (Tuple)value });
		} else {
			ArrayList<Tuple> a = new ArrayList<Tuple>();
			a.addAll(Arrays.asList(existing));
			a.add(value);
			Tuple[] as = new Tuple[a.size()];
			put(key, a.toArray(as));
		}
	}
	
	public Tuple[] getAll(SearchKey key) {
		return this.get(key);
	}
}
