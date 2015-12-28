package query;

import parser.AST_Select;

import java.util.ArrayList;
import java.util.HashMap;

import global.AttrOperator;
import global.AttrType;
import global.Minibase;
import global.SearchKey;
import global.SortKey;
import relop.*;
import heap.HeapFile;
import index.HashIndex;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {
  protected int[] recordCount;
  protected String[] tables;
  protected Predicate[][] predicates;
  protected boolean isExplain;
  protected boolean isDistinct;
  private String[] cols;
  private Schema schema;
  private Iterator[] scans;
  private HashMap<String, IndexDesc> columnIndexes = new HashMap<String, IndexDesc>();
  protected Iterator final_itr;
  private Iterator tmp;
  protected Projection p;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
	isExplain = tree.isExplain;
	isDistinct = tree.isDistinct;	
	tables = tree.getTables();
	cols = tree.getColumns();
	predicates = tree.getPredicates();
	validateSelect();
	
	scans = new Iterator[tables.length];
	
	for (int i = 0; i < tables.length; i++) {
		// Initialize columnIndexes
		IndexDesc[] desc = Minibase.SystemCatalog
				.getIndexes(tables[i]);
		for (IndexDesc id : desc) {
			columnIndexes.put(id.columnName, id);
		}
	}
	
	for (int i = 0; i < scans.length; i++) {
		schema = Minibase.SystemCatalog.getSchema(tables[i]);
		HeapFile hp = new HeapFile(tables[i]);
		scans[i] = new FileScan(schema, hp);
		Predicate[][] tablePredicates = tablePredicates(schema);
		if (tablePredicates.length > 0){
			for (int j = 0; j < tablePredicates.length; j++){
				for(Predicate p : tablePredicates[j]){
					System.out.println(tables[i].toString() + ": " + p);
					if(columnIndexes.containsKey(p.getLeft())){
						IndexDesc id = columnIndexes.get(p
								.getLeft());
						HashIndex hi = new HashIndex(id.indexName);
						IndexScan is = new IndexScan(schema,hi,new HeapFile(id.indexName));
						System.out.println("use index:" + id.indexName);
						scans[i] = new Selection(is, tablePredicates[j]);
					}
					else{
						scans[i] = new Selection(scans[i], tablePredicates[j]);
					}
				}
			}
		}
	}
	
	if(tables.length > 0)
		recordCount = new int[tables.length];
	for(int i = 0; i < tables.length; i++) {
		Schema tempSchema = Minibase.SystemCatalog.getSchema(tables[i]);
		HeapFile tempfile = new HeapFile(tables[i]); 
		recordCount[i] = tempfile.getRecCnt();
	}
	
	final_itr = scans[0]; // get_table_itr(table_names[0]);
	Schema final_schema = Minibase.SystemCatalog.getSchema(tables[0]);
	
	if (tables.length >= 2) {
		tmp = null;

		for (int i = 1; i < tables.length; i++) {
			tmp = scans[i]; // get_table_itr (table_names[i]);
			// choose which relation to be outer and which one to be inner
			if (getTableCardinality(tables[i - 1]) < getTableCardinality(tables[i])) {
				System.out.println("inner table:" + tables[i].toString());
				if(getTableCardinality(tables[i - 1]) > 5){
					System.out.println("table size:" + getTableCardinality(tables[i]) + "use Hash Join");
				}
				else{
					System.out.println("table size:" + getTableCardinality(tables[i]) + "use NestedLoop Join");					
				}
				final_itr = new NestedLoopJoin(final_itr, tmp);
				final_schema = Schema.join(final_schema,
						Minibase.SystemCatalog.getSchema(tables[i]));
			} else {
				System.out.println("inner table:" + tables[i-1].toString());
				if(getTableCardinality(tables[i - 1]) > 5){
					System.out.println("table size:" + getTableCardinality(tables[i-1]) + "use Hash Join");
				}
				else{
					System.out.println("table size:" + getTableCardinality(tables[i-1]) + "use NestedLoop Join");					
				}
				final_itr = new NestedLoopJoin(tmp, final_itr);
				final_schema = Schema.join(
						Minibase.SystemCatalog.getSchema(tables[i]),
						final_schema);
			}
		}
	}
	
	if (cols.length != 0) {
		Integer[] pCols = new Integer[cols.length];
		for (int i = 0; i < pCols.length; i++)
			pCols[i] = (Integer) final_itr.getSchema()
					.fieldNumber(cols[i]);
		p = new Projection(final_itr, pCols);
	}
	else{
		Integer[] pCols = new Integer[final_schema.getCount()];
		for (int i = 0; i < final_schema.getCount(); i++) {
			pCols[i] = i;
		}
		p = new Projection(final_itr, pCols);
	}
	
  } // public Select(AST_Select tree) throws QueryException

  
  //private method to get Table Cardinality
  private int getTableCardinality(String table_name) {
	
	Predicate p = new Predicate(AttrOperator.EQ, AttrType.COLNAME,
				"relName", AttrType.STRING, table_name);
	FileScan scan = new FileScan(Minibase.SystemCatalog.s_rel,
				Minibase.SystemCatalog.f_rel);
	Selection s = new Selection(scan, p);
	Tuple t = s.getNext();
	int rec_num = (Integer) t.getField("recCount");
	
	return rec_num;
	
  }
  
  private void validateSelect() throws QueryException {
	  Schema bigSchema = Minibase.SystemCatalog.getSchema(tables[0]);

	  for (int i = 0; i < tables.length; i++)
		  bigSchema = Schema.join(bigSchema,
		  QueryCheck.tableExists(tables[i]));

	  for (int i = 0; i < cols.length; i++)
		  QueryCheck.columnExists(bigSchema, cols[i]);

	  QueryCheck.predicates(bigSchema, predicates);

  }
  
  private Predicate[][] tablePredicates(Schema schema) {
	boolean flag = true;
	ArrayList<Predicate[]> tablePredicates = new ArrayList<Predicate[]>();
	for (int i = 0; i < predicates.length; i++) {
		flag = true;
		if (predicates[i] != null) {
			for (int j = 0; j < predicates[i].length; j++) {
				if (!predicates[i][j].validate(schema)) {
					flag = false;
					break;
				}
			}
			if (flag == true) {
				tablePredicates.add(predicates[i]);
				predicates[i] = null;
			}
		}
	}
	Predicate[][] result = new Predicate[tablePredicates.size()][];

	for (int i = 0; i < tablePredicates.size(); i++) {
		result[i] = tablePredicates.get(i);
	}
	return result;
  }
  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	p.execute();
	p.close();
	final_itr.close();
	for (Iterator i : scans) {
		i.close();
	}
	if(tmp != null){
		tmp.close();
	}
  } // public void execute()

} // class Select implements Plan
