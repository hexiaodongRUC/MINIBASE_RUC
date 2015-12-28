package query;

import parser.AST_CreateIndex;
import relop.Schema;
import global.Minibase;
import index.HashIndex;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

  /** Name of the index to create. */
  protected String fileName;

  /** Name of the table to index. */
  protected String ixTable;

  /** Name of the column to index. */
  protected String ixColumn;
  
  protected Schema schema;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
	  fileName = tree.getFileName();
	  QueryCheck.fileNotExists(fileName);
	  
	  ixTable = tree.getIxTable();
	  schema = QueryCheck.tableExists(ixTable);
	  
	  ixColumn = tree.getIxColumn();
	  QueryCheck.columnExists(schema, ixColumn);
  
  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    // print the output message
    new HashIndex(fileName);
    
    Minibase.SystemCatalog.createIndex(fileName, ixTable, ixColumn);
	System.out.println("index created!");

  } // public void execute()

} // class CreateIndex implements Plan
