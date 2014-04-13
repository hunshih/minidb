#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"

/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
  cout << "Algorithm: Indexed NL Join" << endl;

  /* Your solution goes here */
    // create heap file and indexed file for inner relation
    Status index_file_status;
    Index indexed_rel(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen,
                      (Datatype)attrDesc2.attrType, 0, index_file_status);
    if (index_file_status != OK) {
        return index_file_status;
    }
    Status inner_heap_file_status;
    HeapFileScan inner_heap(attrDesc2.relName, inner_heap_file_status);
    if (inner_heap_file_status != OK) {
        return inner_heap_file_status;
    }
    
    // create heap file handle for outer relation
    Status heap_file_status;
    HeapFileScan heap_file_scanner(attrDesc1.relName, heap_file_status);
    if (heap_file_status != OK) {
        return heap_file_status;
    }
    Status heap_start_scan_status = heap_file_scanner.startScan(attrDesc1.attrOffset, attrDesc1.attrLen,
                                                                (Datatype)attrDesc1.attrType, NULL,
                                                                (Operator)-1);
    if (heap_start_scan_status != OK) {
        return heap_start_scan_status;
    }
    
    // create heap file for result
    Status result_status;
    HeapFile result_file(result, result_status);
    if (result_status != OK) {
        return result_status;
    }
    
    Status heap_next_status;
    Record outer_rec;
    RID outer_RID;
    while ((heap_next_status = heap_file_scanner.scanNext(outer_RID, outer_rec)) == OK) {
        indexed_rel.startScan((char *)outer_rec.data+attrDesc1.attrOffset);
        Status index_next_status;
        Record inner_rec;
        RID inner_RID;
        
        while ((index_next_status = indexed_rel.scanNext(inner_RID)) == OK) {
            Status get_rec_status = inner_heap.getRandomRecord(inner_RID, inner_rec);
            if (get_rec_status != OK) {
                return get_rec_status;
            }
            Record output_rec;
            RID output_RID;
            Status join_status = join_project(projCnt, attrDescArray, reclen,
                                              attrDesc1.relName, attrDesc2.relName,
                                              outer_rec, inner_rec, &output_rec);
            if (join_status != OK) {
                return join_status;
            }
            Status insert_status = result_file.insertRecord(output_rec, output_RID);
            if (insert_status != OK) {
                return insert_status;
            }
            delete [] (char *)output_rec.data;
            
        }
        if (index_next_status != NOMORERECS) {
            // cerr << "inner loop error" << endl;
            return index_next_status;
        }
        indexed_rel.endScan();
    }
    if (heap_next_status != FILEEOF) {
        // cerr << "outer loop error" << endl;
        return heap_next_status;
    }

  return OK;
}

