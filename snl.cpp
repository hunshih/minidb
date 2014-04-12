#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: Simple NL Join" << endl;

  /* Your solution goes here */
    // variables for the output relation
    Status output_constr_status;
    HeapFile output(result, output_constr_status);
    if (output_constr_status != OK) {
        return output_constr_status;
    }
    
    // outer scan on the right relation
    Status scan_right_constr_status;
    HeapFileScan scan_right(attrDesc2.relName, scan_right_constr_status);
    if (scan_right_constr_status != OK) {
        return scan_right_constr_status;
    }
    Status scan_right_start_status = scan_right.startScan(attrDesc2.attrOffset, attrDesc2.attrLen,
                                                          (Datatype)attrDesc2.attrType, NULL,
                                                          (Operator)-1);
    if (scan_right_start_status != OK) {
        return scan_right_start_status;
    }

    // variables for current record from the right relation that we are doing an inner scan on
    Record cur_record;
    RID cur_record_ID;
    Status scan_right_status;
    // variables for the record that goes into the result relation
    Record result_rec;
    RID result_RID;
    while ((scan_right_status = scan_right.scanNext(cur_record_ID, cur_record)) == OK) {
        char *filter = (char *)cur_record.data+attrDesc2.attrOffset;
        // filtered inner scan on the left relation
        Status scan_left_constr_status;
        HeapFileScan scan_left(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen,
                               (Datatype)attrDesc1.attrType, filter, op, scan_left_constr_status);
        if (scan_left_constr_status != OK) {
            return scan_left_constr_status;
        }
        Status scan_left_status;
        while ((scan_left_status=scan_left.scanNext(result_RID, result_rec)) == OK) {
            // create the output record
            RID output_RID;
            Record output_record;
            Status join_status = join_project(projCnt, attrDescArray, reclen,
                                              attrDesc1.relName, attrDesc2.relName, result_rec,
                                              cur_record, &output_record);
            Status output_status = output.insertRecord(output_record, output_RID);
            delete [](char *)output_record.data;
            if (output_status != OK) {
                return output_status;
            }
        }
        // end of inner scan
        if (scan_left_status != FILEEOF) {
            return scan_left_status;
        }
        
    }
    // end of outer scan
    if (scan_right_status != FILEEOF) {
        return scan_right_status;
    }
    

  return OK;
}

