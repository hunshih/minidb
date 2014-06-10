#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"

/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: SM Join" << endl;

  /* Your solution goes here */
    int num_unpinned_pages = bufMgr->numUnpinnedPages();
    // calculate tuple size for both relations
    int rel1_tuple_size, rel2_tuple_size;
    Status rel1_tuple_size_status = get_rel_tuple_size(attrDesc1.relName, &rel1_tuple_size);
    if (rel1_tuple_size_status != OK) {
        return rel1_tuple_size_status;
    }
    Status rel2_tuple_size_status = get_rel_tuple_size(attrDesc2.relName, &rel2_tuple_size);
    if (rel2_tuple_size_status) {
        return rel2_tuple_size_status;
    }
    int max_rel1_tuples = num_unpinned_pages*0.8*PAGESIZE/rel1_tuple_size;
    int max_rel2_tuples = num_unpinned_pages*0.8*PAGESIZE/rel2_tuple_size;
    
    // sort
    Status rel1_sort_status, rel2_sort_status;
    SortedFile sorted_rel1(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen,
                           (Datatype)attrDesc1.attrType, max_rel1_tuples, rel1_sort_status);
    SortedFile sorted_rel2(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen,
                           (Datatype)attrDesc2.attrType, max_rel2_tuples, rel2_sort_status);
    // merge
    Status result_file_status;
    HeapFile result_file(result, result_file_status);
    if (result_file_status != OK) {
        return result_file_status;
    }
    Record rel1_cur_rec, rel2_cur_rec;
    Status rel1_next_status = sorted_rel1.next(rel1_cur_rec);
    Status rel2_next_status = sorted_rel2.next(rel2_cur_rec);
    while (rel1_next_status == OK && rel2_next_status == OK) {
        int match_result = matchRec(rel1_cur_rec, rel2_cur_rec, attrDesc1, attrDesc2);
        if (match_result < 0) {
            rel1_next_status = sorted_rel1.next(rel1_cur_rec);
        }
        else if (match_result > 0) {
            rel2_next_status = sorted_rel2.next(rel2_cur_rec);
        }
        else {
            Status mark_status = sorted_rel2.setMark();
            if (mark_status != OK) {
                return mark_status;
            }
            while (!match_result) {
                Record output_rec;
                RID output_RID;
                Status join_status = join_project(projCnt, attrDescArray, reclen,
                                                  attrDesc1.relName, attrDesc2.relName,
                                                  rel1_cur_rec, rel2_cur_rec, &output_rec);
                if (join_status != OK) {
                    return join_status;
                }
                Status insert_status = result_file.insertRecord(output_rec, output_RID);
                if (insert_status != OK) {
                    return insert_status;
                }
                delete [] (char *)output_rec.data;
                
                // move the pointers, update
                rel2_next_status = sorted_rel2.next(rel2_cur_rec);
                match_result = matchRec(rel1_cur_rec, rel2_cur_rec, attrDesc1, attrDesc2);
                
            }
            sorted_rel2.gotoMark();
            rel1_next_status = sorted_rel1.next(rel1_cur_rec);
            rel2_next_status = sorted_rel2.next(rel2_cur_rec);
        }
    }
    
    

  return OK;
}

Status Operators::get_rel_tuple_size(const string &relname, int *result)
{
    AttrDesc *relattr;
    int relcount;
    Status rel_get_attr_status = attrCat->getRelInfo(relname, relcount, relattr);
    if (rel_get_attr_status != OK) {
        return rel_get_attr_status;
    }
    *result = relattr[relcount-1].attrOffset + relattr[relcount-1].attrLen;

    delete [] relattr;
    return OK;
}

