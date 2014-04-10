#include "catalog.h"
#include "query.h"
#include "index.h"
#include <vector>
/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */

    //DEFINE NEW STRUCT FOR UPDATING INDEX
    struct IndexedAttr{
        attrInfo Info;
        int offset;
    };

    //CREATE RECORD OBJECT
    Record newTuple;

    //LOOK UP SYSTEM CATALOG FOR RELATION AND ATTR
    RelDesc schema;
    relCat->getInfo(relation, schema);
    if(attrCnt != schema.attrCnt) return ATTRTYPEMISMATCH; //NUMBER OF ATTRIBUTE MISMATCH
    int schemaAttrCnt = 0;
    AttrDesc* allAttr = new AttrDesc;
    attrCat->getRelInfo(relation, schemaAttrCnt, allAttr);

    //USE MEMCPY TO COPY DATA
    int offset = 0, attrSize = 0;
    vector<IndexedAttr> indexList;
    for(int aryCount = 0; aryCount < schemaAttrCnt; aryCount++, allAttr++)
    {
        for(int i = 0; i < attrCnt; i++)
        {
            if(allAttr->attrName == attrList[i].attrName)
            {
                memcpy ( newTuple.data + offset, attrList[i].attrValue, attrList[i].attrLen );
                if(allAttr->indexed) //IF INDEXED
                {
                    IndexedAttr insertIndex;
                    insertIndex.Info = attrList[i];
                    insertIndex.offset = offset;
                    indexList.pushback(insertIndex);
                }
                offset += attrList[i].attrLen;
                attrSize += attrList[i].attrLen;
            }
            if(attrList[i].attrValue == NULL) //NO VALUE SPECIFIED FOR ATTR
            {
                return ATTRNOTFOUND;
            }
        }
    }
    newTuple.length = attrSize;

    //CREATE HEAPFILE AND INSERT DATA
    Status returnStatus = OK;
    RID tupleID;
    HeapFile insertedTuple(relation,returnStatus);
    returnStatus = insertedTuple.insertRecord(newTuple, tupleID);

    //UPDATE INDEX (FOR ALL ATTR)
    for(vector<IndexedAttr>::iterator it = indexList.begin(); it != indexList.end(); ++it)
    {
        Index updateIndex(relation, it->info.offset, it->info.attrLen, it->info.attrType, 0, returnStatus); 
        updateIndex.insertEntry(it->info.attrValue, tupleID);
    }

    return OK;
}
