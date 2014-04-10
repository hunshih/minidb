#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
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

    //DECLARE A MAP THAT KEEP TRACKS OF OFFSET AND ATTRINFO
    map<int,attrInfo> indexMap;

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
    for(int aryCount = 0; aryCount < schemaAttrCnt; aryCount++, allAttr++)
    {
        for(int i = 0; i < attrCnt; i++)
        {
            if(allAttr->attrName == attrList[i].attrName)
            {
                memcpy ( ((char*)newTuple.data) + offset, attrList[i].attrValue, attrList[i].attrLen );
                if(allAttr->indexed) //IF INDEXED
                {
                    indexMap.insert(pair<int,attrInfo>(offset ,attrList[i]));
                }
                attrSize = attrList[i].attrLen;
                offset += attrSize;
            }
            if(attrList[i].attrValue == NULL) //NO VALUE SPECIFIED FOR ATTR
            {
                return ATTRNOTFOUND;
            }
        }
    }
    newTuple.length = offset;

    //CREATE HEAPFILE AND INSERT DATA
    Status returnStatus = OK;
    RID tupleID;
    HeapFile insertedTuple(relation,returnStatus);
    returnStatus = insertedTuple.insertRecord(newTuple, tupleID);

    //UPDATE INDEX (FOR ALL ATTR)
    for(map<int,attrInfo>::iterator it = indexMap.begin() ; it != indexMap.end(); ++it)
    {
        Index updateIndex(relation, it->first, it->second.attrLen, (Datatype)it->second.attrType, 0, returnStatus); 
        updateIndex.insertEntry(it->second.attrValue, tupleID);
    }

    return OK;
}
