#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>

Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
    cout << "Algorithm: Index Select" << endl;

    /* Your solution goes here */
    Status returnStatus;

    //CREATE A RESULT HEAPFILE
    HeapFile resultFile(result, returnStatus);
    if(returnStatus != OK) return returnStatus;
    HeapFileScan scanFile(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, (char*)attrValue, op, returnStatus);
    if(returnStatus != OK) return returnStatus;
    Index indexFile(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, 0, returnStatus);
    if(returnStatus != OK) return returnStatus;

    RID resultID;
    Record resultRecord;
    
    //FINALTUPLE IS THE RECORD BEING INSERTED TO THE RESULT RELATION
    Record finalTuple;
    finalTuple.data = new char[reclen];
    finalTuple.length = reclen;

    returnStatus = indexFile.startScan(attrValue);
    if(returnStatus != OK) return returnStatus;

    while(returnStatus == OK)
    {
    
        returnStatus = indexFile.scanNext(resultID);
        if(returnStatus != OK) break;

        returnStatus = scanFile.getRandomRecord(resultID, resultRecord);
        if(returnStatus != OK) break;

        //COPY THE DESIRE ATTRS FROM RECORD
        for(int i = 0, offset = 0; i < projCnt; i++)
        {
            memcpy((char*)finalTuple.data + offset, (char*)resultRecord.data + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }
        returnStatus = resultFile.insertRecord(finalTuple, resultID);
        
    }
    delete [] finalTuple.data;
    returnStatus = scanFile.endScan();
    if(returnStatus != OK) return returnStatus;
    returnStatus = indexFile.endScan();
    if(returnStatus != OK) return returnStatus;
    return OK;
}

