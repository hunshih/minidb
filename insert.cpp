#include "catalog.h"
#include "query.h"
#include "index.h"

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

    //CHECK IF THE LIST OF ATTR IS VALID
    for(int i = 0; i < attrCnt; i++)
    {
    	if(std::strcmp(attrList[i].relName, relation) != 0) //MAKE SURE SUCH ATTRIBUTE EXISTS IN RELATION
    	{
    		return ATTRTYPEMISMATCH;
    	}

    	if(attrList[i].attrValue == NULL) //NO VALUE SPECIFIED FOR ATTR
    	{
    		return ATTRNOTFOUND;
    	}
    }

    //CREATE RECORD OBJECT
    Record newTuple;

    //LOOK UP SYSTEM CATALOG FOR RELATION AND ATTR
    RelDesc schema;
    relCat->getInfo(relation, schema);
    if(attrCnt != schema.attrCnt) return ATTRTYPEMISMATCH; //NUMBER OF ATTRIBUTE MISMATCH
    int schemaAttrCnt = 0;
    AttrDesc* allAttr = new AttrDesc;
    attrCat->getRelInfo(relation, schemaAttrCnt, allAttr);





    //CREATE HEAPFILE AND INSERT DATA
    Status returnStatus = OK;
    RID tupleID;
    //MAYBE DO MEMCPY FROM THE LIST, AND GET THE LENGTH
    HeapFile insertedTuple(relation,returnStatus);
    returnStatus = insertedTuple.insertRecord(newTuple, tupleID);

    //UPDATE INDEX
    //NEED DATA AND RID TO INSERT ENTRY, CREATE RID STRUCT
    //for RID, need page and slot number

    return OK;
}
