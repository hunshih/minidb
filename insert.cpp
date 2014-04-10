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
    for(int i = 0; i < attrList.size(); i++)
    {
    	if(strcmp(attrList[i].relName, relation) != 0) //MAKE SURE SUCH ATTRIBUTE EXISTS IN RELATION
    	{
    		return ATTRTYPEMISMATCH;
    	}

    	if(attrVar[i].attrValue == NULL) //NO VALUE SPECIFIED FOR ATTR
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
    AttrDesc[] allAttr;
    attrCat->getRelInfogetRelInfo(relation, schemaAttrCnt, allAttr);





    //CREATE HEAPFILE AND INSERT DATA
    Status returnStatus = OK;
    Record insertTarget;
    insertTarget.data = ; //MAYBE DO MEMCPY FROM THE LIST, AND GET THE LENGTH
    Heapfile insertedTuple(relation,returnStatus);
    returnStatus = insertedTuple.insertRecord();

    //UPDATE INDEX
    //NEED DATA AND RID TO INSERT ENTRY, CREATE RID STRUCT
    //for RID, need page and slot number

    return OK;
}
