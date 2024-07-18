from bson import ObjectId
from datetime import datetime

import json
import os
import pymongo
import pytz
import urllib.parse


def get_env(env_key):
    env_value = str()
    if env_key in os.environ:
        env_value = os.getenv(env_key)

    # resolve for file assignment
    file_env = os.environ.get(env_key + '_FILE', str())
    if len(file_env) > 0:
        try:
            with open(file_env, 'r') as mysecret:
                data = mysecret.read().replace('\n', str())
                env_value = data
        except:
            pass
    return env_value


# Configure MongoDB database then, connect to it
username = urllib.parse.quote_plus(get_env('MONGO_USER'))
password = urllib.parse.quote_plus(get_env('MONGO_USER_PASSWORD'))
host = urllib.parse.quote_plus(get_env('MONGO_HOST'))
port = urllib.parse.quote_plus(get_env('MONGO_PORT'))
mongoClient = pymongo.MongoClient(
    f'mongodb://{username}:{password}@{host}:{port}/')
mongoDB = mongoClient['copo_mongo']


def process_changes(doc):
    documentID = doc.get('documentKey', dict()).get('_id', ObjectId())
    collection_name = doc.get('ns', dict()).get('coll', str())
    action_type = doc.get('operationType', str())

    # Get initial state of the document i.e before any modification(s) have been done
    fullDocumentBeforeChange = doc.get('fullDocumentBeforeChange', dict())

    # Get document after update(s) has been completed
    fullDocumentAfterChange = doc.get('fullDocument', dict())

    # Get original document when no value has been popped from the document
    fullDocumentAfterChangeImage = fullDocumentAfterChange

    # Format (terminal) display of json document for legibility
    doc_json = doc
    doc_json['documentKey']['_id'] = str(
        doc_json.get('documentKey', dict()).get('_id', str()))

    print(
        f'\nDocument:\n {json.dumps(doc_json, indent=4, sort_keys=True,default=str)}\n')

    # Exclude fields from the 'update_log'
    excluded_fields = [
        'changelog', 'date_modified', 'time_updated', 'updated_by', 'update_type']
    
    time_updated = doc['wallTime']

    truncatedArrays = doc.get(
        'updateDescription', dict()).get('truncatedArrays', list())
    updatedFields = doc.get(
        'updateDescription', dict()).get('updatedFields', dict())
    removedFields = doc.get('updateDescription',
                            dict()).get('removedFields', list())

    sample_id = documentID # aka copo_id
    manifest_id = fullDocumentAfterChange.get('manifest_id', str())
    sample_type = fullDocumentAfterChange.get('sample_type', str())
    rack_or_plate_id = fullDocumentAfterChange.get('RACK_OR_PLATE_ID', str())
    tube_or_well_id = fullDocumentAfterChange.get('TUBE_OR_WELL_ID', str())

    outdatedFields = {
        field: fullDocumentBeforeChange.get(field, str()) for field in updatedFields if field in fullDocumentBeforeChange}

    # Assemble the main information that will be inserted in the 'AuditCollection'
    insert_record = dict()
    insert_record['_id'] = documentID
    insert_record['action'] = action_type
    insert_record['collection_name'] = collection_name
    insert_record['copo_id'] = sample_id
    insert_record['manifest_id'] = manifest_id
    insert_record['sample_type'] = sample_type
    insert_record['RACK_OR_PLATE_ID'] = rack_or_plate_id
    insert_record['TUBE_OR_WELL_ID'] = tube_or_well_id
    
    # Determine if COPO i.e.'system' or COPO user  i.e. 'user' performed the update
    is_changed = False
    if updatedFields and outdatedFields:
        if 'update_type' in updatedFields:
            
            updated_by = fullDocumentAfterChange.get('updated_by', str())
            update_type = fullDocumentAfterChange.get('update_type', str())
            #print(f'\n\'{update_type}\' updated the document!\n')

            if update_type.startswith('tempuser_'):
                update_type = "user"
                fullDocumentAfterChange.update({'update_type': update_type})
                is_changed = True
        else:
            # print(f'\'system\' updated the document!')
            after_update_by = fullDocumentAfterChange.get('updated_by', str())
            after_update_type = fullDocumentAfterChange.get('update_type', str())
            updated_by = 'system'
            update_type = 'system'

            if after_update_by != 'system' or after_update_type != 'system':
                is_changed = True
                fullDocumentAfterChange.update({'update_type': update_type, 'updated_by': updated_by})
            
        # Update the 'updated_by' field and 'date_modified' field in the 'SampleCollection' using the replace_method
        if 'date_modified' in updatedFields:
            #fullDocumentAfterChange.update({'date_modified': time_updated})
            #is_changed = True
            time_updated = fullDocumentAfterChange.get('date_modified', time_updated)

        if is_changed:
            '''
             NB: The  'replace_one' method is used to replace the entire document in the 'SampleCollection' with the initial document but with modified fields
             instead of the 'update_one' method which updates the specified fields document in the 'SampleCollection'
             This is done to ensure that the update action is not performed since the watch/ChangeStream on the 'SampleCollection'
             considers the last update performed on the collection as the current state of the document
             i.e. if the 'SampleCollection' is updated with the 'system' information, this 'overwrites' the prior update to the fields in the collection
            '''
            
            # Replace the document in the 'SampleCollection'
            mongoDB['SampleCollection'].replace_one(
                    fullDocumentAfterChangeImage, fullDocumentAfterChange)
     
        # Create an 'update_log' dictionary
        output = list()

        for field in updatedFields:
            if field in excluded_fields:
                # Skip fields that are not required in the 'update_log' and go to the next iteration
                pass 
            elif outdatedFields.get(field, str()) == updatedFields.get(field, str()):
                # Do not record fields that have the same outdated and updated values
                pass 
            else:
                update_log = dict()
                update_log['field'] = field
                update_log['outdated_value'] = outdatedFields.get(
                    field, str())
                update_log['updated_value'] = updatedFields.get(
                    field, str())

                update_log['updated_by'] = updated_by
                    
                update_log['update_type'] = update_type
                update_log['time_updated'] = time_updated
                
                output.append(update_log)
                
        # Finds document and performs update
        # If the document is not found, the document is 
        # created with the 'update_log' details
        mongoDB['AuditCollection'].find_one_and_update(
                {'_id': documentID}, {'$push': {'update_log': {'$each': output}}, "$setOnInsert": insert_record}, upsert=True)
        
    # Record fields that have been removed from the document
    if removedFields:
        output = list()

        for field in removedFields:
            removal_log = dict()
            removal_log['field'] = field
            removal_log['removal_type'] = 'system'
            removal_log['time_removed'] = time_updated

            output.append(removal_log)

        # Update the log of removed fields in the collection by 
        # finding the document and performing the update
        # If the document is not found, the document 
        # is created with the details
        mongoDB['AuditCollection'].find_one_and_update(
            {'_id': documentID}, {'$push': {'removal_log': {'$each': output}},"$setOnInsert": insert_record}, upsert=True)

    # Record fields have been truncated in the document
    if truncatedArrays:
        output = list()

        for element in truncatedArrays:
            truncated_log = dict()
            truncated_log['field'] = element.get('field', str())
            truncated_log['newSize'] = element.get('newSize', int())
            truncated_log['truncated_type'] = 'system'
            truncated_log['time_truncated'] = time_updated

            output.append(truncated_log)

        # Update the log of truncated fields in the collection by 
        # finding the document and performing the update
        # If the document is not found, the document 
        # is created with the details
        mongoDB['AuditCollection'].find_one_and_update(
            {'_id': documentID}, {'$push': {'truncated_log': {'$each': output}},"$setOnInsert": insert_record}, upsert=True)


# Record updates whenever an update is performed on a collection
# NB: Currently, 'ChangeStream' is set on 'SampleCollection' with 'update' operation type
try:
    resume_token = None
    # NB: Other operation types are: 'insert', 'replace'
    pipeline = [{'$match': {'operationType': 'update'}}]

    with mongoDB.SampleCollection.watch(pipeline=pipeline,
                                        full_document_before_change='whenAvailable',
                                        full_document='whenAvailable'
                                        ) as stream:
        for update in stream:
            process_changes(update)
            resume_token = stream.resume_token

except pymongo.errors.PyMongoError as e:
    # The 'ChangeStream' encountered an unrecoverable error or the
    # resume attempt failed to recreate the cursor.
    print('Exception:', e)
    if resume_token is None:
        # There is no usable resume token because there was a
        # failure during the ChangeStream initialisation.
        print('\nMessage: There was a failure during the ChangeStream initialisation')
    else:
        # Use the interrupted ChangeStream's resume token to create
        # a new ChangeStream. The new stream will continue from the
        # last seen insert change without missing any events.
        print('\nMessage: ChangeStream is resuming from last seen insert change')
        with mongoDB.SampleCollection.watch(pipeline=pipeline,
                                            full_document_before_change='whenAvailable',
                                            full_document='whenAvailable',
                                            resume_after=resume_token) as stream:
            for update in stream:
                process_changes(update)
