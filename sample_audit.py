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
    documentID = doc.get('documentKey', dict()).get('_id', str())
    collection_name = doc.get('ns', dict()).get('coll', str())
    action_type = doc.get('operationType', str())
    datetime_default_value = datetime.min

    # Get initial state of the document i.e before any modification(s) have been done
    fullDocumentBeforeChange = doc.get('fullDocumentBeforeChange', dict())

    # Get document after update(s) has been completed
    fullDocumentAfterChange = doc.get('fullDocument', dict())

    # Convert 'wallTime' datetime object to GB timezone
    # If datetime value does not exist, set a default datetime value
    doc['clusterTime'] = doc.get('clusterTime', datetime_default_value).as_datetime(
    ).astimezone(pytz.timezone('Europe/London')).replace(microsecond=0) or datetime_default_value

    doc['wallTime'] = pytz.timezone('UTC').localize(doc.get('wallTime', datetime_default_value)).astimezone(
        pytz.timezone('Europe/London')).replace(microsecond=0) or datetime_default_value

    # Format (terminal) display of json document for legibility
    # datetime_format = '%y-%m-%d %H:%M:%S'
    # doc_json = doc
    # doc_json['documentKey']['_id'] = str(
    #     doc_json.get('documentKey', dict()).get('_id', str()))
    # doc_json['clusterTime'] = doc_json.get('clusterTime', datetime_default_value).strftime(
    #     datetime_format) or datetime_default_value.strftime(datetime_format)
    # doc_json['wallTime'] = doc_json.get(
    #     'wallTime', datetime_default_value).strftime(datetime_format) or datetime_default_value.strftime(datetime_format)

    # print(
    #     f'\nDocument:\n {json.dumps(doc_json, indent=4, sort_keys=True,default=str)}\n')

    # Exclude fields from the 'update_log'
    excluded_fields = [
        'changelog', 'date_modified', 'time_updated', 'updated_by', 'update_type']
    time_updated = doc['wallTime']
    # datetime.strptime(
    #     doc['wallTime'], datetime_format) if type(doc['wallTime']) == str() else doc['wallTime']
    truncatedArrays = doc.get(
        'updateDescription', dict()).get('truncatedArrays', list())
    updatedFields = doc.get(
        'updateDescription', dict()).get('updatedFields', dict())
    removedFields = doc.get('updateDescription',
                            dict()).get('removedFields', list())

    profile_id = fullDocumentAfterChange.get('profile_id', str())
    sample_id = documentID
    manifest_id = fullDocumentAfterChange.get('manifest_id', str())
    sample_type = fullDocumentAfterChange.get('sample_type', str())

    outdatedFields = {
        field: fullDocumentBeforeChange.get(field, str()) for field in updatedFields if field in fullDocumentBeforeChange}

    # Assemble the main information/filter for the 'AuditCollection'
    filter = dict()
    filter['_id'] = documentID
    filter['collection_name'] = collection_name
    filter['action'] = action_type
    filter['manifest_id'] = manifest_id
    filter['profile_id'] = profile_id
    filter['sample_id'] = sample_id
    filter['sample_type'] = sample_type

    # Determine if COPO i.e.'system' or COPO user  i.e. 'user' performed the update
    if updatedFields and outdatedFields:
        if 'date_modified' in updatedFields or 'time_updated' in updatedFields and fullDocumentAfterChange.get('update_type', str()) == 'user':
            print(f'\n\'user\' updated the document!\n')

            updated_by = fullDocumentAfterChange.get('updated_by', str())
            update_type = fullDocumentAfterChange.get('update_type', str())
        else:
            '''
             NB: The  'replace_one' method is used to replace the entire document in the 'SampleCollection' with the initial document but with modified fields
             instead of the 'update_one' method which updates the specified fields document in the 'SampleCollection'
             This is done to ensure that the update action is not performed since the watch/ChangeStream on the 'SampleCollection'
             considers the last update performed on the collection as the current state of the document
             i.e. if the 'SampleCollection' is updated with the 'system' information, this 'overwrites' the prior update to the fields in the collection
            '''

            print(f'\'system\' updated the document!')

            updated_by = 'ei.copo@earlham.ac.uk'
            update_type = 'system'

            # Update the 'updated_by' field and 'date_modified' field in the 'SampleCollection' using the replace_method
            if 'date_modified' in fullDocumentAfterChange:
                fullDocumentAfterChange.pop('date_modified')

            if 'time_updated' in fullDocumentAfterChange:
                fullDocumentAfterChange.pop('time_updated')

            if 'updated_by' in fullDocumentAfterChange:
                fullDocumentAfterChange.pop('updated_by')

            if 'update_type' in fullDocumentAfterChange:
                fullDocumentAfterChange.pop('update_type')

            # Replace document in the 'SampleCollection'
            replace_filter = fullDocumentAfterChange

            # Merge dictionaries
            replacement = replace_filter | {
                'date_modified': time_updated, 'time_updated': time_updated, 'updated_by': updated_by, 'update_type': update_type}

            mongoDB['SampleCollection'].replace_one(
                replace_filter, replacement)

        # Create an 'update_log' dictionary
        update_log = dict()

        for field in updatedFields:
            if field in excluded_fields or 'changelog' in field:
                continue
            else:
                update_log['field'] = field
                update_log['outdated_value'] = outdatedFields.get(
                    field, str())
                update_log['updated_value'] = updatedFields.get(
                    field, str())
                update_log['updated_by'] = updated_by
                update_log['update_type'] = update_type
                update_log['time_updated'] = time_updated

        # Merge dictionaries
        update_filter = filter | {'update_log': {'$exists': True}}

        # If the number of documents in the 'AuditCollection' that match the filter is 0,
        # insert a document into the 'AuditCollection' based on the filter criteria
        # and set 'update_log' to an empty list
        if mongoDB['AuditCollection'].count_documents(
                update_filter, limit=1, maxTimeMS=1000) == 0:
            mongoDB['AuditCollection'].update_one(
                filter, {'$set': {'update_log': list()}}, upsert=True)

        # Populate the 'update_log'
        mongoDB['AuditCollection'].update_one(
            {'_id': documentID}, {'$push': {'update_log': update_log}})

    # Record fields that have been removed from the document
    if removedFields:
        # Merge dictionaries
        removal_filter = filter | {'removedFields': {'$exists': True}}
        removal_log = dict()

        if not mongoDB['AuditCollection'].find(removal_filter):
            mongoDB['AuditCollection'].update_one(
                removal_filter, {'$set': {'removedFields': list()}}, upsert=True)

        for field in removedFields:
            removal_log['field'] = field
            removal_log['removed_by'] = 'ei.copo@earlham@ac.uk'
            removal_log['removal_type'] = 'system'
            removal_log['time_removed'] = time_updated

        # Update the log of removed fields in the collection
        mongoDB['AuditCollection'].update_one(
            {'_id': documentID}, {'$push': {'removal_log': removal_log}})

    # Record fields have been truncated in the document
    if truncatedArrays:
        # Merge dictionaries
        truncated_filter = filter | {'truncatedArrays': {'$exists': True}}
        truncated_log = dict()

        if not mongoDB['AuditCollection'].find(truncated_filter):
            mongoDB['AuditCollection'].update_one(
                truncated_filter, {'$set': {'truncatedArrays': list()}}, upsert=True)

        for element in truncatedArrays:
            truncated_log['field'] = element.get('field', str())
            truncated_log['newSize'] = element.get('newSize', int())
            truncated_log['truncated_by'] = 'ei.copo@earlham@ac.uk'
            truncated_log['truncated_type'] = 'system'
            truncated_log['time_truncated'] = time_updated

        # Update the log of removed fields in the collection
        mongoDB['AuditCollection'].update_one(
            {'_id': documentID}, {'$push': {'truncated_log': truncated_log}})


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
