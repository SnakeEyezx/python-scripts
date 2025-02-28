import logging, sys, os, glob, time
import openai
import httpx
from datetime import datetime
import concurrent.futures as pool

logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

api_key = os.environ.get('SOME_API_KEY')
project_id = 'proj-id'
org_id = 'org-id'

# Init check
if all((api_key, assistant_id)):
    proxy = "socks5://proxy.org:8080"
    client = openai.OpenAI(http_client=httpx.Client(proxy=proxy),
                           api_key=api_key,
                           project=project_id,
                           organization=org_id)
    file_paths = glob.glob('some-files/*')
else:
    logging.error('Itin failed: API_KEY or ASSISTANT_ID is not set.')
    sys.exit(1)

def _create_file(file_name):
    retrys = 5
    while True:
        try:
            result = client.files.create(file=open(file_name, 'rb'), purpose='assistants', timeout=3)
        except Exception as e:
            logging.error(e)
            logging.error(f'Create file error. File name: {file_name}')
        else:
            logging.info(f'Success. File name: {file_name} successfuly created.')
            if result.created_at:
                return {"file_name": result.filename, "file_id": result.id}
            elif retrys == 0:
                logging.error(f'Failed create file retrys limit exceeded.')
                return False
            else:
                time.sleep(1)
                retrys -= 1
                logging.warning(f'Failed create file retrys left: {retrys}')

def _delete_file(file_id):
    retrys = 5
    while True:
        try:
            result = client.files.delete(file_id, timeout=3)
        except Exception as e:
            logging.error(e)
            logging.error(f'Delete file error, id: {file_id}')
        else:
            logging.info(f'Success delete file, id: {file_id}.')
            if result.deleted:
                return True
            elif retrys == 0:
                logging.error(f'Failed delete file, retrys limit exceeded.')
                return False
            else:
                time.sleep(1)
                retrys -= 1
                logging.warning(f'Failed delete file, retrys left: {retrys}')

def _attach_file_to_vs(vs_id, file_id):
    # Attach file to vector_store
    retrys = 5
    while True:
        try:
            result = client.beta.vector_stores.files.create(vs_id, file_id=file_id, timeout=3)
        except Exception as e:
            logging.error(e)
            logging.error(f'Attach file error, id: {file_id}')
        else:
            while result.status == 'in_progress':
                time.sleep(1)
                result = client.beta.vector_stores.files.poll(file_id=result.id, vector_store_id=vs_id)
            if result.status == 'completed':
                logging.info(f'Success attach file, id: {file_id}.')
                return True
            elif retrys == 0:
                logging.error(f'Failed attach file, retrys limit exceeded.')
                return False
            else:
                time.sleep(1)
                retrys -= 1
                logging.warning(f'Failed attach file, retrys left: {retrys}')

def _create_vs():
    vs_name = 'AiDatabase' + datetime.now().strftime('%Y-%m-%d-%H-%M')
    retrys = 5
    while True:
        try:
            result = client.beta.vector_stores.create(name=vs_name, timeout=3)
        except Exception as e:
            logging.error(e)
            logging.error(f'Attach file error, id: {vs_name}')
        else:
            logging.info(f'Success attach file, id: {vs_name}.')
            if result.status == 'completed':
                return result
            elif retrys == 0:
                logging.error(f'Failed attach file, retrys limit exceeded.')
                return False
            else:
                time.sleep(1)
                retrys -= 1
                logging.warning(f'Failed attach file, retrys left: {retrys}')

def _delete_vs(vs_id):
    retrys = 5
    while True:
        try:
            result = client.beta.vector_stores.delete(vs_id, timeout=5)
        except Exception as e:
            logging.error(e)
            logging.error(f'Delete vector store error, id: {vs_id}')
        else:
            logging.info(f'Success delete vector store, id: {vs_id}.')
            if result.deleted:
                return True
            elif retrys == 0:
                logging.error(f'Failed delete vector store, retrys limit exceeded.')
                return False
            else:
                time.sleep(1)
                retrys -= 1
                logging.warning(f'Failed delete vector store, retrys left: {retrys}')

def upload_files():
    uploaded_files = []
    file_batches = [file_paths[i:i+50] for i in range(0, len(file_paths), 50)]
    for b in file_batches:
        with pool.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_create_file, s) for s in b]
            for future in pool.as_completed(futures, timeout=120):
                if future.result():
                    uploaded_files.append(future.result())
        time.sleep(5)
    return uploaded_files

def attach_files(uploaded_files):
    attached_files = []
    attach_batches = [uploaded_files[i:i+50] for i in range(0, len(uploaded_files), 50)]
    logging.info('Attaching files to vectore store..')
    for b in attach_batches:
        with pool.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_attach_file_to_vs, new_vs_store.id, s['file_id']) for s in b]
            for future in pool.as_completed(futures, timeout=120):
                if future.result():
                    attached_files.append(future.result())
        time.sleep(5)
    return attached_files

def fail_clear(vs_id, uploaded_files):
    file_batches = [uploaded_files[i:i+50] for i in range(0, len(uploaded_files), 50)]
    for b in file_batches:
        with pool.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_delete_file, s['file_id']) for s in b]
            for future in pool.as_completed(futures, timeout=120):
                if not future.result():
                    logging.error(f'Failed to delete file..')
        time.sleep(5)
    del_vs = _delete_vs(vs_id)
    if not del_vs:
        logging.error(f'Failed delete vectore store{vs_id}')
    sys.exit(1)



if __name__ == '__main__':
    logging.info(f'Files to upload: {len(file_paths)}')
    new_vs_store = _create_vs()
    uploaded_files = upload_files()
    print('Uploaded files count', len(uploaded_files))
    if len(uploaded_files) == len(file_paths):
        logging.info('Upload success..')
        attached_files = attach_files(uploaded_files)
        if len(attached_files) == len(file_paths):
            logging.info('Attaching successed..')
        else:
            fail_clear(new_vs_store.id, uploaded_files)
    else:
        fail_clear(new_vs_store.id, uploaded_files)
