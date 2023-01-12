## S3StreamingTransfer
A simple class to allow fileobj like streaming uploads to S3 buckets without needing to write to disk.

### Example Usage

Simple usage without cached writes:

```python3
import boto3
import mimetypes
import os
import threading

from boto3.s3.transfer import TransferConfig
from s3stream import S3StreamingObject


def main():
    upload_item = f'/path/to/local_or_networked_file_or_FTP_location'
    file_size = os.stat(upload_item).st_size  # Or whatever is appropriate
    fileobj = S3StreamingObject(file_size)
    c_type, _ = mimetypes.guess_type(upload_item)
    if c_type is None:
        c_type = 'application/octet-stream'
    extra_args = {
        'ACL': 'public-read',
        'ContentType': c_type
    }
    t = threading.Thread(
        target=process,
        args=(file_obj, extra_args),
        daemon=True
    )
    t.start()
   
    for chunk in simulated_network_iterator(upload_item):
        file_obj.write(chunk)
   
   
def simulated_network_iterator(fname):
    with open(fname, 'rb') as f:
        for chunk in f:
            yield chunk
   
   
def process(file_obj, extra_args=None):
    client = boto3.client('s3')
    config = TransferConfig(use_threads=False)
    client.upload_fileobj(
        file_obj,
        'BUCKET_NAME',  # bucket
        'OBJECT_NAME',  # key or filename
        ExtraArgs=extra_args,
        Callback=fileobj.prune
        Config=config
    )
```

In almost all situations, the data sent to the `file_obj` will outpace the data read from it and processed by `boto3`. In these cases, enhanced generators with buffered writes really shine:

```python3
def main():
    upload_item = f'/path/to/local_or_networked_file_or_FTP_location'
    file_size = os.stat(upload_item).st_size  # Or whatever is appropriate
    processed = 0
    upload = upload_generator(upload_item)
    upload.send(None)
    for chunk in simulated_network_iterator(upload_item):
        if written_amount := upload.send(chunk):
            processed += written_amount
            print(f'Uploaded: {processed}bytes ({processed/file_size:.%})', end='', flush=True)
    written_amount = upload.send(None)
    processed += written_amount
    print(f'Uploaded: {processed} ({processed/file_size:.%})', flush=True)
    upload.close()
  
def process(upload_item, callback=None):
    file_size = os.stat(upload_item).st_size  # Or whatever is appropriate
    file_obj = S3StreamingObject(file_size)
    c_type, _ = mimetypes.guess_type(upload_item)
    if c_type is None:
        c_type = 'application/octet-stream'
    extra_args = {
        'ACL': 'public-read',
        'ContentType': c_type
    }
    t = threading.Thread(
        target=__process,
        args=(file_obj, extra_args),
        daemon=True
    )
    t.start()
    write_size = 4096
    min_write_size = write_size
    max_write_size = write_size * 20
    written_amount = 0
    data = b''
    try:
        while True:
            chunk = yield written_amount
            if chunk is None:
                written_amount = file_obj.write(data)
                file_obj.close()
                t.join()
                break
            data += chunk
            if len(data) >= min_write_size:
                written_amount = file_obj.write(data)
                data = b''
                if min_write_size < max_write_size:
                    min_write_size += write_size
            else:
                written_amount = 0
    except GeneratorExit as e:
        # Prematurely closed
        # Do any necessary cleaups
        raise e   # Or suppress
    else:
        if callback: callback()
    finally:
        return written_amount
      
def __process(file_obj, extra_args):
    client = boto3.client('s3')
    config = TransferConfig(use_threads=False)
    client.upload_fileobj(
        file_obj,
        'BUCKET_NAME',  # bucket
        'OBJECT_NAME',  # key or filename
        ExtraArgs=extra_args,
        Callback=fileobj.prune
        Config=config
    )
```
By using an enhanced generator, you can take advantage of a pre-write cache to cut down on needless calls to the `file_obj`'s `.write` method which requires capturing and release a `Lock()` each time. Yielding back the amount written by the `boto3` `client` will also help the sender track how much data has actually been processed.
