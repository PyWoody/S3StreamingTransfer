## S3StreamingTransfer
A simple class to allow fileobj like streaming uploads to S3 buckets without needing to write to disk.

### Example Usage

Simplue usage without cached writes:

```
import boto3
import mimetypes
import os
import threading

from boto3.s3.transfer import TransferConfig
from s3stream import S3StreamingObject


def main():
  upload_item = f'/path/to/local_or_networked_file_or_FTP_location'
  fileobj = S3StreamingObject(os.stat(upload_item).st_size)
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
'''
