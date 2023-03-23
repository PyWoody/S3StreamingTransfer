import io
import threading


class BaseS3StreamingObject(io.BytesIO):

    def __init__(self, file_size, buffer_size=None):
        if buffer_size is None:
            buffer_size = 8 * (1024 * 1024)
        self.buffer_size = buffer_size
        self.file_size = file_size
        self.processed = 0
        self.seek_pos = 0
        self.data = b''
        self.__closed = False
        self.__error = None
        self.lock = threading.Lock()
        self.can_write_event = threading.Event()
        self.can_write_event.set()
        self.can_read_event = threading.Event()

    def __repr__(self):
        return (f'{self.__class__.__name__}'
                f'({self.file_size}, buffer_size={self.buffer_size})')

    def __iter__(self):
        while data := self.read(self.buffer_size):
            yield data
        self.close()

    @property
    def error(self):
        """
        The file objects error property

        :rtype: Exception, None
        :return: Returns an Excepion if set; else, None
        """
        return self.__error

    @error.setter
    def error(self, err):
        """
        Setter for the self.error value. Must be an Exception.

        :type err: Exception
        :param err: The exception to set

        :rtype: boolean
        :return: Returns True if err is an Exception; else, False
        """
        if isinstance(err, Exception):
            self.__error = err
            return True
        return False

    @property
    def closed(self):
        """
        The file objects closed property

        :rtype: boolean
        :return: Returns True if closed; else False
        """
        return self.__closed

    def close(self):
        """
        Sets the file object to closed. Can be called multiple times
        """
        with self.lock:
            self.__closed = True
            self.can_read_event.set()  # Release waiting reads

    def tell(self, *args, **kwargs):
        """
        Returns the current seek position
        """
        with self.lock:
            return self.seek_pos

    def read(self, *args, **kwargs):
        raise NotImplementedError('Must create read() in a subclass')

    def write(self, chunk, *args, **kwargs):
        """
        Writes the new data, chunk, to the internal data object.
        Will reset the can_write_event if data is larger than buffer_size

        :type chunk: bytes
        :param chunk: The bytes to write the end of the data object

        :rtype: int
        :return: Returns the amount of bytes written
        """
        self.can_write_event.wait()
        with self.lock:
            self.data += chunk
            if not self.can_read_event.is_set():
                self.can_read_event.set()
            if len(self.data) >= self.buffer_size:
                self.can_write_event.clear()
            return len(chunk)

    def prune(self, amount):
        """
        Removes the set amount from the current data object
        used for reading and writing.

        If all of the internal data object has been pruned, the can_read_event
        will be cleared if the total amount processed is less then file_size;
        else, the can_read_event is set to allow for the final b'' read

        :type amount: int
        :param amount: The number of bytes to remove from the data object
        """
        with self.lock:
            # Dance to ensure everything is gc'd. Probably unnecessary
            new_data = self.data[amount:]
            self.data = None
            self.data = new_data
            new_data = None
            self.seek_pos -= amount
            self.processed += amount
            if not self.can_write_event.is_set():
                self.can_write_event.set()
            if len(self.data) == 0:
                if self.processed == self.file_size:
                    # Release the final read for b''
                    self.can_read_event.set()
                else:
                    self.can_read_event.clear()

    def seek(self, *args, **kwargs):
        raise NotImplementedError('Must create seek() in a subclass')


class S3StreamingUpload(BaseS3StreamingObject):
    """
    A streaming object that can be used with upload_fileobj
    to allow uploading files directly from memory without having to write
    to disk.

    Requires that the boto3.s3.transfer.TransferConfig used has
    use_threads set to False, i.e.,
    config = TransferConfig(user_threads=False)
    This is to prevent multi-part uploads

    Requires setting `S3StreamingUpload.prune` as the upload_fileobj Callback,
    as it cannot prune itself.

    :type file_size: int
    :param file_size: The filesize of the file object to upload
    :type: buffer_size: int, None
    :param: buffer_size: The maximum buffer size to use in memory
    """

    def read(self, n, *args, **kwargs):
        """
        Reads the maximum number number of n-bytes, starting from the current
        seek position.

        Does not self-prune the internal data object. You must add
        `S3StreamingUpload.prune` to the Callback function in upload_fileobj

        :type n: int
        :param n: The maximum number of bytes to read

        :rtype: bytes
        :return: Returns the read bytes or b''
        """
        self.can_read_event.wait()
        with self.lock:
            seek_amount = min([n, len(self.data) - self.seek_pos])
            current_seek = self.seek_pos + seek_amount
            output = self.data[self.seek_pos:current_seek]
            self.seek_pos = current_seek
        return output

    def seek(self, offset, whence=0, *args, **kwargs):
        """
        Moves the current seek position as specified by offest and whence.

        This reimplements the standard whence values, as listed below:
            SEEK_SET or 0 – start of the stream (the default);
                            offset should be zero or positive
            SEEK_CUR or 1 – current stream position; offset may be negative
            SEEK_END or 2 – end of the stream; offset is usually negative

        :rtype: int
        :return: Returns the new seek position
        """
        with self.lock:
            if whence <= 0 or whence > 2:
                self.seek_pos = offset
            elif whence == 1:
                self.seek_pos = self.seek_pos + offset
            elif whence == 2:
                self.seek_pos = self.file_size + offset
            return self.seek_pos


class S3StreamingDownload(BaseS3StreamingObject):
    """
    A streaming object that can be used with download_fileobj
    to allow uploading files directly from memory without having to write
    to disk.

    Requires that the boto3.s3.transfer.TransferConfig used has
    use_threads set to False, i.e.,
    config = TransferConfig(user_threads=False)
    This is to prevent multi-part uploads

    Do not set `S3StreamingDownload.prune` as the download_fileobj Callback.
    The class will handle pruning on reads.

    :type file_size: int
    :param file_size: The filesize of the file object to upload
    :type: buffer_size: int, None
    :param: buffer_size: The maximum buffer size to use in memory
    """

    def read(self, n, *args, **kwargs):
        """
        Reads the maximum number number of n-bytes, starting from the current
        seek position.

        Will self prune the internal data object. Do not add
        `S3StreamingDownload.prune` to the download_fileobj Callback

        :type n: int
        :param n: The maximum number of bytes to read

        :rtype: bytes
        :return: Returns the read bytes or None
        """
        self.can_read_event.wait()
        with self.lock:
            seek_amount = min([n, len(self.data) - self.seek_pos])
            current_seek = self.seek_pos + seek_amount
            output = self.data[self.seek_pos:current_seek]
            self.seek_pos = current_seek
        if output == b'':
            # Returning None here signals to S3 that the file is completed
            return
        self.prune(len(output))
        return output

    def seek(self, offset, whence=0, *args, **kwargs):
        """

        This reimplements the standard whence values, as listed below:
            SEEK_SET or 0 – start of the stream (the default);
                            offset should be zero or positive
            SEEK_CUR or 1 – current stream position; offset may be negative
            SEEK_END or 2 – end of the stream; offset is usually negative

        :rtype: int
        :return: Returns the offest as specified by whence whence.
        """
        with self.lock:
            if whence <= 0 or whence > 2:
                pos = offset
            elif whence == 1:
                pos = self.processed + len(self.data) + offset
            elif whence == 2:
                pos = self.file_size + offset
            return pos
