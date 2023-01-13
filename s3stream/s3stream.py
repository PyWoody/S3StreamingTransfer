import io
import threading


class S3StreamingObject(io.BytesIO):
    """
    A streaming object that can be used with upload_fileobj
    to allow uploading files directly from memory without having to write
    to disk.

    Requires that the boto3.s3.transfer.TransferConfig used has
    use_threads set to False, i.e.,
    config = TransferConfig(user_threads=False)
    This is to prevent multi-part uploads

    :type file_size: int
    :param file_size: The filesize of the file object to upload
    :type: buffer_size: int
    :param: buffer_size: The maximum buffer size to use in memory
    """

    def __init__(self, file_size, buffer_size=None):
        if buffer_size is None:
            buffer_size = 8 * (1024 * 1024)
        self.buffer_size = buffer_size
        self.file_size = file_size
        self.processed = 0
        self.seek_pos = 0
        self.data = b''
        self.lock = threading.Lock()
        self.__closed = False
        self.__error = None

    def __repr__(self):
        return (f'{self.__class__.__name__}'
                f'({self.file_size}, buffer_size={self.buffer_size})')

    def __iter__(self):
        while True:
            if data := self.read(self.buffer_size):
                yield data
            else:
                return b''

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

    def write(self, chunk, *args, **kwargs):
        """
        Writes the new data, chunk, to the internal data object.
        Will block until data is smaller than buffer_size

        :type chunk: bytes
        :param chunk: The bytes to write the end of the data object

        :rtype: int
        :return: Returns the amount of bytes written
        """
        self.can_write_event.wait()
        with self.lock:
            self.data += chunk
            self.can_read_event.set()
            if len(self.data) >= self.buffer_size:
                self.can_write_event.clear()
            return len(chunk)

    def read(self, n, *args, **kwargs):
        """
        Reads the maximum number number of n-bytes, starting from the current
        seek position. If no bytes are available and the object has not
        been closed, it will block until more bytes have been
        written or the file has been closed

        :type n: int
        :param n: The maximum number of bytes to read

        :rtype: bytes
        :return: Returns the read bytes or b''
        """
        with self.lock:
            seek_amount = min([n, len(self.data) - self.seek_pos])
            current_seek = self.seek_pos + seek_amount
            output = self.data[self.seek_pos:current_seek]
            if output != b'' or self.closed:
                self.seek_pos = current_seek
        if output == b'' and not self.closed:
            self.can_read_event.wait()
            return self.read(n)
        return output

    def prune(self, amount):
        """
        Removes the set amount from the current data object
        used for reading and writing. If the total number of bytes processed
        at this point is equal to the filesize, the can_read_event is cleared

        This will be the callback for the upload_fileobj

        :type amount: int
        :param amount: The number of bytes to remove from the data object
        """
        with self.lock:
            new_data = self.data[amount:]
            self.data = None
            self.data = new_data
            new_data = None
            self.seek_pos -= amount
            self.processed += amount
            self.can_write_event.set()
            if self.processed == self.file_size:
                self.can_read_event.clear()

    def seek(self, offset, whence=0):
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
                self.seek_pos = 0 + offset
            elif whence == 1:
                self.seek_pos = self.seek_pos + offset
            elif whence == 2:
                self.seek_pos = self.file_size + offset
            return self.seek_pos

    def tell(self):
        """
        Returns the current seek position
        """
        with self.lock:
            return self.seek_pos
