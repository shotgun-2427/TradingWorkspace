"""
Asynchronous Polars → Google Cloud Storage CSV Writer
=====================================================

This module defines a small helper class, :class:`AsyncGCSCSVWriter`, for
efficiently persisting `polars.DataFrame` objects as CSV files in a
Google Cloud Storage (GCS) bucket.  It uses an `asyncio.LifoQueue` to
prioritise the most recently queued DataFrame so that recent data takes
precedence over older submissions.  When a DataFrame is queued via
:meth:`AsyncGCSCSVWriter.save`, it is placed on the stack and picked up by
an asynchronous worker that performs the conversion to CSV and uploads the
result in a separate thread.  The synchronous GCS client provided by
``google‑cloud‑storage`` blocks I/O; running the upload in a thread ensures
that the asyncio event loop remains responsive.

The `polars` library allows writing a DataFrame to a CSV string simply by
passing ``None`` as the file argument to :meth:`polars.DataFrame.write_csv`;
in this case the method returns the CSV content as a Python `str`【279045858854039†L1339-L1345】.
We rely on this behaviour to avoid writing intermediate files on disk.

Usage example
-------------

import asyncio
import polars as pl
from async_polars_gcs import AsyncGCSCSVWriter

async def main():
    writer = AsyncGCSCSVWriter(
        bucket_name="my‑bucket",
        prefix="data/frames",
        max_workers=4,
    )
    # Prepare a small DataFrame
    df = pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    # Queue the DataFrame for upload.  A filename will be generated
    # automatically using the current timestamp if none is provided.
    await writer.save(df)
    # Wait until all queued frames have been uploaded
    await writer.flush()
    # Shut down the writer cleanly
    await writer.close()

asyncio.run(main())

Authentication
--------------

This helper uses the default credentials mechanism of the
``google‑cloud‑storage`` package.  When the environment variable
``GOOGLE_APPLICATION_CREDENTIALS`` is set to the path of a service account
JSON file, the `storage.Client` constructor will automatically pick up
those credentials.  Alternatively, your application can rely on other
authentication methods supported by Google Cloud.

Implementation notes
--------------------

* A single background worker task reads items from a `LifoQueue`.  A
  `LifoQueue` functions like a stack: new items are popped first, so
  recently queued DataFrames are uploaded before earlier ones.  This
  satisfies the requirement to prioritise the most recently added frames.
* The actual upload is executed in a thread pool by calling
  ``loop.run_in_executor``.  This isolates the synchronous network and
  filesystem operations from the event loop.
* The helper exposes :meth:`save` for queuing DataFrames, :meth:`flush`
  for awaiting the completion of all enqueued tasks, and :meth:`close` to
  cancel the worker and release resources.  If you intend to reuse the
  writer over the lifetime of your application, you need only call
  :meth:`close` when shutting down.

"""

from __future__ import annotations

import asyncio
import datetime as _dt
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple, Union

import polars as pl
from google.cloud import storage


class AsyncGCSCSVWriter:
    """Asynchronously upload Polars DataFrames to GCS as CSV files.

    Parameters
    ----------
    bucket_name:
        Name of the GCS bucket to which DataFrames should be uploaded.
    prefix:
        Object name prefix under which uploaded CSV files will be stored.  The
        prefix is prepended to generated filenames to form the full GCS
        object path (e.g. ``prefix/filename.csv``).
    max_workers:
        Maximum number of worker threads used to perform synchronous
        upload operations.  Each worker handles a single upload at a time.
    loop:
        Optionally supply an existing event loop.  If omitted,
        :func:`asyncio.get_running_loop` is used when the class is first
        instantiated from within an asynchronous context.

    """

    def __init__(
            self,
            bucket_name: str,
            prefix: str = "",
            max_workers: int = 4,
            loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if not bucket_name:
            raise ValueError("bucket_name must be provided")
        self.bucket_name = bucket_name
        # normalise prefix: remove leading/trailing slashes
        self.prefix = prefix.strip("/")
        # LIFO queue ensures the most recently queued frame is uploaded first
        self._queue: asyncio.LifoQueue[Tuple[str, pl.DataFrame]] = asyncio.LifoQueue()
        # event loop used for scheduling thread pool tasks
        self._loop = loop
        # Create GCS client and bucket.  The client picks up credentials from
        # the environment (GOOGLE_APPLICATION_CREDENTIALS).  If you need to
        # override credentials, you can create a custom Client and assign it to
        # self._client before using the writer.
        self._client = storage.Client()
        self._bucket = self._client.bucket(bucket_name)
        # Thread pool for running blocking uploads
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        # Flag to stop the worker gracefully
        self._stopped = threading.Event()
        # Start worker task
        self._worker_task: Optional[asyncio.Task[None]] = None
        # Guard for creating worker only once
        self._started = False

    def _ensure_started(self) -> None:
        """Ensure the background worker is running."""
        if not self._started:
            if self._loop is None:
                try:
                    self._loop = asyncio.get_running_loop()
                except RuntimeError:
                    raise RuntimeError(
                        "AsyncGCSCSVWriter must be instantiated from within an async context or provided with an event loop"
                    )
            self._worker_task = self._loop.create_task(self._worker())
            self._started = True

    async def _worker(self) -> None:
        """Background coroutine that processes queued frames."""
        while not self._stopped.is_set():
            try:
                # Wait for next item from the queue.  Use a timeout to allow
                # periodic checks of the stop flag.
                item: Tuple[str, pl.DataFrame] = await asyncio.wait_for(
                    self._queue.get(), timeout=0.1
                )
            except asyncio.TimeoutError:
                continue
            name, df = item
            try:
                # Offload the upload to a thread to avoid blocking the event loop
                await self._loop.run_in_executor(
                    self._executor, self._upload_dataframe, name, df
                )
            finally:
                # Always mark the task as done to prevent ``join`` hanging
                self._queue.task_done()

    def _upload_dataframe(self, name: str, df: pl.DataFrame) -> None:
        """Synchronously convert DataFrame to CSV and upload it to GCS.

        Parameters
        ----------
        name:
            Filename (without the prefix) to use in GCS.  The prefix is
            automatically prepended.
        df:
            The Polars DataFrame to serialise and upload.
        """
        # Convert DataFrame to CSV string.  When ``file`` is None the
        # CSV content is returned as a string【279045858854039†L1339-L1345】.
        csv_str: str = df.write_csv(None)
        # Build the blob name.  Avoid leading slash if prefix is empty.
        blob_name = f"{self.prefix}/{name}" if self.prefix else name
        blob = self._bucket.blob(blob_name)
        # Upload from string; content_type tells GCS to serve it as CSV
        blob.upload_from_string(csv_str, content_type="text/csv")

    async def save(
            self,
            frame: Union[pl.DataFrame, pl.LazyFrame],
            file_name: Optional[str] = None,
    ) -> None:
        """Queue a DataFrame *or* LazyFrame for asynchronous upload.

        Parameters
        ----------
        frame:
            A Polars `DataFrame` **or** `LazyFrame`.  LazyFrames are collected
            eagerly just before being queued.
        file_name:
            Optional name of the CSV file within the prefix.  If omitted,
            a timestamp-based name is generated.
        """
        self._ensure_started()

        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()

        if file_name is None:
            ts = _dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
            file_name = f"frame_{ts}.csv"

        await self._queue.put((file_name, frame))

    async def flush(self) -> None:
        """Wait until all queued DataFrames have been processed."""
        # Wait for queue to empty.  If no worker has been started yet, return
        if not self._started:
            return
        await self._queue.join()

    async def close(self) -> None:
        """Signal the background worker to stop and release resources.

        After calling this method you should not enqueue more DataFrames.
        """
        # If the worker hasn't been started there's nothing to clean up
        if not self._started:
            self._executor.shutdown(wait=False)
            return
        # Wait until all queued tasks have completed
        await self.flush()
        # Signal the worker to exit and cancel its task
        self._stopped.set()
        if self._worker_task is not None:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        # Shut down the thread pool executor
        self._executor.shutdown(wait=True)
        # Clean up GCS client resources (optional but recommended)
        self._client.close()
        self._started = False


__all__ = ["AsyncGCSCSVWriter"]
