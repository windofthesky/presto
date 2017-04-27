/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.Iterators;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.toArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempFile;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PrestoWasbFileSystem
        extends FileSystem
{
    private static final Logger log = Logger.get(PrestoWasbFileSystem.class);

    public static final String WASB_ACCESS_KEY = "presto.wasb.access-key";

    public static final String WASB_STAGING_DIRECTORY = "presto.wasb.staging-directory";

    private static final DataSize BLOCK_SIZE = new DataSize(32, MEGABYTE);
    private static final DataSize MAX_SKIP_SIZE = new DataSize(1, MEGABYTE);
    private static final String PATH_SEPARATOR = "/";
    private static final Duration BACKOFF_MIN_SLEEP = new Duration(1, SECONDS);
    public static final String WASB_MAX_CLIENT_RETRIES = "presto.wasb.max-client-retries";
    public static final String WASB_MAX_BACKOFF_TIME = "presto.wasb.max-backoff-time";
    public static final String WASB_MAX_RETRY_TIME = "presto.wasb.max-retry-time";

    private URI uri;
    private Path workingDirectory;
    private File stagingDirectory;
    private int maxAttempts;
    private Duration maxBackoffTime;
    private Duration maxRetryTime;

    private CloudBlobClient wasbClient;
    private String containerName = null;

    @Override
    public void initialize(URI uri, Configuration conf)
            throws IOException
    {
        requireNonNull(uri, "uri is null");
        requireNonNull(conf, "conf is null");
        super.initialize(uri, conf);
        setConf(conf);

        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDirectory = new Path(PATH_SEPARATOR).makeQualified(this.uri, new Path(PATH_SEPARATOR));

        HiveWasbConfig defaults = new HiveWasbConfig();
        this.stagingDirectory = new File(conf.get(WASB_STAGING_DIRECTORY, defaults.getWasbStagingDirectory().toString()));
        this.maxAttempts = conf.getInt(WASB_MAX_CLIENT_RETRIES, defaults.getWasbMaxClientRetries()) + 1;
        this.maxBackoffTime = Duration.valueOf(conf.get(WASB_MAX_BACKOFF_TIME, defaults.getWasbMaxBackoffTime().toString()));
        this.maxRetryTime = Duration.valueOf(conf.get(WASB_MAX_RETRY_TIME, defaults.getWasbMaxRetryTime().toString()));

        this.containerName = getContainerName(uri);
        this.wasbClient = createWasbClient(uri, conf);
    }

    private String getContainerName(URI uri)
    {
        return uri.getUserInfo();
    }

    private CloudBlobContainer getContainer()
    {
        try {
            return wasbClient.getContainerReference(containerName);
        }
        catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
        catch (StorageException e) {
            throw Throwables.propagate(e);
        }
    }

    private CloudBlobClient createWasbClient(URI uri, Configuration hadoopConfig)
    {
        StorageCredentials credentials = getWasbCredentialsProvider(uri, hadoopConfig);
        try {
            CloudStorageAccount storageAccount = new CloudStorageAccount(credentials);
            return storageAccount.createCloudBlobClient();
        }
        catch (URISyntaxException e) {
            e.printStackTrace();
            log.info(" Exception in createWasbClient");
            return null;
        }
    }

    private StorageCredentials getWasbCredentialsProvider(URI uri, Configuration conf)
    {
        Optional<StorageCredentials> credentials = getWasbCredentials(uri, conf);
        if (credentials.isPresent()) {
            return credentials.get();
        }
        throw new RuntimeException("WASB credentials not configured");
    }

    private static Optional<StorageCredentials> getWasbCredentials(URI uri, Configuration conf)
    {
        String accessKey = conf.get(WASB_ACCESS_KEY);
        String host = uri.getHost();
        String accountName = host.substring(0, host.indexOf("."));

        if (isNullOrEmpty(accessKey) || isNullOrEmpty(accountName)) {
            return Optional.empty();
        }
        return Optional.of(new StorageCredentialsAccountAndKey(accountName, accessKey));
    }

    @Override
    public URI getUri()
    {
        return uri;
    }

    @Override
    public Path getWorkingDirectory()
    {
        return workingDirectory;
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        workingDirectory = path;
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        List<LocatedFileStatus> list = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = listLocatedStatus(path);
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return toArray(list, LocatedFileStatus.class);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path path)
    {
        return new RemoteIterator<LocatedFileStatus>()
        {
            private final Iterator<LocatedFileStatus> iterator = listPrefix(path);

            @Override
            public boolean hasNext()
                    throws IOException
            {
                try {
                    return iterator.hasNext();
                }
                catch (Exception e) {
                    throw new IOException(e);
                }
            }

            @Override
            public LocatedFileStatus next()
                    throws IOException
            {
                try {
                    return iterator.next();
                }
                catch (Exception e) {
                    throw new IOException(e);
                }
            }
        };
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return null;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        return new FSDataInputStream(
                new BufferedFSInputStream(
                        new PrestoWasbInputStream(wasbClient, uri.getHost(), path, maxAttempts, maxBackoffTime, maxRetryTime),
                        bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        if ((!overwrite) && exists(path)) {
            throw new IOException("File already exists:" + path);
        }

        if (!stagingDirectory.exists()) {
            createDirectories(stagingDirectory.toPath());
        }
        if (!stagingDirectory.isDirectory()) {
            throw new IOException("Configured staging path is not a directory: " + stagingDirectory);
        }
        File tempFile = createTempFile(stagingDirectory.toPath(), "presto-wasb-", ".tmp").toFile();

        String key = keyFromPath(qualifiedPath(path));
        return new FSDataOutputStream(
                new PrestoWasbOutputStream(getContainer(), uri.getHost(), key, tempFile),
                statistics);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
    {
        throw new UnsupportedOperationException("append");
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException
    {
        throw new UnsupportedOperationException("rename");
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        throw new UnsupportedOperationException("delete");
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
    {
        // no need to do anything for Wasb
        return true;
    }

    private Iterator<LocatedFileStatus> listPrefix(Path path)
    {
        try {
            ResultSegment<ListBlobItem> resultSegment = getContainer().listBlobsSegmented(path.toUri().getPath().substring(1),
                    true, EnumSet.noneOf(BlobListingDetails.class), null, null, null, null);

            Iterator<ResultSegment> listings = new AbstractSequentialIterator<ResultSegment>(resultSegment)
            {
                @Override
                protected ResultSegment computeNext(ResultSegment previous)
                {
                    return null;
                }
            };
            return Iterators.concat(Iterators.transform(listings, this::statusFromListing));
        }
        catch (StorageException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Iterator<LocatedFileStatus> statusFromListing(ResultSegment listing)
    {
        return statusFromObjects(listing);
    }

    private Iterator<LocatedFileStatus> statusFromObjects(ResultSegment listing)
    {
        List<CloudBlockBlob> input = getBlobList(listing);
        return input.stream()
                .map(object -> new FileStatus(
                        object.getProperties().getLength(),
                        false,
                        1,
                        BLOCK_SIZE.toBytes(),
                        object.getProperties().getLastModified().getTime(),
                        qualifiedPath(new Path(object.getName()))))
                .map(this::createLocatedFileStatus)
                .iterator();
    }

    private List<CloudBlockBlob> getBlobList(ResultSegment<CloudBlockBlob> listing)
    {
        return listing.getResults();
    }

    private Path qualifiedPath(Path path)
    {
        return path.makeQualified(this.uri, getWorkingDirectory());
    }

    private LocatedFileStatus createLocatedFileStatus(FileStatus status)
    {
        try {
            BlockLocation[] fakeLocation = getFileBlockLocations(status, 0, status.getLen());
            return new LocatedFileStatus(status, fakeLocation);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static String keyFromPath(Path path)
    {
        checkArgument(path.isAbsolute(), "Path is not absolute: %s", path);
        String key = nullToEmpty(path.toUri().getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }

    private static class PrestoWasbInputStream
            extends FSInputStream
    {
        private final CloudBlobClient wasbClient;
        private final String host;
        private final Path path;
        private final int maxAttempts;
        private final Duration maxBackoffTime;
        private final Duration maxRetryTime;

        private boolean closed;
        private InputStream in;
        private long streamPosition;
        private long nextReadPosition;
        private String containerNameIn;

        public PrestoWasbInputStream(CloudBlobClient wasbClient, String host, Path path, int maxAttempts, Duration maxBackoffTime, Duration maxRetryTime)
        {
            this.wasbClient = requireNonNull(wasbClient, "wasbClient is null");
            this.host = requireNonNull(host, "host is null");
            this.path = requireNonNull(path, "path is null");

            checkArgument(maxAttempts >= 0, "maxAttempts cannot be negative");
            this.maxAttempts = maxAttempts;
            this.maxBackoffTime = requireNonNull(maxBackoffTime, "maxBackoffTime is null");
            this.maxRetryTime = requireNonNull(maxRetryTime, "maxRetryTime is null");
            this.containerNameIn = getContainerName(path);
        }

        private String getContainerName(Path path)
        {
            return path.toUri().getUserInfo();
        }

        private CloudBlobContainer getContainer()
        {
            try {
                return wasbClient.getContainerReference(containerNameIn);
            }
            catch (URISyntaxException e) {
                throw Throwables.propagate(e);
            }
            catch (StorageException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close()
        {
            closed = true;
            closeStream();
        }

        @Override
        public void seek(long pos)
        {
            checkState(!closed, "already closed");
            checkArgument(pos >= 0, "position is negative: %s", pos);

            // this allows a seek beyond the end of the stream but the next read will fail
            nextReadPosition = pos;
        }

        @Override
        public long getPos()
        {
            return nextReadPosition;
        }

        @Override
        public int read()
        {
            // This stream is wrapped with BufferedInputStream, so this method should never be called
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            try {
                int bytesRead = retry()
                        .maxAttempts(maxAttempts)
                        .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                        .stopOn(InterruptedException.class)
                        .run("readStream", () -> {
                            seekStream();
                            try {
                                return in.read(buffer, offset, length);
                            }
                            catch (Exception e) {
                                closeStream();
                                throw e;
                            }
                        });

                if (bytesRead != -1) {
                    streamPosition += bytesRead;
                    nextReadPosition += bytesRead;
                }
                return bytesRead;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                Throwables.propagateIfInstanceOf(e, IOException.class);
                throw Throwables.propagate(e);
            }
        }

        @Override
        public boolean seekToNewSource(long targetPos)
        {
            return false;
        }

        private void seekStream()
                throws IOException
        {
            if ((in != null) && (nextReadPosition == streamPosition)) {
                // already at specified position
                return;
            }

            if ((in != null) && (nextReadPosition > streamPosition)) {
                // seeking forwards
                long skip = nextReadPosition - streamPosition;
                if (skip <= max(in.available(), MAX_SKIP_SIZE.toBytes())) {
                    // already buffered or seek is small enough
                    try {
                        if (in.skip(skip) == skip) {
                            streamPosition = nextReadPosition;
                            return;
                        }
                    }
                    catch (IOException ignored) {
                        // will retry by re-opening the stream
                    }
                }
            }

            // close the stream and open at desired position
            streamPosition = nextReadPosition;
            closeStream();
            openStream();
        }

        private void openStream()
                throws IOException
        {
            if (in == null) {
                in = openStream(path, nextReadPosition);
                streamPosition = nextReadPosition;
            }
        }

        private InputStream openStream(Path path, long start)
                throws IOException
        {
            try {
                return retry()
                        .maxAttempts(maxAttempts)
                        .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                        .stopOn(InterruptedException.class)
                        .run("getWasbObject", () -> {
                            try {
                                return getContainer().getBlockBlobReference(path.toUri().getPath().substring(1)).openInputStream();
                            }
                            catch (RuntimeException e) {
                                throw Throwables.propagate(e);
                            }
                        });
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                Throwables.propagateIfInstanceOf(e, IOException.class);
                throw Throwables.propagate(e);
            }
        }

        private void closeStream()
        {
            if (in != null) {
                try {
                    in.close();
                }
                catch (IOException ignored) {
                    // thrown if the current thread is in the interrupted state
                }
                in = null;
            }
        }
    }

    private static class PrestoWasbOutputStream
            extends FilterOutputStream
    {
        private final String host;
        private final String key;
        private final File tempFile;
        private final CloudBlobContainer container;

        private boolean closed;

        public PrestoWasbOutputStream(CloudBlobContainer container, String host, String key, File tempFile)
                throws IOException
        {
            super(new BufferedOutputStream(new FileOutputStream(requireNonNull(tempFile, "tempFile is null"))));

            this.container = requireNonNull(container, "container is null");
            this.host = requireNonNull(host, "host is null");
            this.key = requireNonNull(key, "key is null");
            this.tempFile = tempFile;

            log.debug("OutputStream for key '%s' using file: %s", key, tempFile);
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;

            try {
                super.close();
                uploadObject();
            }
            finally {
                if (!tempFile.delete()) {
                    log.warn("Could not delete temporary file: %s", tempFile);
                }
            }
        }

        private void uploadObject()
                throws IOException
        {
            try {
                CloudBlockBlob blob = container.getBlockBlobReference(key);
                File source = new File(tempFile.getPath());
                blob.upload(new java.io.FileInputStream(source), source.length());
            }
            catch (Exception e) {
                // Output the stack trace.
                e.printStackTrace();
            }
        }
    }
}
