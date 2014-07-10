/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.integration.aws.s3;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.aws.s3.core.AmazonS3Object;
import org.springframework.integration.aws.s3.core.AmazonS3Operations;
import org.springframework.integration.aws.s3.core.PaginatedObjectsView;
import org.springframework.integration.aws.s3.core.S3ObjectSummary;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import static org.springframework.integration.aws.core.AWSCommonUtils.*;

/**
 * The implementation for {@link InboundFileSynchronizer}, this implementation will use the {@link AmazonS3Operations} to list the objects in the remote bucket on invocation of the
 * {@link #synchronizeToLocalDirectory(File, String, String)}. The listed objects will then be checked against the
 * @author Amol Nayak
 * @since 0.5
 */
public class InboundFileSynchronizationImpl implements InboundFileSynchronizer {

	private static final Logger logger = LoggerFactory.getLogger(InboundFileSynchronizationImpl.class);

	public static final String CONTENT_MD5 = "Content-MD5";

	public static final int MD5_HASH_LENGTH = 32;

	private final AmazonS3Operations client;

	private volatile int maxObjectsPerBatch = 10;        //default

	private volatile int maxNumberOfBatches = 2;        //default

	private final InboundLocalFileOperations fileOperations;

	private volatile FileNameFilter filter;

	private volatile String fileWildcard;

	private volatile String fileNameRegex;

	private final Lock lock = new ReentrantLock();

	private volatile boolean acceptSubFolders;

	private volatile String nextMarker = null;

	public InboundFileSynchronizationImpl(AmazonS3Operations client, InboundLocalFileOperations fileOperations) {
		Assert.notNull(client, "AmazonS3Client should be non null");
		Assert.notNull(fileOperations, "fileOperations should be non null");
		this.client = client;
		this.fileOperations = fileOperations;
	}

	@PostConstruct
	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(!(StringUtils.hasText(fileWildcard) && StringUtils.hasText(fileNameRegex)),
				"Only one of the file name wildcard string or file name regex can be specified");

		if (StringUtils.hasText(fileWildcard)) {
			filter = new WildcardFileNameFilter(fileWildcard);
		} else if (StringUtils.hasText(fileNameRegex)) {
			filter = new RegexFileNameFilter(fileNameRegex);
		} else {
			filter = new AlwaysTrueFileNamefilter();    //Match all
		}

		if (acceptSubFolders) {
			((AbstractFileNameFilter) filter).setAcceptSubFolders(true);
			fileOperations.setCreateDirectoriesIfRequired(true);
		}
	}


	/* (non-Javadoc)
	 * @see org.springframework.integration.aws.s3.InboundFileSynchronizer#synchronizeToLocalDirectory(java.io.File, java.lang.String, java.lang.String)
	 */
	public void synchronizeToLocalDirectory(File localDirectory, String bucketName, String remoteFolder) {
		if (!lock.tryLock()) {
			logger.info("Sync already in progess");
			//Prevent concurrent synchronization requests
			return;
		}

		logger.info("Starting sync with local directory");
		//Below sync can take long, above lock ensures only one thread is synchronizing
		try {
			if (remoteFolder != null && "/".equals(remoteFolder)) {
				remoteFolder = null;
			}

			//Set the remote folder for the filter
			if (filter instanceof AbstractFileNameFilter) {
				((AbstractFileNameFilter) filter).setFolderName(remoteFolder);
			}

			if (nextMarker == null) {
				logger.info("Startinf a fresh sync from S3");
			} else {
				logger.info("Continueing a sync from marker: {}", nextMarker);
			}

			int numberOfBatches = 0;
			do {
				PaginatedObjectsView paginatedView = client.listObjects(bucketName, remoteFolder, nextMarker, maxObjectsPerBatch);
				if (paginatedView == null) {
					break; // No results
				}

				nextMarker = paginatedView.getNextMarker();
				List<S3ObjectSummary> summaries = paginatedView.getObjectSummary();
				for (S3ObjectSummary summary : summaries) {
					String key = summary.getKey();
					if (key.endsWith("/") || !filter.accept(key)) {
						continue;
					}

					//The folder is the root as the key is relative to bucket
					AmazonS3Object s3Object = client.getObject(bucketName, "/", key);
					synchronizeObjectWithFile(localDirectory, summary, s3Object);
				}
			} while (nextMarker != null && ++numberOfBatches < maxNumberOfBatches);
		} finally {
			lock.unlock();
			logger.info("Sync completed");
		}
	}

	/**
	 * Synchronizes the Object with the File on the local file system
	 * @param localDirectory local directory to sync to
	 * @param summary s3 overview of objects
	 */
	private void synchronizeObjectWithFile(File localDirectory, S3ObjectSummary summary, AmazonS3Object s3Object) {
		//Get the complete object data

		String key = summary.getKey();
		if (key.endsWith("/")) {
			return;
		}
		int lastIndex = key.lastIndexOf("/");

		String fileName = key.substring(lastIndex + 1);
		String filePath = localDirectory.getAbsolutePath();
		if (!filePath.endsWith(File.separator)) {
			filePath += File.separator;
		}

		File baseDirectory;
		if (lastIndex > 0) {
			//there could very well be previous '/' and thus nested sub folders
			String prefixKey = key.substring(0, lastIndex);
			String[] folders = prefixKey.split("/");
			if (folders.length > 0) {
				for (String folder : folders) {
					filePath = filePath + folder + File.separator;
				}
				//create the directory structure
				baseDirectory = new File(filePath);
				if (baseDirectory.mkdirs()) {
					logger.info("Created new folder: {}", baseDirectory);
				}
			} else {
				baseDirectory = localDirectory;
			}
		} else {
			baseDirectory = localDirectory;
		}

		File file = new File(filePath + fileName);
		if (!file.exists()) {
			//File doesnt exist, write the contents to it
			try {
				fileOperations.writeToFile(baseDirectory, fileName, s3Object.getInputStream());
			} catch (IOException e) {
				logger.error("Caught Exception while writing to file {}", file.getAbsolutePath());
				//continue with next file.
			}
		} else {
			//Synchronize a file that exists
			if (!file.isFile()) {
				logger.warn("The file {} is not a regular file, probably a directory.", file.getAbsolutePath());
				return;
			}
			String eTag = summary.getETag();
			String md5Hex = "";
			if (isEtagMD5Hash(eTag)) {
				//Single thread upload
				try {
					md5Hex = encodeHex(getContentsMD5AsBytes(file));
				} catch (UnsupportedEncodingException e) {
					logger.error("Exception encountered while generating the MD5 hash for the file {}", file.getAbsolutePath(), e);
				}
				if (!eTag.equals(md5Hex)) {
					//The local file is different than the one on S3, could be latest but we will still
					//sync this with the copy on S3
					try {
						fileOperations.writeToFile(baseDirectory, fileName, s3Object.getInputStream());
					} catch (IOException e) {
						logger.error("Caught Exception while writing to file {}", file.getAbsolutePath());
					}
				}
			} else {
				//Multi part upload
				//Get the MD5 hash from the headers
				Map<String, String> userMetaData = s3Object.getUserMetaData();
				String b64MD5 = userMetaData.get(CONTENT_MD5);
				if (b64MD5 != null) {
					//Need to convert to Hex from Base64
					try {
						md5Hex = encodeHex(getContentsMD5AsBytes(file));
					} catch (UnsupportedEncodingException e) {
						logger.error("Exception encountered while generating the MD5 hash for the file {}", file.getAbsolutePath(), e);
					}
					try {
						String remoteHexMD5 = encodeHex(decodeBase64(b64MD5.getBytes("UTF-8")));

						if (!md5Hex.equals(remoteHexMD5)) {
							//Update only if the local file is not same as remote file
							try {
								fileOperations.writeToFile(baseDirectory, fileName, s3Object.getInputStream());
							} catch (IOException e) {
								logger.error("Caught Exception while writing to file {}", file.getAbsolutePath());
							}
						}
					} catch (UnsupportedEncodingException e) {
						//Should never get this, suppress
					}
				} else {
					//Forcefully update the file
					try {
						fileOperations.writeToFile(baseDirectory, fileName, s3Object.getInputStream());
					} catch (IOException e) {
						logger.error("Caught Exception while writing to file {}", file.getAbsolutePath());
					}
				}
			}
		}

		/* Finally clean up S3Object in order to release the HTTP connection lease */
		final InputStream is = s3Object.getInputStream();
		try {
			if (is != null) {
				is.close();
			}
		} catch (IOException e) {
			logger.info("Unable to close connection");
		}
	}

	/**
	 * Checks if the given eTag is a MD5 hash as hex, the hash is 128 bit and hence has to be 32 characters in length, also it should contain only hex characters In case of multi
	 * uploads, it is observed that the eTag contains a "-", and hence this method will return false.
	 * @param eTag
	 */
	private boolean isEtagMD5Hash(String eTag) {
		return !(eTag == null || eTag.length() != MD5_HASH_LENGTH) &&
				eTag.replaceAll("[a-f0-9A-F]", "").isEmpty();
	}

	/* (non-Javadoc)
	 * @see org.springframework.integration.aws.s3.InboundFileSynchronizer#setSynchronizingBatchSize(int)
	 */

	@Override
	public void setSynchronizingBatchSize(int batchSize) {
		if (batchSize > 0) {
			this.maxObjectsPerBatch = batchSize;
		}
	}

	@Override
	public void setMaxNumberOfBatches(final int maxNumberOfBatches) {
		if (maxNumberOfBatches > 0) {
			this.maxNumberOfBatches = maxNumberOfBatches;
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.integration.aws.s3.InboundFileSynchronizer#setFileNamePattern(java.lang.String)
	 */

	@Override
	public void setFileNamePattern(String fileNameRegex) {
		this.fileNameRegex = fileNameRegex;
	}

	/* (non-Javadoc)
	 * @see org.springframework.integration.aws.s3.InboundFileSynchronizer#setFileWildcard(java.lang.String)
	 */

	@Override
	public void setFileWildcard(String fileWildcard) {
		this.fileWildcard = fileWildcard;
	}

	@Override
	public void setAcceptSubFolders(boolean acceptSubFolders) {
		this.acceptSubFolders = acceptSubFolders;
	}
}
