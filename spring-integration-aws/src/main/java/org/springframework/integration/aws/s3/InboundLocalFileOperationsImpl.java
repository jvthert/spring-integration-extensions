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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;

/**
 * The Implementation class for the {@link InboundLocalFileOperations}
 * @author Amol Nayak
 * @since 0.5
 */
public class InboundLocalFileOperationsImpl implements
		InboundLocalFileOperations {

	private static final Logger logger = LoggerFactory.getLogger(InboundLocalFileOperationsImpl.class);

	public static final int DEFAULT_TEMPFILE_SIZE = 4096;

	private final List<FileEventHandler> handlers = new ArrayList<>();

	private volatile String tempFileSuffix = ".writing";

	private volatile boolean createDirectoriesIfRequired;

	/* (non-Javadoc)
	 * @see org.springframework.integration.aws.s3.InboundLocalFileOperations#addEventListener(org.springframework.integration.aws.s3.FileEventHandler)
	 */
	@Override
	public void addEventListener(FileEventHandler handler) {
		Assert.notNull(handler, "Handler instance must non null");
		handlers.add(handler);
	}

	/* (non-Javadoc)
	 * @see org.springframework.integration.aws.s3.InboundLocalFileOperations#setEventListeners(java.util.List)
	 */
	@Override
	public void setEventListeners(List<FileEventHandler> handlers) {
		Assert.notNull(handlers, "Handlers must be non null and non empty");
		Assert.notEmpty(handlers, "Handlers must be non null and non empty");
		this.handlers.clear();
		this.handlers.addAll(handlers);
	}

	/* (non-Javadoc)
	 * @see org.springframework.integration.aws.s3.InboundLocalFileOperations#setTemporaryFileSuffix(java.lang.String)
	 */
	@Override
	public void setTemporaryFileSuffix(String tempFileSuffix) {
		if (!StringUtils.hasText(tempFileSuffix)) {
			return;
		}

		if (!tempFileSuffix.startsWith(".")) {
			this.tempFileSuffix = "." + tempFileSuffix;
		} else {
			this.tempFileSuffix = tempFileSuffix;
		}
	}

	/** Returns true if create directories if required flag is set to true */
	@SuppressWarnings("UnusedDeclaration")
	public boolean isCreateDirectoriesIfRequired() {
		return createDirectoriesIfRequired;
	}

	/**
	 * Sets the flag to true if directories given are to be created if not present
	 */
	@Override
	public void setCreateDirectoriesIfRequired(boolean createDirectoriesIfRequired) {
		this.createDirectoriesIfRequired = createDirectoriesIfRequired;
	}

	/* (non-Javadoc)
	 * @see org.springframework.integration.aws.s3.InboundLocalFileOperations#writeToFile(java.io.File, java.lang.String, java.io.InputStream)
	 */
	public void writeToFile(File directory, String fileName, InputStream in)
			throws IOException {
		Assert.notNull(directory, "Provide a non null directory");
		Assert.hasText(fileName, "Provide a non null non empty file name");
		Assert.notNull(in, "Provide a non null instance of InputStream");
		Assert.isTrue(!directory.exists() || directory.isDirectory(), "Provided directory is not a directory");
		Assert.isTrue(createDirectoriesIfRequired || directory.exists(), "Provided directories does not exist and create directory flag is false");

		if (!directory.exists() && createDirectoriesIfRequired && !directory.mkdirs()) {
			throw new IOException(String.format("Unable to create the directory '%s'", directory.getAbsolutePath()));
		}

		if (!(in instanceof ByteArrayInputStream)
				&& !(in instanceof BufferedInputStream)) {
			in = new BufferedInputStream(in);
		}
		String tempFileName = fileName + tempFileSuffix;
		byte[] bytes = new byte[DEFAULT_TEMPFILE_SIZE];    //4K

		String absoluteDirectoryPath = directory.getAbsolutePath();
		String filePath;
		if (absoluteDirectoryPath.endsWith(File.separator)) {
			filePath = absoluteDirectoryPath + tempFileName;
		} else {
			filePath = absoluteDirectoryPath + File.separator + tempFileName;
		}

		final File fileToWrite = new File(filePath);
		if (!fileToWrite.exists() && !fileToWrite.createNewFile()) {
			throw new RuntimeException("Unable to create file " + fileToWrite.getAbsolutePath());
		}

		try (FileOutputStream fos = new FileOutputStream(fileToWrite);
			 BufferedOutputStream bos = new BufferedOutputStream(fos) ) {
			for (int read; (read = in.read(bytes)) != -1; ) {
				bos.write(bytes, 0, read);
			}
		} catch (FileNotFoundException e) {
			logger.error("File not found: {}", fileToWrite, e);
			throw e;
		}

		//Now rename the file
		final File dest = new File(filePath.substring(0, filePath.indexOf(tempFileSuffix)));
		//if destination file exists, delete it
		final boolean isSuccessful;
		if (dest.exists()) {
			boolean isDeleteSuccessful = dest.delete();
			if (isDeleteSuccessful) {
				logger.debug("Delete of file {} succesful", dest.getName());
				//now rename the temp file to perm destination file
				isSuccessful = renameFile(fileToWrite, dest);
			} else {
				logger.warn("Deletion of file {} not successful, falling back to overwriting the contents", dest.getName());
				FileCopyUtils.copy(fileToWrite, dest);
				boolean deleteTemp = fileToWrite.delete();
				if (!deleteTemp) {
					logger.warn("Deletion of {} unsuccessful", fileToWrite.getName());
				}
				isSuccessful = true;    //as copy has occurred successfully
			}
		} else {
			isSuccessful = renameFile(fileToWrite, dest);
		}
		//Create notification for the listeners
		if (!handlers.isEmpty()) {
			FileEvent event = new FileEvent() {
				public FileOperationType getFileOperation() {
					return FileOperationType.CREATE;
				}
				public File getFile() {
					return isSuccessful ? dest : fileToWrite;
				}
			};

			//Send notification to all listeners
			for (FileEventHandler handler : handlers) {
				try {
					handler.onEvent(event);
				} catch (Exception e) {
					logger.info("Exception occurred while notifying the handler class {}", handler.getClass().getName(), e);
				}
			}
		}
	}

	/** Private helper method that is used to rename the source to destination file */
	private boolean renameFile(final File from, final File to) {
		final boolean isSuccessful;
		final boolean isRenameSuccessful = from.renameTo(to);
		if (isRenameSuccessful) {
			logger.debug("Renaming of file {} to {} successful", from.getName(), to.getName());
			isSuccessful = true;
		} else {
			logger.warn("Renaming of file {} to {} unsuccessful", from.getName(), to.getName());
			isSuccessful = false;
		}
		return isSuccessful;
	}
}
