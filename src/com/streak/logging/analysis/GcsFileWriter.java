package com.streak.logging.analysis;

import com.google.appengine.tools.cloudstorage.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.util.logging.Logger;

/**
 * FancyFileWriter sands down the rough edges of App Engine file API.
 * It automatically handles the gymnastics of opening the file,
 * reopening it periodically, and buffer output.
 */
public class GcsFileWriter {
	// Batch writes so that they're at least FILE_BUFFER_LIMIT bytes
	private static final int FILE_BUFFER_LIMIT = 100000;

	private static final Logger log = Logger.getLogger(GcsFileWriter.class.getName());

    private static GcsService fileService = GcsServiceFactory.createGcsService();

    private GcsFilename logsFile;

    private GcsOutputChannel logsChannel;
	private PrintWriter logsWriter;
	private StringBuffer sb = new StringBuffer();

	public GcsFileWriter(String bucketName, String fileKey) throws IOException {
		log.info("Creating file " + bucketName + ":" + fileKey);

        logsFile = new GcsFilename(bucketName, fileKey);

        logsChannel = fileService.createOrReplace(logsFile, GcsFileOptions.getDefaultInstance());
        logsWriter = new PrintWriter(Channels.newWriter(logsChannel, "UTF8"));
	}

	public void append(String s) throws IOException {
		sb.append(s);
        // check buffer length...
		if (sb.length() > FILE_BUFFER_LIMIT) {
			logsWriter.print(sb);
			sb.delete(0,  sb.length());
		}
	}

	public void closeFinally() throws IOException {
		log.info("Closing file " + logsFile.getObjectName() + " finally ");
        if (sb.length() > 0) {
			logsWriter.print(sb);
			sb.delete(0,  sb.length());
		}
        logsWriter.close();
		logsChannel.close();
	}
	
}
