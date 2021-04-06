package es.udc.fi.ri.mri_indexer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Index all text files under a directory.
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing. Run
 * it with no command-line arguments for usage information.
 */
public class IndexFiles {
	
	public static void auxOpenMode (IndexWriterConfig iwc, String option, boolean openmode) {
		
		if (option == "create" || openmode == false) {
			// Create a new index in the directory, removing any
			// previously indexed documents:
			iwc.setOpenMode(OpenMode.CREATE);
		} else if (option == "append") {
			iwc.setOpenMode(OpenMode.APPEND);
		} else if (option == "create_or_append") {
			// Add new documents to an existing index:
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		}
	}

	private IndexFiles() {
	}

	/** Index all text files under a directory. 
	 * @throws IOException 
	 * @throws FileNotFoundException */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		String usage = "java org.apache.lucene.demo.IndexFiles" + " [-index INDEX_PATH] [-update] [-openMode openMode] [-numThreads num] \n\n"
				+ "This indexes the documents in DOCS_PATH, creating a Lucene index"
				+ "in INDEX_PATH that can be searched with SearchFiles";
		
		Properties p = new Properties();
		p.load(new FileReader("/home/rainor/Escritorio/RI/ejemplosLucene8.1.1/mri-indexer/src/main/resources/config.propierties"));
		String indexPath = "index";
		String docsPath = p.getProperty("docs");
		String partialIndexes = p.getProperty("partialIndexes");
		String option = null;
		Integer numThreads = Runtime.getRuntime().availableProcessors();
		boolean create = true;
		
//		// Cargamos los parametros de el .config y obtenemos los que nos interesan
		
		//String partialIndexes = p.getProperty("PARTIAL_INDEXES");
		
		boolean openmode = false;
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} 
//			else if ("-docs".equals(args[i])) {
//				docsPath = args[i + 1];
//				i++;
//			} 
			else if ("-update".equals(args[i])) {
				create = false;
			}
			if ("-numThreads".equals(args[i])) {
				numThreads = Integer.valueOf(args[i + 1]);
				i++;
			} else if ("-openmode".equals(args[i])) {
				openmode = true;
				option = args[i + 1];
			}
			
		}

		if (docsPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		String[] pathSplited = docsPath.split(" ");
		String[] partialIndex = partialIndexes.split(" ");
		int quantityPartialIndex = partialIndexes.length();
		
		for (int i = 0; i < pathSplited.length; i++) {
			final Path docDir =  Paths.get(pathSplited[i]);
			if (!Files.isReadable(docDir)) {
				System.out.println("Document directory '" + docDir.toAbsolutePath()
						+ "' does not exist or is not readable, please check the path");
				System.exit(1);
			}
		}
		
		Date start = new Date();
		try {
			System.out.println("Indexing to directory '" + indexPath + "'...");

			Directory dir = FSDirectory.open(Paths.get(indexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

			
			auxOpenMode(iwc, option, openmode);
			
//			if (option == "create" || openmode == false) {
//				// Create a new index in the directory, removing any
//				// previously indexed documents:
//				iwc.setOpenMode(OpenMode.CREATE);
//			} else if (option == "append") {
//				iwc.setOpenMode(OpenMode.APPEND);
//			} else if (option == "create_or_append") {
//				// Add new documents to an existing index:
//				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
//			}
			
			if ((pathSplited.length == partialIndex.length) && (quantityPartialIndex != 0)){
				for (int i = 0; i < pathSplited.length; i++) {
				
					
				}
				
			}

			// Optional: for better indexing performance, if you
			// are indexing many documents, increase the RAM
			// buffer. But if you do this, increase the max heap
			// size to the JVM (eg add -Xmx512m or -Xmx1g):
			//
			// iwc.setRAMBufferSizeMB(256.0);

			IndexWriter writer = new IndexWriter(dir, iwc);
			indexDocs(writer, docDir, numThreads);

			// NOTE: if you want to maximize search performance,
			// you can optionally call forceMerge here. This can be
			// a terribly costly operation, so generally it's only
			// worth it when your index is relatively static (ie
			// you're done adding documents to it):
			//
			// writer.forceMerge(1);

			writer.close();

			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
	}

	/**
	 * Indexes the given file using the given writer, or if a directory is given,
	 * recurses over files and directories found under the given directory.
	 * 
	 * NOTE: This method indexes one document per input file. This is slow. For good
	 * throughput, put multiple documents into your input file(s). An example of
	 * this is in the benchmark module, which can create "line doc" files, one
	 * document per line, using the <a href=
	 * "../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
	 * >WriteLineDocTask</a>.
	 * 
	 * @param writer Writer to the index where the given file/dir info will be
	 *               stored
	 * @param path   The file to index, or the directory to recurse into to find
	 *               files to index
	 * @throws IOException If there is a low-level I/O error
	 */
	static void indexDocs(final IndexWriter writer, Path path, Integer numThreads) throws IOException {
		
		if (numThreads != 0){
			final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		}
		
		if (Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					try {
						indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
					} catch (IOException ignore) {
						// don't index files that can't be read.
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
		}
	}

	
	/** Indexes a single document */
	static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
		try (InputStream stream = Files.newInputStream(file)) {
			// make a new, empty document
			Document doc = new Document();

			// Add the path of the file as a field named "path". Use a
			// field that is indexed (i.e. searchable), but don't tokenize
			// the field into separate words and don't index term frequency
			// or positional information:
			Field pathField = new StringField("path", file.toString(), Field.Store.YES);
			doc.add(pathField);

			// Add the last modified date of the file a field named "modified".
			// Use a LongPoint that is indexed (i.e. efficiently filterable with
			// PointRangeQuery). This indexes to milli-second resolution, which
			// is often too fine. You could instead create a number based on
			// year/month/day/hour/minutes/seconds, down the resolution you require.
			// For example the long value 2011021714 would mean
			// February 17, 2011, 2-3 PM.
			doc.add(new LongPoint("modified", lastModified));

			// Add the contents of the file to a field named "contents". Specify a Reader,
			// so that the text of the file is tokenized and indexed, but not stored.
			// Note that FileReader expects the file to be in UTF-8 encoding.
			// If that's not the case searching for special characters will fail.
			doc.add(new TextField("contents",
					new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
			
			doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
			
			doc.add(new StringField("thread",  Thread.currentThread().getName(), Field.Store.YES));
			
			doc.add(new StringField("sizeKb", Double.toString(Files.size(file) / 1024), Field.Store.YES));

			BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
			
			doc.add(new StringField("creationTime",  attr.creationTime().toString(), Field.Store.YES));
			
			doc.add(new StringField("lastAccessTime",  attr.lastAccessTime().toString(), Field.Store.YES));
			
			doc.add(new StringField("lastModifiedTime", attr.lastModifiedTime().toString(), Field.Store.YES));
			
			//System.out.println(attr.lastModifiedTime().toString());
	
			
			Calendar atime1 = Calendar.getInstance();
			atime1.setTimeInMillis(attr.creationTime().toMillis());
			Date atimeAsDate1 = atime1.getTime();
			String creationTimeLucene = DateTools.dateToString(atimeAsDate1, DateTools.Resolution.MINUTE);
			System.out.println(creationTimeLucene);
			//doc.add(new StringField("lastModTime", FileTime.toString(Files.getLastModifiedTime((file))), Field.Store.YES));
			
			doc.add(new StringField("creationTimeLucene",  creationTimeLucene, Field.Store.YES));
			
			Calendar atime2 = Calendar.getInstance();
			atime2.setTimeInMillis(attr.creationTime().toMillis());
			Date atimeAsDate2 = atime2.getTime();
			String lastAccessTimeLucene = DateTools.dateToString(atimeAsDate2, DateTools.Resolution.MINUTE);
			
			doc.add(new StringField("creationTimeLucene",  lastAccessTimeLucene, Field.Store.YES));
			
			Calendar atime3 = Calendar.getInstance();
			atime3.setTimeInMillis(attr.creationTime().toMillis());
			Date atimeAsDate3 = atime3.getTime();
			String lastModifiedTimeLucene = DateTools.dateToString(atimeAsDate3, DateTools.Resolution.MINUTE);
			
			doc.add(new StringField("creationTimeLucene",  lastModifiedTimeLucene, Field.Store.YES));
			
			
			if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
				// New index, so we just add the document (no old document can be there):
				System.out.println("adding " + file);
				writer.addDocument(doc);
			} else {
				// Existing index (an old copy of this document may have been indexed) so
				// we use updateDocument instead to replace the old one matching the exact
				// path, if present:
				System.out.println("updating " + file);
				writer.updateDocument(new Term("path", file.toString()), doc);
			}
		}
	}
}
