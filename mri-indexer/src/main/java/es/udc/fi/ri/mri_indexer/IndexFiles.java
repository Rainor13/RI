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
import java.io.ByteArrayInputStream;
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
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
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
		
		if ((option.trim().equals("create") || openmode == false)) {
			// Create a new index in the directory, removing any
			// previously indexed documents:
			System.out.println("ESTOY AQUI");
			iwc.setOpenMode(OpenMode.CREATE);
		} else if (option.trim().equals("append")) {
			iwc.setOpenMode(OpenMode.APPEND);
		} else if (option.trim().equals("create_or_append")) {
			// Add new documents to an existing index:
			System.out.println("NO ESTOY AQUI");
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		}
	}
	
	public static String readLines (InputStream stream) throws IOException {
		
		String line;
		//String contents = new String();
		StringBuilder contents = new StringBuilder();
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		
		while((line = reader.readLine()) != null) {
			contents.append(line);
		}
		
		reader.close();
		return contents.toString();
	}
	
	public static String readLinesBotAndTop (int topLines, int bottomLines, InputStream stream) throws IOException {
		
		ArrayList<String> linesOfFile = new ArrayList<String>();
		String line;
		StringBuilder contents = new StringBuilder();
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		
		while((line = reader.readLine()) != null) {
			linesOfFile.add(line);
		}
		
		int lines = linesOfFile.size() - bottomLines;
		
		for (int i = 0; i < topLines; i++) {
			if (reader.readLine() != null)
				contents.append(linesOfFile.get(i));
		}
		for (int i = lines; i < linesOfFile.size(); i++) {
			contents.append(linesOfFile.get(i));
		}
		
		//contents.append(line);
//		for (int i = 0; (i < topLines) && (linesOfFile.size() >= topLines); i++) {
//			
//		}
//		for (int i = 0; (i < linesOfFile.size()) && (linesOfFile.size() >= bottomLines); i++) {
//			if (i >= linesOfFile.size()-bottomLines) {
//				contents.append(linesOfFile.get(i));
//			}
//		}
		
		reader.close();
		return contents.toString();
	}
	
	public static String readLinesTopOnly (int topLines, InputStream stream) throws IOException {
		
		ArrayList<String> linesOfFile = new ArrayList<String>();
		String line;
		StringBuilder contents = new StringBuilder();
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		
		while((line = reader.readLine()) != null) {
			linesOfFile.add(line);
		}
		
//		for (int i = 0; (i < topLines) && (linesOfFile.size() >= topLines); i++) {
//			contents.append(linesOfFile.get(i));
//		}
		for (int i = 0; i < topLines; i++) {
			if (reader.readLine() != null)
				contents.append(linesOfFile.get(i));
		}
		
		reader.close();
		return contents.toString();
	}
	
	public static String readLinesBotOnly (int bottomLines, InputStream stream) throws IOException {
		
		ArrayList<String> linesOfFile = new ArrayList<String>();
		String line;
		StringBuilder contents = new StringBuilder();
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		
		while((line = reader.readLine()) != null) {
			linesOfFile.add(line);
		}
		
		int lines = linesOfFile.size() - bottomLines;
		
//		for (int i = 0; (i < linesOfFile.size()) && (linesOfFile.size() >= bottomLines); i++) {
//			if (i >= linesOfFile.size()-bottomLines) {
//				contents.append(linesOfFile.get(i));
//			}
//		}
		for (int i = lines; i < linesOfFile.size(); i++) {
			contents.append(linesOfFile.get(i));
		}
		
		reader.close();
		return contents.toString();
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
		String onlyFiles = p.getProperty("onlyFiles");
		String onlyTopLines = p.getProperty("onlyTopLines");
		String onlyBottomLines = p.getProperty("onlyBottomLines");
		String option = null;
		Integer numThreads = null;
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
				if (numThreads == 0) {
					numThreads = Runtime.getRuntime().availableProcessors();
				}
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
			IndexWriter mainWriter = new IndexWriter(dir, iwc);
			
			//System.out.println(option);
			
			//System.out.println("VOY A ENTRAR");
			auxOpenMode(iwc, option, openmode);
			//System.out.println("PASE POR AQUI");
			
//			if ((option.trim().equals("create") || openmode == false)) {
//				// Create a new index in the directory, removing any
//				// previously indexed documents:
//				System.out.println("ESTOY AQUI");
//				iwc.setOpenMode(OpenMode.CREATE);
//			} else if (option.trim().equals("append")) {
//				iwc.setOpenMode(OpenMode.APPEND);
//			} else if (option.trim().equals("create_or_append")) {
//				// Add new documents to an existing index:
//				System.out.println("NO ESTOY AQUI");
//				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
//			}
			
//			System.out.println(pathSplited.length);
//			System.out.println(partialIndex.length);
//			for (int i = 0; i < partialIndex.length; i++) {
//				System.out.println(partialIndex[i]);
//			}
//			
			if ((pathSplited.length == partialIndex.length) && (quantityPartialIndex != 0)){
				for (int i = 0; i < pathSplited.length; i++) {
					Directory partialDir = FSDirectory.open(Paths.get(partialIndex[i]));
					Analyzer partialAnalyzer = new StandardAnalyzer();
					IndexWriterConfig partialIwc = new IndexWriterConfig(partialAnalyzer);
					
					//System.out.println(option);
					//System.out.println("VOY A ENTRAR");
					auxOpenMode(partialIwc, option, openmode);
					//System.out.println("YA SALI DE AQUI");
			
//					if ((option.trim().equals("create") || openmode == false)) {
//						// Create a new index in the directory, removing any
//						// previously indexed documents:
//						System.out.println("ESTOY AQUI");
//						partialIwc.setOpenMode(OpenMode.CREATE);
//					} else if (option.trim().equals("append")) {
//						partialIwc.setOpenMode(OpenMode.APPEND);
//					} else if (option.trim().equals("create_or_append")) {
//						// Add new documents to an existing index:
//						System.out.println("NO ESTOY AQUI");
//						partialIwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
//					}
					
					IndexWriter writer = new IndexWriter(partialDir, partialIwc);
					indexDocs(writer, Paths.get(pathSplited[i]), numThreads, onlyFiles, onlyTopLines, onlyBottomLines);
					writer.close();
					mainWriter.addIndexes(partialDir);
				}
				
			} else {
				for (int i = 0; i < pathSplited.length; i++) {
					indexDocs(mainWriter, Paths.get(pathSplited[i]), numThreads, onlyFiles, onlyTopLines, onlyBottomLines);
				}
			}

			mainWriter.close();
			
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
	
	static void indexDocs(final IndexWriter writer, Path path, Integer numThreads, String onlyFiles, String onlyTopLines, String onlyBottomLines) throws IOException {
		
		final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		if (Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if(onlyFiles.isEmpty()) {
						//cambiar el nombre
						final Runnable worker = new WorkerThread(file, writer, attrs.lastModifiedTime().toMillis(), onlyTopLines, onlyBottomLines);
						executor.execute(worker);
					}else {
						String[] extensions = onlyFiles.split(" ");
						String extension = ("."+FilenameUtils.getExtension(file.toString()));
						for (int i = 0; i < extensions.length; i++) {
							if(extension.contentEquals(extensions[i])) {
								//cambiar el nombre
								final Runnable worker = new WorkerThread(file, writer, attrs.lastModifiedTime().toMillis(), onlyTopLines, onlyBottomLines);
								executor.execute(worker);
							}
						}
					}
					return FileVisitResult.CONTINUE;
				}
			});
		}else {
			if(onlyFiles.isEmpty() == true) {
				//cambiar el nombre
				final Runnable worker = new WorkerThread(path, writer, Files.getLastModifiedTime(path).toMillis(), onlyTopLines, onlyBottomLines);
				executor.execute(worker);
			}else {
				String[] extensions = onlyFiles.split(" ");
				String extension = ("."+FilenameUtils.getExtension(path.toString()));
				for (int i = 0; i < extensions.length; i++) {
					if(extension.contentEquals(extensions[i])) {
						//cambiar el nombre
						final Runnable worker = new WorkerThread(path, writer, Files.getLastModifiedTime(path).toMillis(), onlyTopLines, onlyBottomLines);
						executor.execute(worker);
					}
				}
			}
			
		}
		executor.shutdown();

		try {
			executor.awaitTermination(1, TimeUnit.HOURS);
		} catch (final InterruptedException e) {
			e.printStackTrace();
			System.exit(-2);
		}
	}

	
	/** Indexes a single document */
	static void indexDoc(IndexWriter writer, Path file, long lastModified,  String onlyTopLines, String onlyBottomLines) throws IOException {
		try (InputStream stream = Files.newInputStream(file)) {
			// make a new, empty document
			Document doc = new Document();
			
			Field pathField = new StringField("path", file.toString(), Field.Store.YES);
			FieldType contentFieldType = new FieldType(StringField.TYPE_STORED);

			String contents = new String();
			
			//System.out.println("top STRING " + onlyTopLines);
			//System.out.println("bot STRING " + onlyBottomLines + ". A");
			
			if ((!onlyTopLines.isEmpty()) && (!onlyBottomLines.isEmpty())) {
				System.out.print("TOP Y BOT");
				int topLines = Integer.parseInt(onlyTopLines);
				int bottomLines = Integer.parseInt(onlyBottomLines);
				contents = readLinesBotAndTop(topLines, bottomLines, stream);
			} else if ((!onlyTopLines.isEmpty()) && (onlyBottomLines.isEmpty())) {
				System.out.print("TOP");
				int topLines = Integer.parseInt(onlyTopLines);
				contents = readLinesTopOnly(topLines, stream);
			} else if ((onlyTopLines.isEmpty()) && (!onlyBottomLines.isEmpty())) {
				System.out.print("BOT");
				int bottomLines = Integer.parseInt(onlyBottomLines);
				contents = readLinesBotOnly(bottomLines, stream);
			} else {
				contents = readLines(stream);

			}
			InputStream movida = new ByteArrayInputStream(contents.getBytes());
			
			//System.out.println("top " + topLines);
			//System.out.println("bot " + bottomLines);
//			if (topLines != null) {
//				readLines(topLinesStr, bottomLinesStr, topLines, bottomLines, stream);
//			}else {
//				
//			}
			//System.out.println("nepe");
			
			doc.add(pathField);

			doc.add(new LongPoint("modified", lastModified));

			//doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
			
			doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(movida, StandardCharsets.UTF_8))));
			
			//doc.add(new Field("contents",contents, contentFieldType));
		
			//doc.add(new StringField("contents", contents, Field.Store.YES));
			
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
			//System.out.println(creationTimeLucene);
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
			
			System.out.print(writer.getConfig().getOpenMode());
			
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
	
	/**
	 * This Runnable takes a folder and prints its path.
	 */
	public static class WorkerThread implements Runnable {

		private final Path folder;
		private final IndexWriter writer;
		private final long lastModified;
		private String onlyTopLines;
		private String onlyBottomLines;

		public WorkerThread(final Path folder, final IndexWriter writer, final long lastModified, String onlyTopLines, String onlyBottomLines) {
			this.folder = folder;
			this.writer = writer;
			this.lastModified = lastModified;
			this.onlyTopLines = onlyTopLines;
			this.onlyBottomLines = onlyBottomLines;
		}

		/**
		 * This is the work that the current thread will do when processed by the pool.
		 * In this case, it will only print some information.
		 */
		@Override
		public void run() {
			try {
				System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
						Thread.currentThread().getName(), folder));
				indexDoc(writer, folder, lastModified, onlyTopLines, onlyBottomLines);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}
