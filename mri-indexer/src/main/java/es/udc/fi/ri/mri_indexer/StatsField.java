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
import java.util.List;
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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Index all text files under a directory.
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing. Run
 * it with no command-line arguments for usage information.
 */
public class StatsField {

    public static void main(String[] args) throws FileNotFoundException, IOException {
		String usage = "java org.apache.lucene.demo.IndexFiles" + " [-index index] [-field field]\n\n";
		String indexPath = "index";
		String option = null;
		Integer numThreads = 0;
		Document doc = null;
		DirectoryReader indexReader = null;
		// Cargamos los parametros de el .config y obtenemos los que nos interesan
		Properties p = new Properties();

		p.load(new FileReader("ruta.config"));
		String docsPath = p.getProperty("DOCS");
		

		boolean field = false;
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				//numThreads = Integer.valueOf(args[i + 1]);
				i++;
			} else if ("-field".equals(args[i])) {
				field = true;
				option = args[i + 1];
			}
		}

		if (docsPath.isEmpty()) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		//Document doc = indexReader.document(docNum);
		//doc.getFields()

		try {
//			dir = FSDirectory.open(Paths.get(indexFolder));
//			indexReader = DirectoryReader.open(dir);
			Directory dirIndex = FSDirectory.open(Paths.get(indexPath));
			indexReader = DirectoryReader.open(dirIndex);	
		} catch (CorruptIndexException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}


        if (field == true){
            IndexStatsField(indexReader, option);
            
        }else{
        	
        	
			for (int i = 0; i < indexReader.numDocs(); i++) {
				
				try {
					doc = indexReader.document(i);
				} catch (CorruptIndexException e1) {
					System.out.println("Graceful message: exception " + e1);
					e1.printStackTrace();
				} catch (IOException e1) {
					System.out.println("Graceful message: exception " + e1);
					e1.printStackTrace();
				}
				
				List<IndexableField> fields = doc.getFields();
				for (IndexableField f : doc.getFields()) {
					//String field =  doc.getField(f.name());
//					String namefield =  f.name();
					IndexStatsField(indexReader, f.name());
				}
//				for(int i1 = 0; i1 < movida.length; i1++){
//					IndexStatsField(fields.get(i1), indexReader);
//				}
				
				
				
			}
			
			
			
        }
		
	}
    
    static void IndexStatsField(DirectoryReader reader, String field) throws IOException{
        //int numDocs = reader.numDocs();
        CollectionStatistics statistics = new CollectionStatistics(field, reader.maxDoc(), reader.numDocs(), reader.getSumTotalTermFreq(field), reader.getSumDocFreq(field));
        //aqui se devuelven segun pidan.
    }
}

