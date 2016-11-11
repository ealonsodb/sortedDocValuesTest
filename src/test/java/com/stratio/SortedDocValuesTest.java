package com.stratio;

import junit.framework.TestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.LongStream;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class SortedDocValuesTest extends TestCase {
    private final String TERM_NAME="term";
    private final String FIELD_NAME="field";
    private final FieldType FIELD_TYPE = new FieldType();
    private Directory directory;
    private IndexWriter indexWriter;



    List<BytesRef> output=new ArrayList<BytesRef>();

    public class FullKeyDataRangeFilteredTermsEnum extends FilteredTermsEnum {
        private boolean first= true;
        FullKeyDataRangeFilteredTermsEnum(TermsEnum tenum) {
            super(tenum);
        }

        /** {@inheritDoc} */
        @Override
        protected AcceptStatus accept(BytesRef term) {
            output.add(term);
            return AcceptStatus.YES;
        }
    }

    public class OwnQuery extends MultiTermQuery {

        public OwnQuery(String field) {
            super(field);
        }

        protected TermsEnum getTermsEnum(Terms terms, AttributeSource attributeSource) throws IOException {
            return new FullKeyDataRangeFilteredTermsEnum(terms.iterator());
        }

        public String toString(String s) {
            return null;
        }
    }

    public void buildFieldType() {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
        FIELD_TYPE.setDocValuesComparator((BytesRef val1, BytesRef val2) -> {
            ByteBuffer bb1 = ByteBufferUtil.byteBuffer(val1);
            ByteBuffer bb2 = ByteBufferUtil.byteBuffer(val2);
            return this.compare(bb1, bb2);
        });
        FIELD_TYPE.freeze();
    }

    public void init() {
        directory = new RAMDirectory();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(null);
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        indexWriterConfig.setUseCompoundFile(true);
        //indexWriterConfig.setIndexSort(mergeSort);
        //indexWriterConfig.setMergePolicy(sortingMergePolicy);
        try {
            indexWriter = new IndexWriter(directory, indexWriterConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Term term(long key) {
        return new Term(TERM_NAME, ByteBufferUtil.bytesRef(ByteBufferUtil.bytes(key)));
    }

    public Field field (long value) {
        return new Field(FIELD_NAME,ByteBufferUtil.bytesRef(ByteBufferUtil.bytes(value)), FIELD_TYPE);
    }

    public Document document(long value) {
        Document doc= new Document();
        doc.add(field(value));
        return doc;
    }

    public void loadData() {
        // Setup index writer
        LongStream.range(0, 10).forEach((value)->{
            try {
                indexWriter.updateDocument(term(value),document(value));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void queryData() throws IOException {
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);


        Query query= new OwnQuery(FIELD_NAME);
        TopDocs doc= indexSearcher.search(query, 20);

        System.out.println("Printing results, must be ordered");
        output.forEach(byteRef -> {System.out.println(Long.toString(ByteBufferUtil.toLong(ByteBufferUtil.byteBuffer(byteRef))));});


    }

    public int compare(ByteBuffer bb1, ByteBuffer bb2) {
        Long long1=ByteBufferUtil.toLong(bb1);
        Long long2=ByteBufferUtil.toLong(bb2);
        return long1.compareTo(long2);
    }

    @Test
    public void test() throws IOException {
        buildFieldType();
        init();
        loadData();
         queryData();

    }
}
