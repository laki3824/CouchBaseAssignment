package com.couchbase.assignment;

public class Test {

    private static final String defaultBucketName = "default";

    public static void main(String... args) throws Exception {
        DocumentLoader documentLoader = new DocumentLoader("Administrator", "password");

        System.out.println("Insert 100 documents with arbitrary values.");
        documentLoader.upsertDocuments(defaultBucketName, 100);

        System.out.println("Print all the inserted documents in the bucket: ");
        documentLoader.getAllDocuments(defaultBucketName);

        System.out.println("Document by id: " + documentLoader.getDocumentById(defaultBucketName, "document-1"));

    }
}
