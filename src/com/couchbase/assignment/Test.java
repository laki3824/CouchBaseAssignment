package com.couchbase.assignment;

import com.couchbase.client.java.document.JsonDocument;

public class Test {

    private static final String defaultBucketName = "default";

    public static void main(String... args) throws Exception {
        DocumentLoader documentLoader = new DocumentLoader("Administrator", "password");

        System.out.println("Insert 100 documents with arbitrary values.");
        documentLoader.upsertDocuments(defaultBucketName, 100);

        System.out.println("Print all the inserted documents in the bucket: ");
        documentLoader.getAllDocuments(defaultBucketName);

        System.out.println("Document by id: " + documentLoader.getDocumentById(defaultBucketName, "document-1"));

        /*Initialize the Connection
        Cluster cluster = CouchbaseCluster.create("localhost");
        cluster.authenticate("Administrator", "password");
        final Bucket bucket = cluster.openBucket("default");

        // Create a JSON Document
        /*JsonObject arthur = JsonObject.create()
                .put("name", "Arthur")
                .put("email", "kingarthur@couchbase.com")
                .put("interests", JsonArray.from("Holy Grail", "African Swallows"));

        // Store the Document
        bucket.upsert(JsonDocument.create("u:king_arthur", arthur));




        // Generate a number of dummy JSON documents
        int docsToCreate = 5;
        List<JsonDocument> documents = new ArrayList<JsonDocument>();
        for (int i = 0; i < docsToCreate; i++) {
            JsonObject content = JsonObject.create()
                    .put("id", UUID.randomUUID().toString());
            documents.add(JsonDocument.create("doc-"+i, content));
        }

        // Insert them in one batch, waiting until the last one is done.
        Observable
                .from(documents)
                .flatMap(new Func1<JsonDocument, Observable<?>>() {
                    public Observable<JsonDocument> call(final JsonDocument docToInsert) {
                        return bucket.async().insert(docToInsert);
                    }
                })
                .last()
                .toBlocking()
                .single();

        // Load the Document and print it
        // Prints Content and Metadata of the stored Document
        //System.out.println(bucket.get("u:king_arthur"));

        // Create a N1QL Primary Index (but ignore if it exists)
        bucket.bucketManager().createN1qlPrimaryIndex(true, false);

        // Perform a N1QL Query
        N1qlQueryResult result = bucket.query(
                N1qlQuery.simple("SELECT name FROM `default`")
                /*N1qlQuery.parameterized("SELECT name FROM `default` WHERE $1 IN interests",
                        JsonArray.from("African Swallows"))
        );

        // Print each found Row
        for (N1qlQueryRow row : result) {
            // Prints {"name":"Arthur"}
            System.out.println(row);
        }*/
    }
}
