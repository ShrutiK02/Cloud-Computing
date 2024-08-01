const { MongoClient } = require('mongodb');

async function run() {
  const url = "mongodb://35.197.115.51/studentdb";
  const client = new MongoClient(url);

  try {
    // Connect to the MongoDB cluster
    await client.connect();

    // Specify the database and collection
    const db = client.db("studentdb");
    const collection = db.collection("students");

    // Create documents to be inserted
    const docs = [
      { student_id: 11111, student_name: "Bruce Lee", grade: 84 },
      { student_id: 22222, student_name: "Jackie Chen", grade: 93 },
      { student_id: 33333, student_name: "Jet Li", grade: 88 }
    ];

    // Insert the documents
    const insertResult = await collection.insertMany(docs);
    console.log(`${insertResult.insertedCount} documents were inserted`);

    // Find one document
    const result = await collection.findOne({ student_id: 11111 });
    console.log(result);
  } finally {
    // Close the connection
    await client.close();
  }
}

run().catch(console.dir);
