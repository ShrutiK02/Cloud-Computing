const http = require('http');
const url = require('url');
const { MongoClient } = require('mongodb');

const { MONGO_URL, MONGO_DATABASE } = process.env;

if (!MONGO_URL || !MONGO_DATABASE) {
  console.error('Environment variables MONGO_URL and MONGO_DATABASE must be set');
  process.exit(1);
}

const uri = `mongodb://${MONGO_URL}/${MONGO_DATABASE}`;
console.log(`Connecting to MongoDB at ${uri}`);

const server = http.createServer(async (req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const student_id = parseInt(parsedUrl.query.student_id);

  if (isNaN(student_id)) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end("Invalid student_id, please provide a valid number\n");
    return;
  }

  if (/^\/api\/score/.test(req.url)) {
    try {
      const client = await MongoClient.connect(uri);
      const db = client.db();
      const student = await db.collection("students").findOne({ "student_id": student_id });

      if (student) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          student_id: student.student_id,
          student_name: student.student_name,
          student_score: student.grade,
        }));
      } else {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end("Student Not Found\n");
      }

      client.close();
    } catch (err) {
      console.error('Database error:', err);
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end("Internal Server Error\n");
    }
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end("Wrong URL, please try again\n");
  }
});

server.listen(8080, () => {
  console.log('Server listening on port 8080');
});

