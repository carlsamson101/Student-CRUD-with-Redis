const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const redis = require('redis');
const cors = require('cors');
const bodyParser = require('body-parser');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(bodyParser.json());

// Setup multer for file upload
const upload = multer({ dest: 'uploads/' });

// Connect to Redis
const client = redis.createClient({
  url: 'redis://@127.0.0.1:6379'  // Default Redis connection
});

client.connect()
  .then(() => console.log('Connected to Redis'))
  .catch(err => console.error('Redis connection error:', err));

// CRUD Operations

// Route to save student data
app.post('/students', async (req, res) => {
  const { id, name, email, contact, college, course, age, address } = req.body;

  // Validate input fields
  if (!id || !name || !course || !age || !address) {
    return res.status(400).json({ message: 'All fields are required' });
  }

  try {
    // Set student data in Redis
    const studentData = { name, email, contact, college, course, age, address };

    // Save student data in Redis hash
    await client.hSet(`student:${id}`, 'name', studentData.name);
    await client.hSet(`student:${id}`, 'email', studentData.email);
    await client.hSet(`student:${id}`, 'contact', studentData.contact);
    await client.hSet(`student:${id}`, 'college', studentData.college);
    await client.hSet(`student:${id}`, 'course', studentData.course);
    await client.hSet(`student:${id}`, 'age', studentData.age);
    await client.hSet(`student:${id}`, 'address', studentData.address);

    res.status(201).json({ message: 'Student saved successfully' });
  } catch (error) {
    console.error('Error saving student:', error);
    res.status(500).json({ message: 'Failed to save student' });
  }
});

// Read all students
app.get('/students', async (req, res) => {
  const keys = await client.keys('student:*');
  const students = await Promise.all(keys.map(async (key) => {
    return { id: key.split(':')[1], ...(await client.hGetAll(key)) };
  }));
  res.json(students);
});

// Update student data
app.put('/students/:id', async (req, res) => {
  const id = req.params.id;
  const { name, email, contact, college, course, age, address } = req.body;

  if (!name && !course && !age && !address) {
    return res.status(400).json({ message: 'At least one field is required to update' });
  }

  try {
    const existingStudent = await client.hGetAll(`student:${id}`);
    if (Object.keys(existingStudent).length === 0) {
      return res.status(404).json({ message: 'Student not found' });
    }

    // Update student data in Redis
    if (name) await client.hSet(`student:${id}`, 'name', name);
    if (name) await client.hSet(`student:${id}`, 'email', email);
    if (name) await client.hSet(`student:${id}`, 'contact', contact);
    if (name) await client.hSet(`student:${id}`, 'college', college);
    if (course) await client.hSet(`student:${id}`, 'course', course);
    if (age) await client.hSet(`student:${id}`, 'age', age);
    if (address) await client.hSet(`student:${id}`, 'address', address);

    res.status(200).json({ message: 'Student updated successfully' });
  } catch (error) {
    console.error('Error updating student:', error);
    res.status(500).json({ message: 'Failed to update student' });
  }
});

// Delete student data
app.delete('/students/:id', async (req, res) => {
  const id = req.params.id;
  await client.del(`student:${id}`);
  res.status(200).json({ message: 'Student deleted successfully' });
});

// Route for uploading CSV
app.post('/students/upload-csv', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ message: 'No file uploaded' });
  }

  const results = [];
  fs.createReadStream(req.file.path)
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', async () => {
      try {
        // Save CSV data to Redis
        for (const student of results) {
          const { id, name, email, contact, college, course, age, address } = student;

          // Validate student fields
          if (!id || !name || !course || !age || !address) {
            continue; // Skip invalid rows
          }

          // Save each student data
          await client.hSet(`student:${id}`, 'name', name);
          await client.hSet(`student:${id}`, 'email', email);
          await client.hSet(`student:${id}`, 'contact', contact);
          await client.hSet(`student:${id}`, 'college', college);
          await client.hSet(`student:${id}`, 'course', course);
          await client.hSet(`student:${id}`, 'age', age);
          await client.hSet(`student:${id}`, 'address', address);
        }
        fs.unlinkSync(req.file.path); // Clean up uploaded CSV file
        res.json({ message: 'CSV data uploaded and processed successfully', data: results });
      } catch (error) {
        console.error('Error saving CSV data:', error);
        res.status(500).json({ message: 'Failed to process CSV' });
      }
    });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
