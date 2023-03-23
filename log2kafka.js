const fs = require('fs');
const readline = require('readline');
const yaml = require('js-yaml');
const Kafka = require('node-rdkafka');
const Mustache = require('mustache');

// Load Kafka configuration from YAML file
const kafkaConfig = yaml.safeLoad(fs.readFileSync('kafka-config.yaml', 'utf8'));

// Create Kafka producer
const producer = new Kafka.Producer(kafkaConfig);

// Connect to Kafka broker
producer.connect();

// Load log configuration from JSON file
const logConfig = JSON.parse(fs.readFileSync('log-config.json', 'utf8'));

// Map to keep track of the file positions
const filePositions = new Map();

// Read each log file asynchronously
logConfig.files.forEach(file => {
  const positionFile = file.path + '.position';
  let position = 0;

  try {
    // Try to load the position from the position file
    const positionData = fs.readFileSync(positionFile, 'utf8');
    position = parseInt(positionData);
  } catch (error) {}

  filePositions.set(file.path, position);

  // Watch the log file for changes
  fs.watchFile(file.path, { persistent: true, interval: 1000 }, (curr, prev) => {
    if (curr.mtime <= prev.mtime) {
      // The file hasn't changed
      return;
    }

    // Create a read stream starting from the last known position
    const readStream = fs.createReadStream(file.path, { start: position, end: curr.size });

    const rl = readline.createInterface({
      input: readStream,
      crlfDelay: Infinity
    });

    rl.on('line', line => {
      const fields = line.trim().split(file.delimiter);

      // Create a message object with the mapped data
      const message = {};
      file.fields.forEach((field, index) => {
        message[field.name] = fields[index];
      });

      // Add a timestamp to the message object
      message.timestamp = Date.now();

      // Create a topic name using Mustache syntax to substitute the mapped data
      const topicName = Mustache.render(file.topic, message);

      // Create a Kafka message with the mapped data
      const kafkaMessage = {
        value: Buffer.from(JSON.stringify(message)),
        headers: {
          [file.headerKey]: Buffer.from(fields[file.headerIndex])
        }
      };

      // Send the Kafka message to the appropriate topic
      producer.produce(topicName, null, kafkaMessage);
    });

    rl.on('close', () => {
      // Update the file position
      filePositions.set(file.path, curr.size);
      // Save the file position to the position file
      fs.writeFileSync(positionFile, curr.size.toString(), 'utf8');
    });
  });
});

// Handle errors
producer.on('error', error => {
  console.error(error);
});

// Gracefully shutdown producer on SIGINT
process.on('SIGINT', () => {
  producer.disconnect();
});
